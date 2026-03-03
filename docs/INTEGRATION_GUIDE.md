# Airweave Integration Guide – Caretta

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│  User clicks "Connect Notion" in Settings > Integrations           │
└──────────────┬──────────────────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────┐    POST /api/airweave/sources
│   caretta-webapp         │──────────────────────────────────┐
│   (Next.js)              │                                  │
│                          │    GET /api/airweave/sources      │
│   Settings > Integrations│◄─────────────────────────────────┤
└──────────────────────────┘                                  │
                                                              ▼
                                               ┌──────────────────────────┐
                                               │   Airweave Backend       │
                                               │   (ECS Fargate / :8001)  │
                                               │                          │
                                               │   ALB → backend          │
                                               │   + temporal-worker      │
                                               └─────┬───────────┬───────┘
                                                     │           │
                                          ┌──────────┘           └──────────┐
                                          ▼                                 ▼
                                  ┌───────────────┐              ┌──────────────────┐
                                  │ Supabase PG   │              │ Temporal Cloud    │
                                  │ (pgvector)    │              │ (workflow orch)   │
                                  └───────────────┘              └──────────────────┘
                                          ▲
                                          │  vector search
               ┌──────────────────────────┘
               │
┌──────────────┴───────────┐
│   Project-N              │    POST {AIRWEAVE_API_URL}/api/v1/search
│   (Electron desktop)     │───────────────────────────────────────────►  Airweave
│                          │◄──────────────────────────────────────────
│   OrgKnowledgeContext    │    { results: [...] }
│   + Airweave search      │
└──────────────────────────┘
```

### Data flow end-to-end

1. **User connects Notion** in caretta-webapp Settings > Integrations tab
2. caretta-webapp reads the user's existing OAuth tokens from `user_oauth_tokens` (via projectN-lambdas API)
3. caretta-webapp calls Airweave API to:
   - Create a collection for the user (idempotent, one per user)
   - Create a source connection (e.g. Notion) with the injected token
4. **Airweave syncs** — Temporal workflow crawls the source, chunks content, embeds, stores vectors in Supabase pgvector
5. **During a call**, Project-N's `OrgKnowledgeContextWorkflow` queries Airweave search API in parallel with existing org knowledge search
6. **Results are merged** into the LLM context prompt and surfaced as real-time insights

---

## 1. caretta-webapp Changes

### 1.1 Add `AIRWEAVE_API_URL` env var

**File: `src/env.ts`**

```diff
 const server = {
+  AIRWEAVE_API_URL: z.string().url().optional(),
   ATTIO_CLIENT_ID: z.string().min(1),
   // ...
 };
```

And in `runtimeEnv`:

```diff
   runtimeEnv: {
+    AIRWEAVE_API_URL: process.env.AIRWEAVE_API_URL,
     ATTIO_CLIENT_ID: process.env.ATTIO_CLIENT_ID,
```

### 1.2 Create Airweave API proxy route

**New file: `src/app/api/airweave/[...path]/route.ts`**

This proxies all `/api/airweave/*` requests to the Airweave backend, forwarding the user's Supabase JWT for auth. Airweave's fork should validate this JWT against the same Supabase project.

```typescript
import { NextRequest, NextResponse } from "next/server";
import { env } from "@/env";
import { createClient } from "@supabase/supabase-js";

const AIRWEAVE_URL = env.AIRWEAVE_API_URL ?? "http://localhost:8001";

async function proxyToAirweave(req: NextRequest) {
  // Extract the sub-path after /api/airweave/
  const url = new URL(req.url);
  const subPath = url.pathname.replace("/api/airweave/", "");
  const targetUrl = `${AIRWEAVE_URL}/api/v1/${subPath}${url.search}`;

  // Forward the Authorization header as-is (Supabase JWT)
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };

  const authHeader = req.headers.get("Authorization");
  if (authHeader) {
    headers["Authorization"] = authHeader;
  }

  const fetchOptions: RequestInit = {
    method: req.method,
    headers,
  };

  if (req.method !== "GET" && req.method !== "HEAD") {
    fetchOptions.body = await req.text();
  }

  const response = await fetch(targetUrl, fetchOptions);
  const data = await response.text();

  return new NextResponse(data, {
    status: response.status,
    headers: { "Content-Type": response.headers.get("Content-Type") ?? "application/json" },
  });
}

export const GET = proxyToAirweave;
export const POST = proxyToAirweave;
export const PUT = proxyToAirweave;
export const DELETE = proxyToAirweave;
```

### 1.3 Create Airweave client library

**New file: `src/lib/airweaveClient.ts`**

```typescript
const AIRWEAVE_PROXY = "/api/airweave";

async function airweaveFetch<T>(
  path: string,
  options: RequestInit = {},
  token?: string,
): Promise<T> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...(options.headers as Record<string, string>),
  };
  if (token) {
    headers["Authorization"] = `Bearer ${token}`;
  }

  const res = await fetch(`${AIRWEAVE_PROXY}/${path}`, {
    ...options,
    headers,
  });

  if (!res.ok) {
    const err = await res.json().catch(() => ({ message: res.statusText }));
    throw new Error(err.detail ?? err.message ?? `Airweave error ${res.status}`);
  }

  return res.json();
}

// Types matching Airweave API
export type AirweaveCollection = {
  id: string;
  name: string;
  readable_id: string;
  created_at: string;
};

export type AirweaveSource = {
  id: string;
  name: string;
  short_name: string; // e.g. "notion", "google_drive", "gmail"
  status: "active" | "syncing" | "error" | "disconnected";
  last_synced_at: string | null;
};

export type AirweaveConnection = {
  id: string;
  source_id: string;
  collection_id: string;
  status: string;
  created_at: string;
};

// ---- Collections ----

export async function getOrCreateCollection(
  userId: string,
  token: string,
): Promise<AirweaveCollection> {
  const readableId = `user-${userId}`;

  // Try to get existing collection
  const collections = await airweaveFetch<AirweaveCollection[]>(
    "collections",
    { method: "GET" },
    token,
  );

  const existing = collections.find((c) => c.readable_id === readableId);
  if (existing) return existing;

  // Create new one
  return airweaveFetch<AirweaveCollection>(
    "collections",
    {
      method: "POST",
      body: JSON.stringify({
        name: `Caretta – ${userId}`,
        readable_id: readableId,
      }),
    },
    token,
  );
}

// ---- Source Connections ----

export async function listConnections(
  collectionId: string,
  token: string,
): Promise<AirweaveConnection[]> {
  return airweaveFetch<AirweaveConnection[]>(
    `collections/${collectionId}/connections`,
    { method: "GET" },
    token,
  );
}

export async function createSourceConnection(
  collectionId: string,
  sourceShortName: string,
  credentials: { access_token: string; refresh_token?: string },
  token: string,
): Promise<AirweaveConnection> {
  return airweaveFetch<AirweaveConnection>(
    `connections`,
    {
      method: "POST",
      body: JSON.stringify({
        collection_id: collectionId,
        source_short_name: sourceShortName,
        credentials,
      }),
    },
    token,
  );
}

export async function deleteConnection(
  connectionId: string,
  token: string,
): Promise<void> {
  await airweaveFetch(
    `connections/${connectionId}`,
    { method: "DELETE" },
    token,
  );
}

// ---- Available Sources ----

export async function listAvailableSources(
  token: string,
): Promise<AirweaveSource[]> {
  return airweaveFetch<AirweaveSource[]>("sources", { method: "GET" }, token);
}

// ---- Search ----

export async function searchAirweave(
  collectionId: string,
  query: string,
  limit: number = 10,
  token?: string,
): Promise<{ results: { content: string; metadata: Record<string, unknown>; score: number }[] }> {
  return airweaveFetch(
    `search`,
    {
      method: "POST",
      body: JSON.stringify({
        collection_id: collectionId,
        query,
        limit,
      }),
    },
    token,
  );
}
```

### 1.4 Update Settings > Integrations tab

**File: `src/components/settings/data.ts`** — Enable Notion and Google Drive (already listed as `available: false`):

```diff
   {
     id: "notion",
     name: "Notion",
     description:
       "Integrating with Notion enables you to sync your workspace data, access your notes and documents, and streamline your knowledge management process.",
     logoPath: "/integrations/notion.png",
-    available: false,
+    available: true,
     color: "#000000",
   },
   {
     id: "google-drive",
     name: "Google Drive",
     description:
       "Integrating with Google Drive enables you to access your files and documents. Seamlessly share and collaborate on files directly from your workspace.",
     logoPath: "/integrations/google_drive.png",
-    available: false,
+    available: true,
     color: "#A3E635",
   }
```

**File: `src/components/settings/tab-integrations.tsx`** — Add Airweave source connection handlers.

Add this block inside `IntegrationsTab()`, after the existing CRM/Slack/Telegram handlers:

```typescript
// --- Airweave source connections ---
import {
  getOrCreateCollection,
  createSourceConnection,
  deleteConnection,
  listConnections,
  type AirweaveConnection,
} from "@/lib/airweaveClient";

const [airweaveConnections, setAirweaveConnections] = useState<AirweaveConnection[]>([]);
const [airweaveCollectionId, setAirweaveCollectionId] = useState<string | null>(null);
const [connectingSource, setConnectingSource] = useState<string | null>(null);

// Map integration IDs to Airweave source short names
const AIRWEAVE_SOURCE_MAP: Record<string, string> = {
  notion: "notion",
  "google-drive": "google_drive",
};

const initAirweaveCollection = useCallback(async () => {
  if (!user?.id) return;
  const runtimeApi = getRuntimeAPI();
  const authStatus = await runtimeApi.getAuthStatus();
  if (!authStatus?.token) return;

  const collection = await getOrCreateCollection(user.id, authStatus.token);
  setAirweaveCollectionId(collection.id);

  const connections = await listConnections(collection.id, authStatus.token);
  setAirweaveConnections(connections);
}, [user?.id]);

useEffect(() => {
  initAirweaveCollection();
}, [initAirweaveCollection]);

const connectAirweaveSource = useCallback(
  async (integrationId: string) => {
    const sourceShortName = AIRWEAVE_SOURCE_MAP[integrationId];
    if (!sourceShortName || !airweaveCollectionId || !user?.id) return;

    setConnectingSource(integrationId);
    try {
      const runtimeApi = getRuntimeAPI();
      const authStatus = await runtimeApi.getAuthStatus();
      if (!authStatus?.token) throw new Error("Not authenticated");

      // Fetch the user's existing OAuth token for this provider
      const apiClient = getApiClient();
      const tokens = await apiClient.users.getOAuthTokens({ provider: sourceShortName });
      const providerToken = tokens.find((t: any) => t.provider === sourceShortName);

      if (!providerToken?.accessToken) {
        toast.error(`No ${integrationId} token found. Please connect ${integrationId} first via OAuth.`);
        return;
      }

      await createSourceConnection(
        airweaveCollectionId,
        sourceShortName,
        {
          access_token: providerToken.accessToken,
          refresh_token: providerToken.refreshToken,
        },
        authStatus.token,
      );

      toast.success(`${integrationId} connected to Airweave — syncing started`);
      await initAirweaveCollection(); // refresh
    } catch (err: any) {
      toast.error(`Failed to connect: ${err.message}`);
    } finally {
      setConnectingSource(null);
    }
  },
  [airweaveCollectionId, user?.id, initAirweaveCollection],
);

const disconnectAirweaveSource = useCallback(
  async (connectionId: string) => {
    try {
      const runtimeApi = getRuntimeAPI();
      const authStatus = await runtimeApi.getAuthStatus();
      if (!authStatus?.token) return;

      await deleteConnection(connectionId, authStatus.token);
      toast.success("Source disconnected");
      await initAirweaveCollection();
    } catch (err: any) {
      toast.error(`Failed to disconnect: ${err.message}`);
    }
  },
  [initAirweaveCollection],
);
```

Then, in the JSX where integration cards are rendered, add a condition for Airweave-backed sources:

```typescript
// Inside the integration card rendering logic:
const isAirweaveSource = (id: string) => id in AIRWEAVE_SOURCE_MAP;
const getAirweaveConnection = (id: string) => {
  const shortName = AIRWEAVE_SOURCE_MAP[id];
  return airweaveConnections.find((c) => c.source_id === shortName);
};

// For Airweave sources, use connectAirweaveSource/disconnectAirweaveSource
// instead of the existing CRM/Slack flows
```

---

## 2. Project-N Changes

### 2.1 Add Airweave search method to API client

**File: `electron/api.ts`**

Add after the existing `searchOrgKnowledge` method (line ~413):

```typescript
// Airweave search
async searchAirweave(
    collectionId: string,
    query: string,
    limit: number = 10,
): Promise<{ results: { content: string; metadata: Record<string, unknown>; score: number }[] }> {
    const AIRWEAVE_URL = process.env.AIRWEAVE_API_URL || "http://localhost:8001";
    return this.fetchAPIWithBase(
        AIRWEAVE_URL,
        "/api/v1/search",
        {
            method: "POST",
            body: JSON.stringify({
                collection_id: collectionId,
                query,
                limit,
            }),
            headers: await this.authHeaders(),
        },
    );
}
```

### 2.2 Wire Airweave search into OrgKnowledgeContextWorkflow

**File: `electron/services/llmOrchestrator/orgKnowledgeContextWorkflow/orgKnowledgeContextWorkflow.ts`**

Add a parallel fetch method and merge results with existing org knowledge:

```typescript
// Add to class properties (after line ~59):
private airweaveCollectionId: string | null = null;

// Add init logic (in the init method or constructor):
// Resolve the user's Airweave collection ID on start
private async resolveAirweaveCollection(): Promise<void> {
    try {
        const AIRWEAVE_URL = process.env.AIRWEAVE_API_URL;
        if (!AIRWEAVE_URL) return;

        const user = this.api.authService.getUser();
        if (!user?.id) return;

        const res = await fetch(`${AIRWEAVE_URL}/api/v1/collections`, {
            headers: { Authorization: `Bearer ${user.token}` },
        });
        if (!res.ok) return;

        const collections = await res.json();
        const match = collections.find(
            (c: any) => c.readable_id === `user-${user.id}`,
        );
        if (match) {
            this.airweaveCollectionId = match.id;
            log.info("[OrgKnowledgeContext] Airweave collection resolved", match.id);
        }
    } catch (err) {
        log.warn("[OrgKnowledgeContext] Airweave collection lookup failed", err);
    }
}

// Add parallel search method:
private async fetchAirweaveResults(queryText: string, limit: number): Promise<OrgKnowledge[]> {
    if (!this.airweaveCollectionId) return [];

    try {
        const { results } = await this.api.searchAirweave(
            this.airweaveCollectionId,
            queryText,
            limit,
        );

        // Map Airweave results to OrgKnowledge shape for unified handling
        return results.map((r, i) => ({
            id: `airweave-${i}-${Date.now()}`,
            title: (r.metadata?.title as string) ?? "Airweave result",
            content: r.content,
            category: "product_info" as const,
            status: "active" as const,
            sourceInfo: { type: "notion" as const },
            priority: Math.round(r.score * 100),
            triggers: { keywords: [] },
        })) as OrgKnowledge[];
    } catch (err) {
        log.warn("[OrgKnowledgeContext] Airweave search failed", err);
        return [];
    }
}
```

Then modify `fetchOrgKnowledge` (line ~428) to run both searches in parallel:

```typescript
private async fetchOrgKnowledge(queryText: string, limit: number): Promise<OrgKnowledge[]> {
    const [orgItems, airweaveItems] = await Promise.all([
        (async () => {
            const embedding = await this.embeddingClient.embedText(queryText);
            const { items } = await this.api.searchOrgKnowledge({
                queryEmbedding: embedding,
                limit,
            });
            return Array.isArray(items) ? items : [];
        })(),
        this.fetchAirweaveResults(queryText, limit),
    ]);

    // Interleave: org knowledge first, then airweave, deduped by mergeItems
    return [...orgItems, ...airweaveItems];
}
```

### 2.3 Add environment variable

**File: `.env.example` and `.env.production`**

```
AIRWEAVE_API_URL=https://airweave-alb.eu-north-1.elb.amazonaws.com
```

---

## 3. projectN-lambdas Changes

**No code changes required.** The existing OAuth token CRUD (`POST/GET /users/me/oauth/tokens`) already supports arbitrary providers. When caretta-webapp needs Notion/Google Drive tokens to pass to Airweave, it fetches them via the existing `apiClient.users.getOAuthTokens()`.

If you later want the lambda layer to proxy Airweave search (instead of Project-N calling Airweave directly), you would add:

```typescript
// router.ts — optional future addition
router.addRoute("POST", "/airweave/search", requireAuth(airweaveHandler.search));
```

But this is **not needed for v1** — Project-N can call Airweave ALB directly with the Supabase JWT.

---

## 4. Env Var Checklist

### caretta-webapp (Vercel / Next.js)

| Variable | Value | Where |
|----------|-------|-------|
| `AIRWEAVE_API_URL` | `https://<ALB_DNS>` | Vercel env vars |

### Project-N (Electron desktop)

| Variable | Value | Where |
|----------|-------|-------|
| `AIRWEAVE_API_URL` | `https://<ALB_DNS>` | `.env.production` / build config |

### Airweave ECS (Secrets Manager + task def)

| Variable | Source | Notes |
|----------|--------|-------|
| `PGVECTOR_CONNECTION_STRING` | Secrets Manager | `postgresql://postgres:<pw>@db.<ref>.supabase.co:5432/postgres` |
| `TEMPORAL_HOST` | Task def env | `caretta.<id>.tmprl.cloud` |
| `TEMPORAL_PORT` | Task def env | `7233` |
| `TEMPORAL_NAMESPACE` | Task def env | `caretta` |
| `TEMPORAL_TLS_CERT` | Secrets Manager | PEM cert for Temporal mTLS |
| `TEMPORAL_TLS_KEY` | Secrets Manager | PEM key for Temporal mTLS |
| `REDIS_HOST` | Task def env | Existing Redis endpoint |
| `REDIS_PORT` | Task def env | `6379` |

### Supabase (existing, no changes)

Airweave authenticates requests using the same Supabase JWT. Configure in Airweave's auth middleware:

| Variable | Value |
|----------|-------|
| `SUPABASE_URL` | `https://ztejbfpbhxgwecvxngtf.supabase.co` |
| `SUPABASE_ANON_KEY` | (same anon key used by other services) |
| `SUPABASE_JWT_SECRET` | (from Supabase dashboard > Settings > API) |

---

## 5. Deployment

### 5.1 Build & push Docker image

```bash
# From the airweave repo root
export AWS_REGION=eu-north-1
export ECR_URL=$(terraform -chdir=terraform output -raw ecr_repository_url)

aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $ECR_URL

docker build -t airweave:latest .
docker tag airweave:latest $ECR_URL:latest
docker push $ECR_URL:latest
```

### 5.2 Populate secrets

```bash
aws secretsmanager put-secret-value \
  --secret-id airweave-production/pgvector-connection-string \
  --secret-string "postgresql://postgres:PASSWORD@db.ztejbfpbhxgwecvxngtf.supabase.co:5432/postgres"

aws secretsmanager put-secret-value \
  --secret-id airweave-production/temporal-tls-cert \
  --secret-string "$(cat temporal-client.pem)"

aws secretsmanager put-secret-value \
  --secret-id airweave-production/temporal-tls-key \
  --secret-string "$(cat temporal-client.key)"
```

### 5.3 Terraform apply

```bash
cd terraform/
terraform init
terraform plan -var-file=terraform.tfvars
terraform apply -var-file=terraform.tfvars
```

### 5.4 Force new deployment (after image push)

```bash
CLUSTER=$(terraform output -raw ecs_cluster_name)

aws ecs update-service --cluster $CLUSTER \
  --service $(terraform output -raw backend_service_name) \
  --force-new-deployment

aws ecs update-service --cluster $CLUSTER \
  --service $(terraform output -raw worker_service_name) \
  --force-new-deployment
```

### 5.5 Verify

```bash
ALB_URL=$(terraform output -raw alb_url)
curl -s $ALB_URL/health | jq .
# Expected: {"status": "ok"}
```

---

## 6. Auth Model

Airweave shares the **same Supabase project** as all other Caretta services. Auth works as follows:

1. All requests to Airweave carry the user's Supabase access token: `Authorization: Bearer <jwt>`
2. Airweave validates the JWT against the Supabase JWT secret
3. The `sub` claim maps to the user ID, scoping collections and connections
4. Source credentials (e.g., Notion OAuth tokens) are **injected directly** via the `POST /connections` endpoint — Airweave never redirects to OAuth providers itself

This means:
- **No new OAuth flows** in Airweave
- Caretta-webapp already manages all OAuth tokens
- Airweave just receives and uses them

---

## 7. Key Airweave API Endpoints Reference

All under `/api/v1/`:

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/sources` | List available source types (notion, google_drive, etc.) |
| `GET` | `/collections` | List user's collections |
| `POST` | `/collections` | Create a new collection |
| `GET` | `/collections/{id}/connections` | List connections in a collection |
| `POST` | `/connections` | Create source connection (with injected credentials) |
| `DELETE` | `/connections/{id}` | Remove a source connection |
| `POST` | `/connections/{id}/sync` | Trigger a manual sync |
| `GET` | `/connections/{id}/status` | Check sync status |
| `POST` | `/search` | Vector search across a collection |
| `GET` | `/health` | Health check |

---

## 8. Future Enhancements

1. **HTTPS on ALB**: Add ACM certificate + Route53 record, uncomment the HTTPS listener in terraform
2. **Webhook sync status**: Airweave can POST sync completion events back to caretta-webapp (the svix replacement)
3. **Lambda proxy**: Route Airweave search through projectN-lambdas if you want a single API gateway
4. **Sync scheduling**: Configure Temporal cron schedules for periodic re-sync (e.g., every 6 hours)
5. **Multi-source per user**: The collection model already supports multiple sources — just call `POST /connections` for each
