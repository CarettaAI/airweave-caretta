###############################################################################
# Airweave ECS Deployment – Caretta Integration
#
# Two Fargate services:
#   1. airweave-backend  (port 8001, behind ALB)
#   2. airweave-temporal-worker  (no ingress, connects out to Temporal Cloud)
#
# All secrets are pulled from AWS Secrets Manager at task startup.
###############################################################################

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ---------------------------------------------------------------------------
# Variables
# ---------------------------------------------------------------------------

variable "aws_region" {
  type    = string
  default = "eu-north-1"
}

variable "environment" {
  type    = string
  default = "production"
}

variable "vpc_id" {
  type        = string
  description = "VPC to deploy into (must have public + private subnets)"
}

variable "public_subnet_ids" {
  type        = list(string)
  description = "Public subnets for ALB"
}

variable "private_subnet_ids" {
  type        = list(string)
  description = "Private subnets for Fargate tasks"
}

variable "backend_image_tag" {
  type    = string
  default = "latest"
}

variable "worker_image_tag" {
  type    = string
  default = "latest"
}

variable "backend_cpu" {
  type    = number
  default = 512
}

variable "backend_memory" {
  type    = number
  default = 1024
}

variable "worker_cpu" {
  type    = number
  default = 512
}

variable "worker_memory" {
  type    = number
  default = 1024
}

variable "backend_desired_count" {
  type    = number
  default = 1
}

variable "worker_desired_count" {
  type    = number
  default = 1
}

variable "supabase_host" {
  type        = string
  description = "Supabase Postgres host (e.g. db.ztejbfpbhxgwecvxngtf.supabase.co)"
}

variable "redis_host" {
  type        = string
  description = "Existing Redis host"
}

variable "redis_port" {
  type    = number
  default = 6379
}

variable "temporal_host" {
  type        = string
  description = "Temporal Cloud host (e.g. <namespace>.tmprl.cloud)"
}

variable "temporal_port" {
  type    = number
  default = 7233
}

variable "temporal_namespace" {
  type    = string
  default = "caretta"
}

locals {
  name_prefix = "airweave-${var.environment}"
}

# ---------------------------------------------------------------------------
# ECR Repository
# ---------------------------------------------------------------------------

resource "aws_ecr_repository" "airweave" {
  name                 = "caretta/airweave"
  image_tag_mutability = "MUTABLE"
  force_delete         = false

  image_scanning_configuration {
    scan_on_push = true
  }
}

# ---------------------------------------------------------------------------
# Secrets Manager
# ---------------------------------------------------------------------------

resource "aws_secretsmanager_secret" "pgvector_connection_string" {
  name = "${local.name_prefix}/pgvector-connection-string"
}

resource "aws_secretsmanager_secret" "temporal_tls_cert" {
  name = "${local.name_prefix}/temporal-tls-cert"
}

resource "aws_secretsmanager_secret" "temporal_tls_key" {
  name = "${local.name_prefix}/temporal-tls-key"
}

# Note: populate these secrets manually or via CI:
#   aws secretsmanager put-secret-value --secret-id airweave-production/pgvector-connection-string \
#     --secret-string "postgresql://user:pass@db.xxx.supabase.co:5432/postgres"

# ---------------------------------------------------------------------------
# ECS Cluster
# ---------------------------------------------------------------------------

resource "aws_ecs_cluster" "airweave" {
  name = local.name_prefix

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

# ---------------------------------------------------------------------------
# IAM – Task execution & task role
# ---------------------------------------------------------------------------

resource "aws_iam_role" "ecs_execution" {
  name = "${local.name_prefix}-ecs-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_execution_base" {
  role       = aws_iam_role.ecs_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "ecs_execution_secrets" {
  name = "secrets-access"
  role = aws_iam_role.ecs_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "secretsmanager:GetSecretValue"
      ]
      Resource = [
        aws_secretsmanager_secret.pgvector_connection_string.arn,
        aws_secretsmanager_secret.temporal_tls_cert.arn,
        aws_secretsmanager_secret.temporal_tls_key.arn,
      ]
    }]
  })
}

resource "aws_iam_role" "ecs_task" {
  name = "${local.name_prefix}-ecs-task"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# ---------------------------------------------------------------------------
# CloudWatch Log Groups
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "backend" {
  name              = "/ecs/${local.name_prefix}-backend"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "worker" {
  name              = "/ecs/${local.name_prefix}-worker"
  retention_in_days = 30
}

# ---------------------------------------------------------------------------
# Security Groups
# ---------------------------------------------------------------------------

# ALB SG – inbound 80/443 from internet
resource "aws_security_group" "alb" {
  name_prefix = "${local.name_prefix}-alb-"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  lifecycle { create_before_destroy = true }
}

# Backend SG – inbound 8001 from ALB only
resource "aws_security_group" "backend" {
  name_prefix = "${local.name_prefix}-backend-"
  vpc_id      = var.vpc_id

  ingress {
    from_port       = 8001
    to_port         = 8001
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }

  # Supabase Postgres (5432)
  egress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Supabase Postgres"
  }

  # Temporal Cloud (7233)
  egress {
    from_port   = 7233
    to_port     = 7233
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Temporal Cloud"
  }

  # Redis
  egress {
    from_port   = var.redis_port
    to_port     = var.redis_port
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Redis"
  }

  # HTTPS (for external API calls, e.g. OAuth token exchange)
  egress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "HTTPS outbound"
  }

  lifecycle { create_before_destroy = true }
}

# Worker SG – no ingress, same egress as backend
resource "aws_security_group" "worker" {
  name_prefix = "${local.name_prefix}-worker-"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Supabase Postgres"
  }

  egress {
    from_port   = 7233
    to_port     = 7233
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Temporal Cloud"
  }

  egress {
    from_port   = var.redis_port
    to_port     = var.redis_port
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Redis"
  }

  egress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "HTTPS outbound"
  }

  lifecycle { create_before_destroy = true }
}

# ---------------------------------------------------------------------------
# ALB
# ---------------------------------------------------------------------------

resource "aws_lb" "airweave" {
  name               = "${local.name_prefix}-alb"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets            = var.public_subnet_ids
}

resource "aws_lb_target_group" "backend" {
  name        = "${local.name_prefix}-backend"
  port        = 8001
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"

  health_check {
    path                = "/health"
    port                = "traffic-port"
    healthy_threshold   = 2
    unhealthy_threshold = 3
    interval            = 30
    timeout             = 5
  }
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.airweave.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.backend.arn
  }
}

# Add HTTPS listener when ACM cert is ready:
# resource "aws_lb_listener" "https" {
#   load_balancer_arn = aws_lb.airweave.arn
#   port              = 443
#   protocol          = "HTTPS"
#   ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"
#   certificate_arn   = var.acm_certificate_arn
#   default_action {
#     type             = "forward"
#     target_group_arn = aws_lb_target_group.backend.arn
#   }
# }

# ---------------------------------------------------------------------------
# Task Definitions
# ---------------------------------------------------------------------------

resource "aws_ecs_task_definition" "backend" {
  family                   = "${local.name_prefix}-backend"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.backend_cpu
  memory                   = var.backend_memory
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name      = "airweave-backend"
    image     = "${aws_ecr_repository.airweave.repository_url}:${var.backend_image_tag}"
    essential = true
    command   = ["uvicorn", "airweave.main:app", "--host", "0.0.0.0", "--port", "8001"]

    portMappings = [{
      containerPort = 8001
      protocol      = "tcp"
    }]

    environment = [
      { name = "PORT", value = "8001" },
      { name = "ENVIRONMENT", value = var.environment },
      { name = "TEMPORAL_HOST", value = var.temporal_host },
      { name = "TEMPORAL_PORT", value = tostring(var.temporal_port) },
      { name = "TEMPORAL_NAMESPACE", value = var.temporal_namespace },
      { name = "REDIS_HOST", value = var.redis_host },
      { name = "REDIS_PORT", value = tostring(var.redis_port) },
    ]

    secrets = [
      {
        name      = "PGVECTOR_CONNECTION_STRING"
        valueFrom = aws_secretsmanager_secret.pgvector_connection_string.arn
      },
      {
        name      = "TEMPORAL_TLS_CERT"
        valueFrom = aws_secretsmanager_secret.temporal_tls_cert.arn
      },
      {
        name      = "TEMPORAL_TLS_KEY"
        valueFrom = aws_secretsmanager_secret.temporal_tls_key.arn
      },
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.backend.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "backend"
      }
    }
  }])
}

resource "aws_ecs_task_definition" "worker" {
  family                   = "${local.name_prefix}-worker"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.worker_cpu
  memory                   = var.worker_memory
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name      = "airweave-temporal-worker"
    image     = "${aws_ecr_repository.airweave.repository_url}:${var.worker_image_tag}"
    essential = true
    command   = ["python", "-m", "airweave.worker"]

    environment = [
      { name = "ENVIRONMENT", value = var.environment },
      { name = "TEMPORAL_HOST", value = var.temporal_host },
      { name = "TEMPORAL_PORT", value = tostring(var.temporal_port) },
      { name = "TEMPORAL_NAMESPACE", value = var.temporal_namespace },
      { name = "REDIS_HOST", value = var.redis_host },
      { name = "REDIS_PORT", value = tostring(var.redis_port) },
    ]

    secrets = [
      {
        name      = "PGVECTOR_CONNECTION_STRING"
        valueFrom = aws_secretsmanager_secret.pgvector_connection_string.arn
      },
      {
        name      = "TEMPORAL_TLS_CERT"
        valueFrom = aws_secretsmanager_secret.temporal_tls_cert.arn
      },
      {
        name      = "TEMPORAL_TLS_KEY"
        valueFrom = aws_secretsmanager_secret.temporal_tls_key.arn
      },
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.worker.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "worker"
      }
    }
  }])
}

# ---------------------------------------------------------------------------
# ECS Services
# ---------------------------------------------------------------------------

resource "aws_ecs_service" "backend" {
  name            = "${local.name_prefix}-backend"
  cluster         = aws_ecs_cluster.airweave.id
  task_definition = aws_ecs_task_definition.backend.arn
  desired_count   = var.backend_desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.backend.id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.backend.arn
    container_name   = "airweave-backend"
    container_port   = 8001
  }

  depends_on = [aws_lb_listener.http]
}

resource "aws_ecs_service" "worker" {
  name            = "${local.name_prefix}-worker"
  cluster         = aws_ecs_cluster.airweave.id
  task_definition = aws_ecs_task_definition.worker.arn
  desired_count   = var.worker_desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.worker.id]
    assign_public_ip = false
  }
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "alb_dns_name" {
  description = "ALB DNS name for the Airweave backend"
  value       = aws_lb.airweave.dns_name
}

output "alb_url" {
  description = "Full HTTP URL for the Airweave backend"
  value       = "http://${aws_lb.airweave.dns_name}"
}

output "ecr_repository_url" {
  description = "ECR repository URL for pushing images"
  value       = aws_ecr_repository.airweave.repository_url
}

output "ecs_cluster_name" {
  description = "ECS cluster name"
  value       = aws_ecs_cluster.airweave.name
}

output "backend_service_name" {
  description = "Backend ECS service name (for aws ecs update-service)"
  value       = aws_ecs_service.backend.name
}

output "worker_service_name" {
  description = "Worker ECS service name (for aws ecs update-service)"
  value       = aws_ecs_service.worker.name
}
