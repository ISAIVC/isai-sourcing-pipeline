module "networking" {
  source = "./modules/networking"
  vpc_id              = var.vpc_id
  azs                 = var.azs
  public_subnet_cidrs = var.public_subnet_cidrs
  name_prefix         = local.name_prefix
  tags                = local.common_tags
}

module "iam" {
  source = "./modules/iam"

  name_prefix = local.name_prefix
  tags        = local.common_tags
}

module "ecr" {
  source = "./modules/ecr"

  name_prefix = local.name_prefix
  tags        = local.common_tags
}


module "secrets" {
  source = "./modules/secrets"

  name_prefix = local.name_prefix
  tags        = local.common_tags
  secrets = {
    google-credentials            = var.google_credentials
    dealroom-api-key          = var.dealroom_api_key
    supabase-url              = var.supabase_url
    supabase-service-role-key = var.supabase_service_role_key
    attio-by-token            = var.attio_by_token
    attio-cg-token            = var.attio_cg_token
  }
}

locals {
  prefect_envvars = concat([
    {
      name  = "PREFECT_LOGGING_LEVEL"
      value = "INFO"
    },
    {
      name  = "PREFECT_LOGGING_LOG_PRINTS"
      value = true
    }
  ], var.task_env_vars)
}

module "ecs" {
  source = "./modules/ecs"

  name_prefix        = local.name_prefix
  tags               = local.common_tags
}
# Prefect Work Pools - using shared module (replaces old prefect_infra)
module "prefect_workpools" {
  source = "./modules/prefect_workpools"

  # Environment configuration
  environment  = var.environment
  workspace_id = var.prefect_config.workspace_id

  # Work pool names
  pool_name = var.work_pool.pool_name
  pool_cpu = var.work_pool.pool_cpu
  pool_memory = var.work_pool.pool_memory

  # ECS configuration for slow pool
  image = module.ecr.repository_url

  envvars = {
    for envvar in local.prefect_envvars : envvar.name => envvar.value
  }

  vpc_id             = var.vpc_id
  cluster            = module.ecs.cluster_name
  command            = local.prefect_ecs_task_command
  task_role_arn      = module.iam.task_role_arn
  container_name     = local.prefect_ecs_task_container_name
  execution_role_arn = module.iam.task_execution_role_arn
  subnets            = module.networking.public_subnet_ids
  security_groups    = [module.networking.security_group_id]

  # AWS credentials
  aws_access_key_id     = module.iam.prefect_ecs_user_access_key_id
  aws_secret_access_key = module.iam.prefect_ecs_user_secret_access_key
  region_name           = var.aws_region
  
  # Secrets configuration
  secrets_arn    = module.secrets.secret_arn

  # CloudWatch logging
  log_group_name = module.ecs.log_group_name
}

## Prefect Automations
module "prefect_automations" {
  source = "./modules/prefect_automations"

  environment = var.environment
  workspace_id = var.prefect_config.workspace_id

  crash_zombie_flows_enabled = true
}