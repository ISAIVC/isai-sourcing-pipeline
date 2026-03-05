# General variables
environment  = "dev"
project_name = "isai-etl"
aws_region   = "eu-north-1"

# Network variables
vpc_id              = "vpc-0d8792a657a498d03"
azs                 = ["eu-north-1a", "eu-north-1b"]
public_subnet_cidrs = ["172.31.64.0/20", "172.31.80.0/20"]

work_pool = {
  pool_name   = "ecs-push-pool"
  pool_cpu    = 1024
  pool_memory = 2048
}

task_env_vars = [
  {
    name  = "ENV"
    value = "dev"
  },
  {
    name  = "GOOGLE_CLOUD_LOCATION"
    value = "global"
  },
  {
    name  = "GOOGLE_GENAI_USE_VERTEXAI"
    value = "true"
  },
  {
    name  = "GOOGLE_CLOUD_PROJECT"
    value = "data-driven-sourcing-486915"
  },
  {
      name  = "PREFECT_FLOWS_HEARTBEAT_FREQUENCY"
      value = "30"
  },
  {
    name = "TRAXCN_EXPORTS_BUCKET_NAME"
    value = "traxcn_exports"
  },
  {
    name = "WEBSITES_BUCKET_NAME"
    value = "websites"
  },
  {
    name = "DEALROOM_BUCKET_NAME"
    value = "dealroom"
  },
  {
    name = "BATCH_SIZE"
    value = "200"
  },
  {
    name = "PARALLEL_BATCHES"
    value = "2"
  },
  {
    name = "ESTIMATED_TIME_PER_BATCH"
    value = "120"
  },
  {
    name = "OFFSET_BETWEEN_PARALLEL_BATCHES"
    value = "3"
  },
  {
    name = "FULL_PIPELINE_DEPLOYMENT_NAME"
    value = "full-pipeline-flow/full-pipeline-deployment"
  },
  {
    name = "WEBSITE_ENRICHMENT_BATCH_SIZE"
    value = "20"
  },
  {
    name = "COMPUTE_BUSINESS_METRIC_BATCH_SIZE"
    value = "200"
  },
  {
    name = "SEARCH_RESOURCES_BUCKET_NAME"
    value = "search_ressources"
  }
]