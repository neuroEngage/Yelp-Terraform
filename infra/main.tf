module "s3" {
  source             = "./modules/s3"
  bronze_bucket_name = var.bronze_bucket_name
  silver_bucket_name = var.silver_bucket_name
  gold_bucket_name   = var.gold_bucket_name
  project_name       = var.project_name
  environment        = var.environment
}

module "glue" {
  source                = "./modules/glue"
  project_name          = var.project_name
  environment           = var.environment
  bronze_bucket_id      = module.s3.bronze_bucket_id
  bronze_bucket_arn     = module.s3.bronze_bucket_arn
  silver_bucket_id      = module.s3.silver_bucket_id
  silver_bucket_arn     = module.s3.silver_bucket_arn
  gold_bucket_id        = module.s3.gold_bucket_id
  gold_bucket_arn       = module.s3.gold_bucket_arn
  glue_service_role_arn = var.glue_service_role_arn

  depends_on = [module.s3]
}
