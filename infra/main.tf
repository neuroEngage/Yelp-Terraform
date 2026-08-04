module "s3" {
  source       = "./modules/s3"
  bucket_name  = var.bucket_name
  project_name = var.project_name
  environment  = var.environment
}

module "cloudwatch" {
  source       = "./modules/cloudwatch"
  project_name = var.project_name
  environment  = var.environment
}

module "glue" {
  source                = "./modules/glue"
  project_name          = var.project_name
  environment           = var.environment
  bucket_id             = module.s3.bucket_id
  bucket_arn            = module.s3.bucket_arn
  glue_service_role_arn = var.glue_service_role_arn

  depends_on = [module.s3]
}

module "athena" {
  source              = "./modules/athena"
  project_name        = var.project_name
  environment         = var.environment
  athena_results_path = module.s3.athena_results_path

  depends_on = [module.s3]
}
