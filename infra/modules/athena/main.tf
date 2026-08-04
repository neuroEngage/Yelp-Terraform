resource "aws_athena_workgroup" "yelp_workgroup" {
  name        = "${var.project_name}_workgroup_${var.environment}"
  description = "Athena workgroup for Yelp Analytics"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = var.athena_results_path

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
