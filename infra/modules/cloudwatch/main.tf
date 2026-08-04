resource "aws_cloudwatch_log_group" "glue_log_group" {
  name              = "/aws/glue/${var.project_name}-${var.environment}"
  retention_in_days = 7
}
