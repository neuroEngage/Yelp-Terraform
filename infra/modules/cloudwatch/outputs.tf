output "log_group_name" {
  value       = aws_cloudwatch_log_group.glue_log_group.name
  description = "CloudWatch Log Group Name"
}

output "log_group_arn" {
  value       = aws_cloudwatch_log_group.glue_log_group.arn
  description = "CloudWatch Log Group ARN"
}
