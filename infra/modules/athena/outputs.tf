output "workgroup_name" {
  value       = aws_athena_workgroup.yelp_workgroup.name
  description = "Athena Workgroup Name"
}

output "workgroup_arn" {
  value       = aws_athena_workgroup.yelp_workgroup.arn
  description = "Athena Workgroup ARN"
}
