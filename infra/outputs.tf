output "bucket_name" {
  description = "Name of the S3 data lake bucket."
  value       = aws_s3_bucket.lake.id
}

output "bucket_arn" {
  description = "ARN of the S3 data lake bucket."
  value       = aws_s3_bucket.lake.arn
}

output "glue_database" {
  description = "Glue catalog database name."
  value       = aws_glue_catalog_database.lake.name
}

output "athena_workgroup" {
  description = "Athena workgroup name."
  value       = aws_athena_workgroup.main.name
}