output "bucket_name" {
  description = "Name of the S3 data lake bucket."
  value       = aws_s3_bucket.lake.id
}

output "bucket_arn" {
  description = "ARN of the S3 data lake bucket."
  value       = aws_s3_bucket.lake.arn
}