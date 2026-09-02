variable "aws_region" {
  description = "AWS region for all resources."
  type        = string
  default     = "us-east-2"
}

variable "aws_profile" {
  description = "Local AWS CLI profile used for authentication."
  type        = string
  default     = "de-portfolio"
}

variable "project_name" {
  description = "Prefix for resource names."
  type        = string
  default     = "weather-rides"
}