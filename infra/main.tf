# S3 bucket names are globally unique across all AWS accounts,
# so a random suffix avoids collisions.
resource "random_id" "suffix" {
  byte_length = 4
}

locals {
  bucket_name = "${var.project_name}-lake-${random_id.suffix.hex}"
}

resource "aws_s3_bucket" "lake" {
  bucket = local.bucket_name
}

# Block all public access. Data lakes leaking publicly is a
# well-documented category of breach; this is non-negotiable.
resource "aws_s3_bucket_public_access_block" "lake" {
  bucket = aws_s3_bucket.lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Athena writes query results to S3. Expiring them keeps storage
# costs from creeping up on a free-tier account.
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "expire-athena-results"
    status = "Enabled"

    filter {
      prefix = "athena-results/"
    }

    expiration {
      days = 7
    }
  }
}

resource "aws_glue_catalog_database" "lake" {
  name        = replace("${var.project_name}_lake", "-", "_")
  description = "Catalog for NYC taxi trip data in S3."
}

resource "aws_glue_catalog_table" "trips" {
  name          = "trips"
  database_name = aws_glue_catalog_database.lake.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification        = "parquet"
    "parquet.compression" = "SNAPPY"
    EXTERNAL              = "TRUE"
  }

  # Partition keys are NOT stored in the files — they come from the
  # S3 path. Filtering on these lets Athena skip whole prefixes.
  partition_keys {
    name = "year"
    type = "string"
  }

  partition_keys {
    name = "month"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.id}/trips/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "tpep_pickup_datetime"
      type = "timestamp"
    }

    columns {
      name = "tpep_dropoff_datetime"
      type = "timestamp"
    }

    columns {
      name = "passenger_count"
      type = "double"
    }

    columns {
      name = "trip_distance"
      type = "double"
    }

    columns {
      name = "pulocationid"
      type = "bigint"
    }

    columns {
      name = "dolocationid"
      type = "bigint"
    }

    columns {
      name = "payment_type"
      type = "bigint"
    }

    columns {
      name = "fare_amount"
      type = "double"
    }

    columns {
      name = "tip_amount"
      type = "double"
    }

    columns {
      name = "total_amount"
      type = "double"
    }
  }
}

resource "aws_athena_workgroup" "main" {
  name = var.project_name

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.id}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }

    # Hard cap: any single query scanning more than 1 GB is killed.
    # This is the guardrail that makes a runaway bill impossible.
    bytes_scanned_cutoff_per_query = 1073741824
  }

  force_destroy = true
}