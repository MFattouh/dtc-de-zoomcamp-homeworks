terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  project     = var.gcp_project_id
  region      = "us-central1"
  credentials = file(var.credentials_file)
}

resource "google_storage_bucket" "homework3-bucket" {
  name          = var.bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "homework3-dataset" {
  dataset_id                      = var.bq_dataset_name
  location                        = var.location
  delete_contents_on_destroy      = true
  default_table_expiration_ms     = 86400000 # 24 hours
  default_partition_expiration_ms = 86400000 # 24 hours
}
