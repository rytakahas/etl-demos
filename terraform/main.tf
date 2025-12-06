terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_bigquery_dataset" "raw" {
  dataset_id = "ryoji_raw_demos"
  project    = var.raw_project_id
  location   = "EU"
}

resource "google_bigquery_dataset" "wh" {
  dataset_id = "bank_dwh_demo"
  project    = var.wh_project_id
  location   = "EU"
}
