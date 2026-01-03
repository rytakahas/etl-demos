resource "google_bigquery_dataset" "raw" {
  project    = var.raw_project_id
  dataset_id = "ryoji_raw_demos"
  location   = var.region
}

resource "google_bigquery_dataset" "wh" {
  project    = var.wh_project_id
  dataset_id = "ryoji_wh_demos"
  location   = var.region
}

