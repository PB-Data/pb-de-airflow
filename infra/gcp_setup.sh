#!/usr/bin/env bash
# Setup GCP sekali jalan, oleh admin yang punya akses ke semua project (bisa di Cloud Shell).
# Isi variabel di bawah dulu. Aman dijalankan ulang.
#
#   1. Service account (SA) VM Airflow -> BigQuery + bucket lake di project BigQuery
#   2. Bucket data lake (region sama dengan dataset BigQuery)
#   3. IP publik VM dijadikan statis + didaftarkan di authorized networks Cloud SQL
#   4. Cek access scope VM (default scope Compute Engine TIDAK mencakup BigQuery)
set -euo pipefail

# ---------- ISI DI SINI ----------
BQ_PROJECT="smi-dwh"                # samakan dengan gcp.bq_project di dags/configs/platform.yml
REGION="asia-southeast2"            # samakan dengan gcp.bq_location
LAKE_BUCKET="smi-dwh-datalake"      # samakan dengan lake.bucket_url
VM_PROJECT="smi-dwh"
VM_NAME="airflow-prod"
VM_ZONE="asia-southeast2-c"
SQL_PROJECT="<project-cloud-sql>"
SQL_INSTANCE="<nama-instance-cloud-sql>"
# ---------------------------------

vm() { gcloud compute instances describe "$VM_NAME" --project "$VM_PROJECT" --zone "$VM_ZONE" --format="value($1)"; }

echo ">> Service account VM"
VM_SA=$(vm 'serviceAccounts[0].email')
echo "   $VM_SA"
if [[ "$(vm 'serviceAccounts[0].scopes')" != *cloud-platform* ]]; then
  cat <<EOF
   !! Access scope VM belum cloud-platform: dlt/Airflow akan 403 ke BigQuery walau IAM benar.
      VM harus di-stop dulu:
        gcloud compute instances stop $VM_NAME --project $VM_PROJECT --zone $VM_ZONE
        gcloud compute instances set-service-account $VM_NAME --project $VM_PROJECT --zone $VM_ZONE \\
          --service-account $VM_SA --scopes cloud-platform
        gcloud compute instances start $VM_NAME --project $VM_PROJECT --zone $VM_ZONE
EOF
fi

echo ">> API & bucket gs://$LAKE_BUCKET ($REGION)"
gcloud services enable bigquery.googleapis.com storage.googleapis.com --project "$BQ_PROJECT"
gcloud storage buckets describe "gs://$LAKE_BUCKET" --project "$BQ_PROJECT" >/dev/null 2>&1 \
  || gcloud storage buckets create "gs://$LAKE_BUCKET" --project "$BQ_PROJECT" --location "$REGION" \
       --uniform-bucket-level-access --public-access-prevention

echo ">> IAM lintas project untuk $VM_SA"
# jobUser: menjalankan load/query job (billing di project BigQuery).
# dataEditor level project: dlt & ensure_datasets membuat dataset bronze/silver/gold sendiri.
for ROLE in roles/bigquery.jobUser roles/bigquery.dataEditor; do
  gcloud projects add-iam-policy-binding "$BQ_PROJECT" --member "serviceAccount:$VM_SA" \
    --role "$ROLE" --condition=None >/dev/null
  echo "   $ROLE @ $BQ_PROJECT"
done
gcloud storage buckets add-iam-policy-binding "gs://$LAKE_BUCKET" \
  --member "serviceAccount:$VM_SA" --role roles/storage.objectAdmin >/dev/null
echo "   roles/storage.objectAdmin @ gs://$LAKE_BUCKET"

echo ">> IP publik VM -> statis + authorized networks Cloud SQL"
VM_IP=$(vm 'networkInterfaces[0].accessConfigs[0].natIP')
if [[ -z "$VM_IP" ]]; then
  echo "   !! VM tanpa IP publik (mungkin lewat Cloud NAT): daftarkan IP NAT-nya di Cloud SQL secara manual."
  exit 0
fi
if [[ -z "$(gcloud compute addresses list --project "$VM_PROJECT" --filter="address=$VM_IP" --format='value(name)')" ]]; then
  # IP ephemeral yang sedang dipakai dijadikan statis (IP-nya tidak berubah)
  gcloud compute addresses create airflow-vm-ip --project "$VM_PROJECT" --region "${VM_ZONE%-*}" --addresses "$VM_IP"
fi
# HATI-HATI: --authorized-networks MENIMPA seluruh daftar, jadi gabungkan dengan yang sudah ada.
EXISTING=$(gcloud sql instances describe "$SQL_INSTANCE" --project "$SQL_PROJECT" \
  --format='value(settings.ipConfiguration.authorizedNetworks[].value)' | tr ';' ',')
if [[ ",$EXISTING," == *",$VM_IP/32,"* || ",$EXISTING," == *",$VM_IP,"* ]]; then
  echo "   $VM_IP sudah terdaftar"
else
  gcloud sql instances patch "$SQL_INSTANCE" --project "$SQL_PROJECT" \
    --authorized-networks "${EXISTING:+$EXISTING,}$VM_IP/32" --quiet
fi

cat <<EOF

Selesai. Berikutnya:
  1. Login read-only di SQL Server: infra/sqlserver_readonly_login.sql
  2. Tes dari VM: nc -vz <IP publik Cloud SQL> 1433
  3. Airflow Connection mssql_pb (README)
EOF
