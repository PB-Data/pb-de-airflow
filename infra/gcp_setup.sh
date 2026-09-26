#!/usr/bin/env bash
# =====================================================================
# Setup GCP sekali jalan (oleh admin yang punya akses ke semua project).
# Bisa dijalankan di Cloud Shell. Isi variabel di bawah dulu.
#
#   1. Service account (SA) VM Airflow  -> akses BigQuery + bucket data lake di project smi-dwh
#   2. Bucket data lake (region sama dengan dataset BigQuery)
#   3. IP publik VM dijadikan STATIS + didaftarkan di authorized networks Cloud SQL
#   4. Cek access scope VM (default scope Compute Engine TIDAK mencakup BigQuery)
# =====================================================================
set -euo pipefail

# ---------- ISI DI SINI ----------
BQ_PROJECT="smi-dwh"                 # project BigQuery + bucket data lake
REGION="asia-southeast2"             # Jakarta (samakan dengan bq_location di platform.yml)
LAKE_BUCKET="smi-dwh-datalake"       # samakan dengan lake.bucket_url di platform.yml
VM_PROJECT="<project-vm-airflow>"
VM_NAME="<nama-vm-airflow>"
VM_ZONE="asia-southeast2-a"
SQL_PROJECT="<project-cloud-sql>"
SQL_INSTANCE="<nama-instance-cloud-sql>"
STATIC_IP_NAME="airflow-vm-ip"
APPLY_LIFECYCLE="false"             # true = file lake >30 hari pindah ke Nearline, >180 hari ke Coldline
# ---------------------------------

echo ">> Service account VM"
VM_SA=$(gcloud compute instances describe "$VM_NAME" --project "$VM_PROJECT" --zone "$VM_ZONE" \
  --format='value(serviceAccounts[0].email)')
SCOPES=$(gcloud compute instances describe "$VM_NAME" --project "$VM_PROJECT" --zone "$VM_ZONE" \
  --format='value(serviceAccounts[0].scopes)')
echo "   SA     : $VM_SA"
echo "   scopes : $SCOPES"
if [[ "$SCOPES" != *"cloud-platform"* ]]; then
  cat <<EOF
   !! Access scope VM belum 'cloud-platform' -> dlt/Airflow akan kena 403 ke BigQuery walau IAM benar.
      Perlu STOP VM dulu, lalu:
        gcloud compute instances stop $VM_NAME --project $VM_PROJECT --zone $VM_ZONE
        gcloud compute instances set-service-account $VM_NAME --project $VM_PROJECT --zone $VM_ZONE \\
          --service-account $VM_SA --scopes cloud-platform
        gcloud compute instances start $VM_NAME --project $VM_PROJECT --zone $VM_ZONE
EOF
fi

echo ">> Enable API di project BigQuery"
gcloud services enable bigquery.googleapis.com storage.googleapis.com --project "$BQ_PROJECT"

echo ">> Bucket data lake gs://$LAKE_BUCKET ($REGION)"
if ! gcloud storage buckets describe "gs://$LAKE_BUCKET" --project "$BQ_PROJECT" >/dev/null 2>&1; then
  gcloud storage buckets create "gs://$LAKE_BUCKET" --project "$BQ_PROJECT" --location "$REGION" \
    --uniform-bucket-level-access --public-access-prevention
fi
if [[ "$APPLY_LIFECYCLE" == "true" ]]; then
  # file lama pindah ke storage class lebih murah (lihat infra/lake_lifecycle.json)
  gcloud storage buckets update "gs://$LAKE_BUCKET" --lifecycle-file="$(dirname "$0")/lake_lifecycle.json"
fi

echo ">> IAM lintas project untuk $VM_SA"
# jobUser: menjalankan load/query job di project smi-dwh (billing di project DWH)
# dataEditor (level project): dlt & ensure_datasets bisa membuat dataset bronze/silver/gold
#   (least privilege: buat dataset manual lalu beri dataEditor per dataset saja)
for ROLE in roles/bigquery.jobUser roles/bigquery.dataEditor; do
  gcloud projects add-iam-policy-binding "$BQ_PROJECT" --member "serviceAccount:$VM_SA" \
    --role "$ROLE" --condition=None >/dev/null
  echo "   $ROLE @ $BQ_PROJECT"
done
gcloud storage buckets add-iam-policy-binding "gs://$LAKE_BUCKET" \
  --member "serviceAccount:$VM_SA" --role roles/storage.objectAdmin >/dev/null
echo "   roles/storage.objectAdmin @ gs://$LAKE_BUCKET"

echo ">> IP publik VM -> statis"
VM_REGION="${VM_ZONE%-*}"   # asia-southeast2-a -> asia-southeast2
VM_IP=$(gcloud compute instances describe "$VM_NAME" --project "$VM_PROJECT" --zone "$VM_ZONE" \
  --format='value(networkInterfaces[0].accessConfigs[0].natIP)')
if [[ -z "$VM_IP" ]]; then
  echo "   !! VM tidak punya IP publik (mungkin keluar lewat Cloud NAT) -> daftarkan IP NAT-nya di Cloud SQL."
else
  RESERVED=$(gcloud compute addresses list --project "$VM_PROJECT" --filter="address=$VM_IP" --format='value(name)')
  if [[ -n "$RESERVED" ]]; then
    echo "   IP $VM_IP sudah statis (nama: $RESERVED)"
  else
    # promote IP ephemeral yang sedang dipakai jadi statis (IP tidak berubah)
    gcloud compute addresses create "$STATIC_IP_NAME" --project "$VM_PROJECT" --region "$VM_REGION" --addresses "$VM_IP"
  fi
  echo "   IP VM: $VM_IP"

  echo ">> Authorized networks Cloud SQL $SQL_INSTANCE"
  # HATI-HATI: --authorized-networks MENIMPA seluruh list -> gabungkan dengan yang sudah ada
  EXISTING=$(gcloud sql instances describe "$SQL_INSTANCE" --project "$SQL_PROJECT" \
    --format='value(settings.ipConfiguration.authorizedNetworks[].value)' | tr ';' ',')
  if [[ ",$EXISTING," == *",$VM_IP/32,"* || ",$EXISTING," == *",$VM_IP,"* ]]; then
    echo "   sudah terdaftar"
  else
    echo "   list lama: ${EXISTING:-<kosong>}"
    gcloud sql instances patch "$SQL_INSTANCE" --project "$SQL_PROJECT" \
      --authorized-networks "${EXISTING:+$EXISTING,}$VM_IP/32" --quiet
  fi
fi

# Opsional (disarankan): tolak koneksi tanpa enkripsi ke Cloud SQL.
# PASTIKAN semua client lain sudah pakai SSL dulu sebelum menjalankan ini!
# gcloud sql instances patch "$SQL_INSTANCE" --project "$SQL_PROJECT" --ssl-mode=ENCRYPTED_ONLY

cat <<EOF

Selesai. Berikutnya:
  1. Buat login read-only di SQL Server: infra/sqlserver_readonly_login.sql
  2. Tes dari VM:  nc -vz <IP-public-Cloud-SQL> 1433
  3. Buat Airflow Connection mssql_sqlserver_main (lihat README)
EOF
