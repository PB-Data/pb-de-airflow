# =====================================================================
# Deploy manual laptop (Windows PowerShell) -> VM Airflow (Docker Compose)
#
#   Copy-Item scripts\deploy.env.example scripts\deploy.env   # isi sekali
#   .\scripts\deploy.ps1           # sync dags/
#   .\scripts\deploy.ps1 -Image    # + build ulang image
# Butuh: gcloud CLI, Python (untuk validasi), tar (bawaan Windows 10+).
# =====================================================================
param([switch]$Image)
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

$cfg = @{ REMOTE_DIR = "airflow"; DOCKER = "docker"; IMAGE = "smi-airflow:3.1.6-dlt"; USE_IAP = "false" }
if (Test-Path "scripts\deploy.env") {
    Get-Content "scripts\deploy.env" | Where-Object { $_ -match '^\s*[A-Z_]+\s*=' } | ForEach-Object {
        $k, $v = $_ -split '=', 2
        $v = ($v -split '\s+#')[0]
        $cfg[$k.Trim()] = $v.Trim().Trim('"')
    }
}
foreach ($k in "VM_NAME", "VM_ZONE", "VM_PROJECT") { if (-not $cfg[$k]) { throw "Isi $k di scripts\deploy.env" } }
$flags = @("--zone", $cfg.VM_ZONE, "--project", $cfg.VM_PROJECT)
if ($cfg.USE_IAP -eq "true") { $flags += "--tunnel-through-iap" }
$withImage = if ($Image) { "true" } else { "false" }

Write-Host ">> 1/3 Validasi config & SQL (lokal)"
Push-Location dags
try { python -m medallion.validate; if ($LASTEXITCODE -ne 0) { throw "Validasi gagal" } } finally { Pop-Location }

Write-Host ">> 2/3 Paketkan & upload ke $($cfg.VM_NAME)"
# folder sementara RELATIF di dalam repo: path absolut "C:\..." bisa disalahartikan sebagai
# nama instance oleh `gcloud compute scp`
$tmp = ".deploy_tmp"
Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Path $tmp | Out-Null
try {
    tar --exclude=__pycache__ --exclude=.local --exclude=.dlt -czf "$tmp\medallion_dags.tgz" -C dags .
    if ($LASTEXITCODE -ne 0) { throw "tar gagal" }
    # pastikan line ending LF untuk script bash di VM
    $remote = (Get-Content "scripts\remote_install.sh" -Raw) -replace "`r`n", "`n"
    # .NET memakai current directory proses (bukan Set-Location) -> pakai path absolut di sini
    [IO.File]::WriteAllText((Join-Path (Resolve-Path $tmp).Path "remote_install.sh"), $remote)
    $files = @("$tmp\medallion_dags.tgz", "$tmp\remote_install.sh")
    if ($Image) {
        tar -czf "$tmp\medallion_docker.tgz" -C docker .
        if ($LASTEXITCODE -ne 0) { throw "tar gagal" }
        $files += "$tmp\medallion_docker.tgz"
    }
    gcloud compute scp @files "$($cfg.VM_NAME):/tmp/" @flags
    if ($LASTEXITCODE -ne 0) { throw "Upload gagal" }

    Write-Host ">> 3/3 Pasang di VM"
    gcloud compute ssh $cfg.VM_NAME @flags --command "bash /tmp/remote_install.sh '$($cfg.REMOTE_DIR)' $withImage '$($cfg.IMAGE)' $($cfg.DOCKER)"
    if ($LASTEXITCODE -ne 0) { throw "Install di VM gagal" }
    Write-Host "Deploy selesai."
} finally {
    Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
}
