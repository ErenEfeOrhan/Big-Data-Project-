cd "C:\Users\iclal dere\OneDrive\Desktop\spotify-bigdata-pipeline"

Write-Host ""
Write-Host "========== GIT DURUMU =========="
git status
git log --oneline -5

Write-Host ""
Write-Host "========== 13 MAYIS 2026 21:00 SONRASI COMMIT KONTROL =========="
git log --after="2026-05-13 21:00" --date=iso --pretty=format:"%h | %ad | %s"

Write-Host ""
Write-Host "========== KRITIK DOSYA KONTROL =========="
$required = @(
"docker-compose.yml",
"README.md",
"producer\Dockerfile",
"producer\producer.py",
"kafka\Dockerfile",
"zookeeper\Dockerfile",
"spark\Dockerfile",
"spark\jobs\stream_kafka_to_bronze.py",
"spark\jobs\stream_bronze_to_silver.py",
"spark\jobs\stream_silver_to_gold.py",
"notebooks\1_Docker_Environment.ipynb",
"notebooks\2_Kafka_Streaming.ipynb",
"notebooks\3_Spark_Delta_Pipeline.ipynb",
"notebooks\4_EDA.ipynb",
"notebooks\5_Feature_Engineering.ipynb",
"notebooks\6_ML_Models.ipynb",
"notebooks\7_Dashboard_Visualization.ipynb",
"dashboard\index.html",
"reports\technical_report.md",
"reports\final_checklist.md",
"reports\step6_regression_results.csv",
"databricks\07_Databricks_Dashboard.py",
"databricks\README.md"
)

foreach ($f in $required) {
    if (Test-Path $f) {
        Write-Host "[OK] $f"
    } else {
        Write-Host "[YOK] $f"
    }
}

Write-Host ""
Write-Host "========== PRODUCER MESAJ FORMATI =========="
Select-String -Path "producer\producer.py" -Pattern "event_time|timestamp|user_id|event_type|track_id|MESSAGES_PER_SECOND|Total sent messages" -CaseSensitive:$false

Write-Host ""
Write-Host "========== DATA / DELTA GIT RISK KONTROL =========="
Write-Host "Git'e giren data dosyalari:"
git ls-files data
Write-Host "Git'e giren delta dosyalari:"
git ls-files delta
Write-Host "50MB ustu tracked dosyalar:"
git ls-files | ForEach-Object {
    if (Test-Path $_) {
        $item = Get-Item $_
        if ($item.Length -gt 50MB) {
            Write-Host "$($item.Length) bytes - $_"
        }
    }
}

Write-Host ""
Write-Host "========== DOCKER SERVISLERI =========="
docker ps
docker compose ps

Write-Host ""
Write-Host "========== DELTA KATMANLARI =========="
$deltaPaths = @(
"delta\bronze\spotify_tracks\_delta_log",
"delta\silver\spotify_tracks\_delta_log",
"delta\gold\spotify_analytics\_delta_log",
"delta\gold\spotify_tracks_features\_delta_log"
)
foreach ($d in $deltaPaths) {
    if (Test-Path $d) {
        Write-Host "[OK] $d"
    } else {
        Write-Host "[YOK] $d"
    }
}

Write-Host ""
Write-Host "========== EDA / ML / DASHBOARD CIKTI SAYILARI =========="
Write-Host "EDA plot sayisi:"
(Get-ChildItem notebooks\eda_plots\*.png -ErrorAction SilentlyContinue).Count
Write-Host "ML plot sayisi:"
(Get-ChildItem notebooks\ml_plots\*.png -ErrorAction SilentlyContinue).Count
Write-Host "Dashboard asset sayisi:"
(Get-ChildItem dashboard\assets -Recurse -File -ErrorAction SilentlyContinue).Count

Write-Host ""
Write-Host "========== TIME SERIES IDDIA KONTROL =========="
Select-String -Path "dashboard\index.html","reports\technical_report.md","README.md","databricks\README.md","databricks\07_Databricks_Dashboard.py" -Pattern "Time Series|time_series|Zaman serisi|zaman serisi" -CaseSensitive:$false

Write-Host ""
Write-Host "========== SON GIT DURUMU =========="
git status
