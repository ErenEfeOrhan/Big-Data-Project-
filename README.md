# Spotify Big Data Pipeline Project

Bu proje, Spotify Tracks Dataset kullanılarak hazırlanmış uçtan uca büyük veri pipeline projesidir.

Amaç, Spotify şarkı verilerini gerçek zamanlı akış gibi Kafka'ya göndermek, Spark Structured Streaming ile bu veriyi işlemek, Delta Lake üzerinde saklamak ve sonraki aşamalarda makine öğrenmesi modelleri ile `popularity` tahmini yapmaktır.

## Kullanılan Teknolojiler

- Docker
- Docker Compose
- Apache Kafka
- Apache Zookeeper
- Apache Spark Structured Streaming
- Delta Lake
- MLflow
- Python
- Matplotlib

## Proje Akışı

```text
Spotify CSV Dataset
        |
        v
Python Kafka Producer
        |
        v
Kafka Topic: spotify_tracks
        |
        v
Spark Structured Streaming
        |
        v
Delta Lake Bronze Layer
        |
        v
Silver Layer
        |
        v
Gold Feature Layer
        |
        v
ML Models + MLflow
        |
        v
Dashboard / Visualizations
```

## Klasör Yapısı

```text
spotify-bigdata-pipeline/
│
├── data/                 # Veri seti buraya konur
├── producer/             # Kafka Producer kodları
├── spark/                # Spark Dockerfile ve Spark job dosyaları
│   └── jobs/
├── notebooks/            # EDA ve ML notebookları
├── delta/                # Delta Lake çıktıları
├── mlflow/               # MLflow çıktıları
├── dashboard/            # Grafik ve dashboard çıktıları
├── reports/              # Teknik rapor
├── scripts/              # Yardımcı çalıştırma scriptleri
│
├── docker-compose.yml
├── requirements.txt
├── .gitignore
└── README.md
```

## Kurulum

### 1. Repoyu bilgisayara indir

```powershell
git clone https://github.com/ErenEfeOrhan/Big-Data-Project-.git
cd Big-Data-Project-
```

### 2. Docker Desktop'ı aç

Docker Desktop bilgisayarda kurulu ve açık olmalıdır.

Kontrol için:

```powershell
docker --version
docker compose version
```

### 3. Veri setini indir

Kaggle üzerinden Spotify Tracks Dataset indirilmelidir.

CSV dosyası şu isimle bu klasöre konmalıdır:

```text
data/spotify_tracks.csv
```

Önemli: Veri dosyası GitHub'a yüklenmez. Her grup üyesi veri dosyasını kendi bilgisayarında `data` klasörüne koymalıdır.

Beklenen kolonlardan bazıları:

```text
track_id, artists, album_name, track_name, popularity, duration_ms,
danceability, energy, loudness, speechiness, acousticness,
instrumentalness, liveness, valence, tempo, track_genre
```

## Docker Servislerini Çalıştırma

### 1. Docker imajlarını oluştur

```powershell
docker compose build
docker compose --profile producer build producer
```

### 2. Ana servisleri başlat

```powershell
docker compose up -d zookeeper kafka spark mlflow
```

### 3. Çalışan container'ları kontrol et

```powershell
docker ps
```

Beklenen container'lar:

```text
spotify_zookeeper
spotify_kafka
spotify_spark
spotify_mlflow
```

## Kafka Producer Çalıştırma

Producer, CSV dosyasını okuyup Kafka'ya JSON mesajları gönderir.

```powershell
docker compose --profile producer up -d producer
```

Producer loglarını kontrol etmek için:

```powershell
docker logs spotify_producer --tail 30
```

Beklenen örnek çıktı:

```text
Kafka connection successful.
Data file: /app/data/spotify_tracks.csv
Kafka topic: spotify_tracks
Message speed: 10 messages per second
Total sent messages: 100
```

## Bronze Delta Streaming Job Çalıştırma

Spark Structured Streaming ile Kafka'dan veri okunur ve Delta Lake Bronze katmanına yazılır.

```powershell
docker exec -it spotify_spark spark-submit --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.2.0" --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" /home/jovyan/work/spark/jobs/stream_kafka_to_bronze.py
```

Başarılı çalışırsa şu klasör oluşur:

```text
delta/bronze/spotify_tracks
```

Kontrol için:

```powershell
dir .\delta\bronze\spotify_tracks
```

Beklenen çıktı içinde şunlar görülür:

```text
_delta_log
part-....snappy.parquet
```

## Servisleri Durdurma

```powershell
docker compose down
```

## MLflow Arayüzü

MLflow çalıştığında tarayıcıdan şu adrese gidilebilir:

```text
http://localhost:5000
```

## Jupyter / Spark Arayüzü

Spark notebook ortamı için:

```text
http://localhost:8888
```

## Şu Ana Kadar Tamamlananlar

- Docker ortamı oluşturuldu.
- Kafka ve Zookeeper servisleri eklendi.
- Python Kafka Producer yazıldı.
- Spotify CSV dosyasından Kafka'ya streaming veri gönderildi.
- Spark Structured Streaming job yazıldı.
- Kafka'dan gelen veri Delta Lake Bronze katmanına yazıldı.
- MLflow servisi eklendi.
- Silver temiz veri katmanı

## Devam Eden Aşamalar

- Gold feature engineering katmanı
- EDA notebookları
- 5 farklı regresyon modeli
- MLflow deney takibi
- Dashboard ve grafikler
- Teknik rapor
- Sunum
'@ | Set-Content README.md -Encoding utf8
