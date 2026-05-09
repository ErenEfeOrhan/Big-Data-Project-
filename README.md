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
Gold Layer
        |
        v
EDA
        |
        v
Feature Engineering
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

## Docker Servislerini Çalıştırma (Adım 1)

### 1. Docker image'larını oluştur

```powershell
docker compose build
docker compose --profile producer build producer
```

Not: `build` komutları sadece image oluşturur, container başlatmaz.

### 2. Ana servisleri başlat

```powershell
docker compose up -d zookeeper kafka spark mlflow
```

Not: `producer` servisi `profiles: producer` altında olduğu için bu komutla başlamaz.

### 3. Producer'ı başlat

```powershell
docker compose --profile producer up -d producer
```

### 4. Container durumunu kontrol et

```powershell
docker ps
```

Beklenen container'lar:

```text
spotify_zookeeper
spotify_kafka
spotify_spark
spotify_mlflow
spotify_producer
```

## Kafka Producer Kontrolü (Adım 2)

Producer, CSV dosyasını okuyup Kafka topic'ine JSON mesaj gönderir.

Producer loglarını kontrol etmek için:

```powershell
docker logs spotify_producer --tail 30
```

Beklenen örnek çıktı:

```text
Kafka connection successful.
Data file: /app/data/spotify_tracks.csv
Kafka topic: spotify_tracks
Message speed: 50 messages per second
Total sent messages: 100
```

Mesaj hızı `MESSAGES_PER_SECOND` environment değişkeniyle ayarlanabilir.

## Spark Streaming Job'ları (Adım 3)

3 ayrı terminal açıp job'ları paralel çalıştır:

Terminal 1:

```powershell
bash scripts/run_bronze_stream.sh
```

Terminal 2:

```powershell
bash scripts/run_silver_stream.sh
```

Terminal 3:

```powershell
bash scripts/run_gold_stream.sh
```

Not: Job'lar varsayılan olarak sürekli çalışır. Test için süreli çalıştırmak istersen:

```powershell
docker exec -e RUN_SECONDS=180 spotify_spark spark-submit --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.2.0" --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" /home/jovyan/work/spark/jobs/stream_kafka_to_bronze.py
```

## Delta Katman Kontrolü

Adım 3 başarılıysa aşağıdaki klasörler oluşur:

```text
delta/bronze/spotify_tracks
delta/silver/spotify_tracks
delta/gold/spotify_analytics
```

Kontrol için:

```powershell
ls -la delta/bronze/spotify_tracks/_delta_log
ls -la delta/silver/spotify_tracks/_delta_log
ls -la delta/gold/spotify_analytics/_delta_log
```

## EDA (Adım 4)

EDA notebook'u Jupyter Lab üzerinden çalıştırılır:

1. Spark container'ının çalıştığından emin ol.
2. Tarayıcıda `http://localhost:8888` adresine git.
3. `work/notebooks/4_EDA.ipynb` dosyasını aç.
4. Hücreleri sırayla çalıştır.

Notebook Silver katmanından veriyi okur ve grafikleri `notebooks/eda_plots/` altına kaydeder.

## Feature Engineering (Adım 5)

`notebooks/5_Feature_Engineering.ipynb` dosyası:

- Silver veriden en az 5 feature üretir.
- Feature mantıklarını notebook içinde açıklar.
- Çıktıyı Delta Gold path'ine yazar.

Gold feature path:

```text
delta/gold/spotify_tracks_features
```

## Katman Ayrımı (Adım 3 vs Adım 5)

- Adım 3 Gold (`delta/gold/spotify_analytics`): Silver veriden streaming özet/analytics tablo üretir.
- Adım 5 Gold (`delta/gold/spotify_tracks_features`): ML modeli için feature engineering ile özellik tablosu üretir.

## Servisleri Durdurma

Streaming job'lar çalıştığı terminalde `Ctrl+C` ile durdurulur.

Tüm container'ları kapatmak için:

```powershell
docker compose down
```

## Erişim Adresleri

Spark/Jupyter:

```text
http://localhost:8888
```

MLflow:

```text
http://localhost:5001
```

## Şu Ana Kadar Tamamlananlar

- Docker ortamı oluşturuldu (Adım 1) ✅
- Kafka ve Zookeeper servisleri eklendi (Adım 1) ✅
- Python Kafka Producer yazıldı (Adım 2) ✅
- Spotify CSV dosyasından Kafka'ya streaming veri gönderildi (Adım 2) ✅
- Spark Structured Streaming job'ları yazıldı (Adım 3) ✅
- Kafka'dan gelen veri Delta Lake Bronze katmanına yazıldı (Adım 3) ✅
- Silver temiz veri katmanı oluşturuldu (Adım 3) ✅
- Gold analytics katmanı oluşturuldu (Adım 3) ✅
- EDA notebook'u oluşturuldu (Adım 4) ✅
- Feature engineering notebook'u oluşturuldu ve Gold feature tablosu yazıldı (Adım 5) ✅
- MLflow servisi eklendi ✅

## Devam Eden Aşamalar

- 5 farklı regresyon modeli (Adım 6)
- MLflow deney takibi (Adım 6)
- Dashboard ve grafikler (Adım 7)
- Teknik rapor
- Sunum
