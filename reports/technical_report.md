# Spotify Big Data Pipeline Teknik Raporu

## 1. Proje Amacı

Bu projenin amacı, Spotify Tracks Dataset kullanılarak uçtan uca bir büyük veri pipeline'ı kurmaktır. Projede Docker ile konteynerize edilmiş bir çalışma ortamı hazırlanmış, Kafka ile streaming veri üretimi simüle edilmiş, Spark Structured Streaming ile veri işlenmiş, Delta Lake üzerinde Bronze/Silver/Gold katmanları oluşturulmuş ve makine öğrenmesi modelleri ile popularity tahmini yapılmıştır.

## 2. Kullanılan Teknolojiler

- Docker ve Docker Compose
- Apache Kafka
- Apache Zookeeper
- Apache Spark Structured Streaming
- Delta Lake
- Python
- Spark MLlib
- MLflow
- Matplotlib

## 3. Veri Seti

Projede Spotify Tracks Dataset kullanılmıştır. Veri setinde track_id, artists, album_name, track_name, popularity, duration_ms, explicit, danceability, energy, loudness, speechiness, acousticness, instrumentalness, liveness, valence, tempo ve track_genre gibi alanlar bulunmaktadır.

Tahmin hedefi popularity değişkenidir. Bu nedenle problem regresyon problemi olarak ele alınmıştır.

## 4. Sistem Mimarisi

Pipeline akışı:

Spotify CSV Dataset -> Python Kafka Producer -> Kafka Topic -> Spark Structured Streaming -> Delta Bronze -> Delta Silver -> Delta Gold -> EDA -> Feature Engineering -> ML Models + MLflow -> Dashboard

## 5. Docker ve Servisler

Docker Compose ile aşağıdaki servisler ayağa kaldırılmıştır:

- spotify_zookeeper
- spotify_kafka
- spotify_spark
- spotify_mlflow
- spotify_producer

Bu yapı sayesinde proje servisleri izole ve tekrar çalıştırılabilir bir ortamda yönetilmektedir.

## 6. Kafka Streaming

Python Producer, data/spotify_tracks.csv dosyasındaki satırları okuyarak Kafka üzerindeki spotify_tracks topic'ine JSON mesajları göndermektedir. Producer logları üzerinden kaç mesaj gönderildiği takip edilebilmektedir.

## 7. Spark Structured Streaming ve Delta Lake

Spark Structured Streaming ile Kafka'dan gelen JSON mesajları okunmuş, şema tanımlanmış ve Delta Lake formatında katmanlı veri yapısı oluşturulmuştur.

Bronze Layer:
delta/bronze/spotify_tracks

Silver Layer:
delta/silver/spotify_tracks

Gold Analytics Layer:
delta/gold/spotify_analytics

Gold Features Layer:
delta/gold/spotify_tracks_features

## 8. EDA

EDA aşamasında aşağıdaki analizler yapılmıştır:

- Popularity dağılımı
- Eksik değer analizi
- Korelasyon matrisi
- Tür bazlı analizler
- Tempo bucket analizi
- Sayısal değişken dağılımları
- Energy ve danceability ilişkisi
- Mood ve popularity ilişkisi

EDA çıktıları notebooks/eda_plots klasöründe saklanmıştır.

## 9. Feature Engineering

Feature Engineering aşamasında makine öğrenmesi için anlamlı değişkenler üretilmiştir. Üretilen özellikler arasında şarkı süresi, tempo bucket, mood segmentleri, energy-danceability kombinasyonları ve modele uygun sayısal dönüşümler bulunmaktadır.

Feature tablosu Delta Lake Gold katmanına yazılmıştır.

## 10. Makine Öğrenmesi

Problem tipi regresyondur çünkü hedef değişken popularity sayısaldır. Aşağıdaki 5 model eğitilmiştir:

1. Linear Regression
2. Decision Tree Regressor
3. Random Forest Regressor
4. Gradient Boosted Trees Regressor
5. Generalized Linear Regression

Model değerlendirme metrikleri:

- RMSE
- MAE
- R2 Score
- Residual analizi

Model sonuçları reports/step6_regression_results.csv dosyasında saklanmıştır. Model görselleri notebooks/ml_plots klasöründe üretilmiştir.

## 11. MLflow Deney Takibi

MLflow servisi Docker Compose ile http://localhost:5001 adresinde çalışacak şekilde yapılandırılmıştır. Model deneylerinde parametreler, metrikler ve model çıktıları MLflow ile takip edilmiştir. MLflow veritabanı ve artifact klasörü mlflow/ altında tutulmaktadır.

## 12. Dashboard

Dashboard dashboard/index.html olarak hazırlanmıştır. Dashboard içinde:

- 5 modelin performans karşılaştırması
- En iyi model feature importance grafiği
- Gerçek vs tahmin grafikleri
- Residual dağılım grafiği
- Popularity dağılımı
- Eksik değer analizi
- Korelasyon matrisi
- Genre analizi
- Tempo bucket analizi
- Energy vs danceability grafiği

yer almaktadır.

## 13. Karşılaşılan Zorluklar

- Docker Desktop daemon ilk çalıştırmada kapalı olduğu için Docker API bağlantı hatası alınmıştır.
- Spark submit komutlarında Windows PowerShell için satır devam karakteri olarak ters slash yerine backtick kullanılması gerekmiştir.
- Kafka broker hazır olmadan producer başlatıldığında NoBrokersAvailable uyarısı görülmüştür.
- Spark paketleri ilk çalıştırmada Maven üzerinden indirildiği için ilk çalışma uzun sürmüştür.

## 14. Sonuç

Proje kapsamında Docker, Kafka, Spark Structured Streaming, Delta Lake, EDA, Feature Engineering, 5 farklı regresyon modeli, MLflow deney takibi ve dashboard görselleştirme adımları uçtan uca uygulanmıştır. Spotify şarkı özellikleri kullanılarak popularity tahmini için farklı regresyon modelleri karşılaştırılmış ve sonuçlar dashboard ile görsel hale getirilmiştir.

