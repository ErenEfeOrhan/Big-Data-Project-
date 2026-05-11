# Spotify Big Data Pipeline - Sunum Planı

## 1. Proje Amacı
Bu projede Spotify Tracks Dataset kullanılarak Docker, Kafka, Spark, Delta Lake, MLflow ve makine öğrenmesi bileşenleriyle uçtan uca büyük veri pipeline kurulmuştur.

## 2. Veri Seti
- Spotify Tracks Dataset
- Hedef değişken: popularity
- Problem tipi: Regresyon
- Örnek kolonlar: track_id, artists, track_name, popularity, duration_ms, danceability, energy, tempo, track_genre

## 3. Mimari
Akış:
Spotify CSV -> Python Kafka Producer -> Kafka Topic -> Spark Structured Streaming -> Delta Bronze/Silver/Gold -> EDA -> Feature Engineering -> ML Models -> MLflow -> Dashboard

## 4. Docker Ortamı
Çalışan servisler:
- Zookeeper
- Kafka
- Spark
- MLflow
- Producer

Canlı demo komutları:
docker ps
docker logs spotify_producer --tail 30

## 5. Kafka Streaming
Producer CSV dosyasını okuyup Kafka topic'ine JSON mesajları gönderir.
Mesajlarda:
- event_time
- event_type
- user_id
- track_id
- şarkı özellikleri

bulunmaktadır.

## 6. Spark + Delta Lake
Katmanlar:
- Bronze: Ham Kafka verisi
- Silver: Temizlenmiş ve parse edilmiş veri
- Gold Analytics: Analitik özetler
- Gold Features: ML için feature tablosu

## 7. EDA
Gösterilecek analizler:
- Popularity dağılımı
- Eksik değer analizi
- Korelasyon matrisi
- Genre analizi
- Zaman serisi trendi
- Energy vs Danceability

## 8. Feature Engineering
Üretilen örnek özellikler:
- duration_min
- tempo_bucket
- mood segmentleri
- energy/danceability kombinasyonları
- explicit dönüşümü
- popularity segmentleri

## 9. Makine Öğrenmesi
5 regresyon modeli:
1. Linear Regression
2. Decision Tree Regressor
3. Random Forest Regressor
4. Gradient Boosted Trees Regressor
5. Generalized Linear Regression

## 10. Değerlendirme Metrikleri
Regresyon metrikleri:
- RMSE
- MAE
- R2 Score
- Residual analizi
- Feature importance

## 11. MLflow
MLflow üzerinde her model ayrı run olarak takip edilmiştir.
Runlarda metrikler, parametreler ve model çıktıları tutulmuştur.

## 12. Dashboard
dashboard/index.html içinde:
- Model karşılaştırması
- Feature importance
- Actual vs Predicted
- Residual Distribution
- EDA grafikleri
- Delta katman özeti

gösterilmektedir.

## 13. Karşılaşılan Zorluklar
- Docker daemon bağlantı hatası
- Windows PowerShell satır devam karakteri
- Kafka broker hazır olmadan producer başlama sorunu
- Spark paketlerinin ilk çalıştırmada uzun indirilmesi

## 14. Sonuç
Proje kapsamında uçtan uca büyük veri pipeline kurulmuş, Spotify şarkılarının popularity değerini tahmin etmek için 5 regresyon modeli karşılaştırılmış ve sonuçlar MLflow ile takip edilip dashboard üzerinde görselleştirilmiştir.
