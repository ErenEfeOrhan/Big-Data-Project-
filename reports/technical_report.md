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

  Her teknoloji pipeline içerisinde farklı bir görevi yerine getirmektedir:

- Docker servis izolasyonu sağlamaktadır.
- Kafka streaming veri aktarımı için kullanılmaktadır.
- Spark büyük veri işleme motoru olarak görev almaktadır.
- Delta Lake ACID destekli veri gölü mimarisi sağlamaktadır.
- MLflow model deney takibi için kullanılmaktadır.
- Matplotlib veri görselleştirmelerinde kullanılmıştır.

## 3. Veri Seti

Projede Spotify Tracks Dataset kullanılmıştır. Veri setinde track_id, artists, album_name, track_name, popularity, duration_ms, explicit, danceability, energy, loudness, speechiness, acousticness, instrumentalness, liveness, valence, tempo ve track_genre gibi alanlar bulunmaktadır.

Tahmin hedefi popularity değişkenidir. Bu nedenle problem regresyon problemi olarak ele alınmıştır.

## 4. Sistem Mimarisi

Pipeline akışı:

Spotify CSV Dataset -> Python Kafka Producer -> Kafka Topic -> Spark Structured Streaming -> Delta Bronze -> Delta Silver -> Delta Gold -> EDA -> Feature Engineering -> ML Models + MLflow -> Dashboard

Bu yapı modern lakehouse mimarisine uygun şekilde katmanlı veri işleme yaklaşımı kullanmaktadır.

## Bronze Layer

Ham verilerin tutulduğu katmandır. Kafka’dan gelen veri minimum dönüşüm ile saklanmıştır.

## Silver Layer

Temizlenmiş ve doğrulanmış veri katmanıdır. Null kontrolü, veri tipi dönüşümleri ve temel preprocessing işlemleri yapılmıştır.

## Gold Layer

Analitik ve makine öğrenmesi için optimize edilmiş veri katmanıdır. Dashboard ve ML modelleri bu katmandaki verileri kullanmaktadır.

Bu mimari sayesinde veri kaybı azaltılmış, veri yönetilebilirliği artırılmış ve ölçeklenebilir bir yapı oluşturulmuştur.

## 5. Docker ve Servisler

Docker Compose ile aşağıdaki servisler ayağa kaldırılmıştır:

- spotify_zookeeper
- spotify_kafka
- spotify_spark
- spotify_mlflow
- spotify_producer

Docker kullanımının avantajları:

- Ortam bağımsız çalıştırma
- Kolay deployment
- Servis izolasyonu
- Tek komut ile tüm sistemin ayağa kaldırılması
- Yeniden üretilebilir geliştirme ortamı

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

Makine öğrenmesi modellerinin tahmin performansını artırmak amacıyla veri seti üzerinde ek özellik mühendisliği işlemleri uygulanmıştır. Bu aşamada mevcut değişkenlerden yeni anlamlı özellikler türetilmiş ve modelin şarkı popülerliğini daha doğru tahmin edebilmesi hedeflenmiştir.

Eklenen yeni özellikler aşağıdaki gibidir:

- `artist_count`  
  Şarkıda yer alan sanatçı sayısını ifade etmektedir. Özellikle ortak çalışmaların (collaboration) popularity üzerindeki etkisini analiz etmek amacıyla eklenmiştir.

- `is_short_track`  
  Şarkının 2.5 dakikadan kısa olup olmadığını belirten binary özelliktir. Kısa süreli şarkıların modern müzik trendlerinde daha sık tercih edilmesi nedeniyle modele dahil edilmiştir.

- `energy_dance_interaction`  
  Energy ve danceability değişkenlerinin çarpımı ile oluşturulan etkileşim özelliğidir. Yüksek enerjili ve dans edilebilir şarkıların popularity üzerindeki ortak etkisini ölçmek için kullanılmıştır.

- `mood_index`  
  Valence ve energy değişkenlerinin çarpımı ile oluşturulmuştur. Şarkının genel pozitiflik ve coşku seviyesini temsil eden birleşik bir özellik olarak modele eklenmiştir.

- `is_feat`  
  Şarkı isminde “ft.” veya “feat” ifadesi bulunup bulunmadığını belirleyen binary özelliktir. Düet veya ortak çalışmaların popularity üzerindeki etkisini incelemek amacıyla kullanılmıştır.

Bu özellikler sayesinde veri seti yalnızca ham müzik verilerinden oluşan bir yapı olmaktan çıkarılmış, modele daha fazla davranışsal ve müzikal bağlam sağlayan zenginleştirilmiş bir feature set oluşturulmuştur.

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

Proje geliştirme sürecinde farklı büyük veri teknolojilerinin birlikte çalıştırılması nedeniyle çeşitli teknik problemler ve yapılandırma zorlukları ile karşılaşılmıştır. Özellikle Docker, Kafka ve Spark servislerinin aynı ortam içerisinde senkronize çalıştırılması sırasında servis bağımlılıkları önemli hale gelmiştir.

- Docker Desktop daemon ilk çalıştırmada kapalı olduğu için Docker API bağlantı hatası alınmıştır. Bu nedenle container servisleri başlangıçta ayağa kaldırılamamış ve Docker servislerinin manuel olarak yeniden başlatılması gerekmiştir.

- Spark submit komutlarında Windows PowerShell ortamında satır devam karakteri problemi yaşanmıştır. Linux terminalinde kullanılan ters slash (`\`) karakteri yerine PowerShell üzerinde backtick (`` ` ``) kullanılması gerektiği için komut yapıları yeniden düzenlenmiştir.

- Kafka broker tamamen hazır olmadan producer başlatıldığında `NoBrokersAvailable` uyarısı görülmüştür. Bu problem servislerin başlatılma sırasının önemini göstermiştir. Çözüm olarak Kafka servisinin tam olarak ayağa kalkması beklendikten sonra producer çalıştırılmıştır.

- Spark paketleri ilk çalıştırmada Maven repository üzerinden indirildiği için başlangıç süresi uzun olmuştur. Özellikle Delta Lake ve Spark SQL bağımlılıklarının ilk kez yüklenmesi ek süre oluşturmuştur.

- Kafka üzerinden gelen JSON verilerinin Spark tarafında işlenmesi sırasında schema uyumsuzlukları ve veri tipi dönüşüm problemleri yaşanmıştır. Özellikle null değerler ve eksik alanlar parsing işlemlerinde hata oluşturmuştur. Bu problem explicit schema tanımları ile çözülmüştür.

- Docker Compose ortamında Spark, Kafka, Producer ve MLflow servislerinin aynı anda çalıştırılması sırasında container senkronizasyon problemleri yaşanmıştır. Bazı servislerin diğer servisler tamamen ayağa kalkmadan çalışmaya başlaması bağlantı hatalarına neden olmuştur.

Karşılaşılan bu problemler, dağıtık sistemler ve streaming veri mimarilerinde servis yönetimi, container orchestration ve veri işleme süreçlerinin ne kadar kritik olduğunu göstermiştir. Aynı zamanda proje sürecinde gerçek dünya büyük veri sistemlerine benzer teknik deneyimler kazanılmıştır.

---

## 14. Sonuç

Bu proje kapsamında modern büyük veri teknolojileri kullanılarak uçtan uca çalışan bir Big Data Pipeline sistemi geliştirilmiştir. Projede veri üretiminden veri işlenmesine, veri gölü mimarisinden makine öğrenmesi modellemelerine kadar birçok süreç entegre şekilde uygulanmıştır.

Pipeline içerisinde Docker ile container tabanlı çalışma ortamı oluşturulmuş, Kafka ile streaming veri akışı sağlanmış ve Spark Structured Streaming ile gerçek zamanlı veri işleme gerçekleştirilmiştir. İşlenen veriler Delta Lake üzerinde Bronze, Silver ve Gold katmanları halinde saklanarak modern lakehouse mimarisi uygulanmıştır.

Veri analizi aşamasında Exploratory Data Analysis (EDA) çalışmaları gerçekleştirilmiş, veri setindeki ilişkiler incelenmiş ve popularity üzerinde etkili olabilecek değişkenler analiz edilmiştir. Daha sonra Feature Engineering adımında modele katkı sağlayabilecek yeni özellikler üretilmiş ve veri seti makine öğrenmesi için optimize edilmiştir.

Makine öğrenmesi aşamasında:

- Linear Regression
- Decision Tree Regressor
- Random Forest Regressor
- Gradient Boosted Trees Regressor
- Generalized Linear Regression

olmak üzere 5 farklı regresyon modeli eğitilmiş ve performansları karşılaştırılmıştır. Modeller RMSE, MAE ve R² Score gibi metrikler ile değerlendirilmiştir.

MLflow kullanılarak model deneyleri, parametreler ve performans metrikleri takip edilmiş; böylece deney yönetimi ve model versiyonlama süreçleri merkezi hale getirilmiştir.

Ayrıca dashboard yapısı ile model sonuçları, veri analizleri ve görselleştirme çıktıları kullanıcı dostu şekilde sunulmuştur. Böylece hem teknik hem de görsel açıdan kapsamlı bir veri platformu oluşturulmuştur.

Sonuç olarak bu proje yalnızca bir makine öğrenmesi çalışması değil; aynı zamanda gerçek dünyadaki veri mühendisliği ve büyük veri sistemlerine benzer şekilde tasarlanmış ölçeklenebilir, katmanlı ve modern bir Big Data mimarisi örneğidir. Proje kapsamında veri mühendisliği, streaming sistemleri, veri analitiği, feature engineering, makine öğrenmesi ve deney yönetimi süreçleri tek bir platform üzerinde başarılı şekilde birleştirilmiştir.

