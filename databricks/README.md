# Databricks Dashboard

Bu klasör, Adım 7 için Databricks üzerinde çalıştırılabilir dashboard notebook dosyasını içerir.

## Dosyalar

- `07_Databricks_Dashboard.py`
- `data/step6_regression_results.csv`
- `data/feature_importance_gbt.csv`

## Databricks Üzerinde Kullanım

1. Databricks Free Edition veya Databricks workspace açılır.
2. GitHub repository workspace'e Repo olarak bağlanır veya bu klasördeki notebook import edilir.
3. `07_Databricks_Dashboard.py` notebook olarak import edilir.
4. Gerekirse CSV dosyaları `/FileStore/tables/` altına upload edilir.
5. Notebook çalıştırılır.
6. Grafik üreten hücreler dashboard'a eklenir.
7. Dashboard sunum sırasında gösterilir.

## Not

Spotify Tracks Dataset içinde doğal bir tarih alanı olmadığı için müzik verisine dayalı zaman serisi grafiği yerine tempo bucket analizi kullanılmıştır.
Streaming tarafında üretilen `event_time` alanı canlı veri akışını göstermek için kullanılabilir.
