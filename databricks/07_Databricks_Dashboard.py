# Databricks notebook source
# MAGIC %md
# MAGIC # Adım 7 - Databricks Dashboard ve Görselleştirme
# MAGIC
# MAGIC Bu notebook, Spotify Big Data Pipeline projesinin Databricks üzerinde dashboard olarak gösterilebilmesi için hazırlanmıştır.
# MAGIC
# MAGIC Dashboard içeriği:
# MAGIC
# MAGIC - 5 regresyon modelinin performans karşılaştırması
# MAGIC - Feature importance analizi
# MAGIC - Popularity dağılımı
# MAGIC - Genre dağılımı
# MAGIC - Explicit dağılımı
# MAGIC - Tempo bucket analizi
# MAGIC - Actual vs Predicted scatter plot
# MAGIC - Residual dağılım analizi
# MAGIC
# MAGIC Not: Spotify Tracks Dataset içinde gerçek tarih bazlı bir şarkı zamanı alanı bulunmadığı için müzik verisi üzerinden zaman serisi iddiası yapılmamıştır. Streaming sırasında üretilen event_time alanı canlı veri akışı kanıtı olarak kullanılabilir.

# COMMAND ----------

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Veri Yolları
# MAGIC
# MAGIC Databricks Repo olarak GitHub reposu bağlanırsa aşağıdaki relative path kullanılabilir.
# MAGIC Eğer dosyaları manuel upload ederseniz CSV dosyalarını /FileStore/tables altına koyup pathleri güncelleyin.

# COMMAND ----------

MODEL_RESULTS_PATH = "databricks/data/step6_regression_results.csv"
FEATURE_IMPORTANCE_PATH = "databricks/data/feature_importance_gbt.csv"
SPOTIFY_CSV_PATH = "data/spotify_tracks.csv"

if not os.path.exists(MODEL_RESULTS_PATH):
    MODEL_RESULTS_PATH = "/dbfs/FileStore/tables/step6_regression_results.csv"

if not os.path.exists(FEATURE_IMPORTANCE_PATH):
    FEATURE_IMPORTANCE_PATH = "/dbfs/FileStore/tables/feature_importance_gbt.csv"

if not os.path.exists(SPOTIFY_CSV_PATH):
    SPOTIFY_CSV_PATH = "/dbfs/FileStore/tables/spotify_tracks.csv"

print("MODEL_RESULTS_PATH:", MODEL_RESULTS_PATH)
print("FEATURE_IMPORTANCE_PATH:", FEATURE_IMPORTANCE_PATH)
print("SPOTIFY_CSV_PATH:", SPOTIFY_CSV_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Model Performans Sonuçları

# COMMAND ----------

model_results = pd.read_csv(MODEL_RESULTS_PATH)
display(model_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 5 Model Performans Karşılaştırması
# MAGIC
# MAGIC Bu grafik, RMSE, MAE ve R2 metrikleri ile modellerin karşılaştırmasını gösterir.
# MAGIC Databricks üzerinde bu hücre çıktısını dashboard'a ekleyin.

# COMMAND ----------

metrics = [col for col in ["rmse", "mae", "r2", "RMSE", "MAE", "R2"] if col in model_results.columns]
model_col = "model" if "model" in model_results.columns else model_results.columns[0]

plot_df = model_results.copy()
plot_df.columns = [c.lower() for c in plot_df.columns]
model_col = "model" if "model" in plot_df.columns else plot_df.columns[0]

available_metrics = [m for m in ["rmse", "mae", "r2"] if m in plot_df.columns]

ax = plot_df.set_index(model_col)[available_metrics].plot(kind="bar", figsize=(12, 6))
plt.title("5 Regresyon Modeli Performans Karşılaştırması")
plt.xlabel("Model")
plt.ylabel("Metrik Değeri")
plt.xticks(rotation=30, ha="right")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Feature Importance

# COMMAND ----------

if os.path.exists(FEATURE_IMPORTANCE_PATH):
    fi = pd.read_csv(FEATURE_IMPORTANCE_PATH)
    display(fi)

    fi_columns = [c.lower() for c in fi.columns]
    fi.columns = fi_columns

    feature_col = "feature" if "feature" in fi.columns else fi.columns[0]
    importance_col = "importance" if "importance" in fi.columns else fi.columns[-1]

    fi_plot = fi.sort_values(importance_col, ascending=True).tail(15)

    plt.figure(figsize=(10, 7))
    plt.barh(fi_plot[feature_col], fi_plot[importance_col])
    plt.title("Feature Importance - En Etkili Özellikler")
    plt.xlabel("Importance")
    plt.ylabel("Feature")
    plt.tight_layout()
    plt.show()
else:
    print("Feature importance CSV bulunamadi:", FEATURE_IMPORTANCE_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Spotify Veri Seti EDA Görselleri
# MAGIC
# MAGIC Bu bölüm veri dağılımı ve EDA bulgularını Databricks üzerinde üretir.

# COMMAND ----------

spotify = pd.read_csv(SPOTIFY_CSV_PATH)
print("Satır sayısı:", len(spotify))
print("Kolon sayısı:", len(spotify.columns))
display(spotify.head(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Popularity Dağılımı

# COMMAND ----------

plt.figure(figsize=(10, 5))
plt.hist(spotify["popularity"].dropna(), bins=30)
plt.title("Popularity Distribution")
plt.xlabel("Popularity")
plt.ylabel("Count")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Genre Dağılımı - Top 15

# COMMAND ----------

genre_counts = spotify["track_genre"].value_counts().head(15)
display(genre_counts.reset_index().rename(columns={"index": "track_genre", "track_genre": "count"}))

plt.figure(figsize=(12, 6))
genre_counts.sort_values().plot(kind="barh")
plt.title("Top 15 Track Genres")
plt.xlabel("Count")
plt.ylabel("Genre")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Explicit Dağılımı

# COMMAND ----------

explicit_counts = spotify["explicit"].value_counts()
display(explicit_counts.reset_index())

plt.figure(figsize=(6, 6))
plt.pie(explicit_counts.values, labels=explicit_counts.index.astype(str), autopct="%1.1f%%")
plt.title("Explicit Track Distribution")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Tempo Bucket Analizi
# MAGIC
# MAGIC Spotify dataset içinde gerçek tarih alanı olmadığı için dashboard'da müzik verisine dayalı zaman serisi yerine tempo bucket analizi kullanılmıştır.

# COMMAND ----------

tempo_bins = [0, 80, 100, 120, 140, 1000]
tempo_labels = ["Very Slow", "Slow", "Medium", "Fast", "Very Fast"]
spotify["tempo_bucket"] = pd.cut(spotify["tempo"], bins=tempo_bins, labels=tempo_labels)

tempo_summary = spotify.groupby("tempo_bucket", observed=False).agg(
    track_count=("track_id", "count"),
    avg_popularity=("popularity", "mean")
).reset_index()

display(tempo_summary)

plt.figure(figsize=(9, 5))
plt.bar(tempo_summary["tempo_bucket"].astype(str), tempo_summary["avg_popularity"])
plt.title("Tempo Bucket Average Popularity")
plt.xlabel("Tempo Bucket")
plt.ylabel("Average Popularity")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5 Energy vs Danceability Scatter

# COMMAND ----------

sample_df = spotify.sample(min(5000, len(spotify)), random_state=42)

plt.figure(figsize=(8, 6))
plt.scatter(sample_df["energy"], sample_df["danceability"], alpha=0.25)
plt.title("Energy vs Danceability")
plt.xlabel("Energy")
plt.ylabel("Danceability")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Dashboard'a Ekleme
# MAGIC
# MAGIC Databricks üzerinde:
# MAGIC
# MAGIC 1. Bu notebook'u import edin.
# MAGIC 2. Notebook'u çalıştırın.
# MAGIC 3. Grafik çıkan hücrelerde sağ üstteki hücre menüsünden Add to dashboard seçeneğini kullanın.
# MAGIC 4. Şu hücreleri dashboard'a ekleyin:
# MAGIC    - Model performans karşılaştırması
# MAGIC    - Feature importance
# MAGIC    - Popularity dağılımı
# MAGIC    - Genre dağılımı
# MAGIC    - Explicit dağılımı
# MAGIC    - Tempo bucket analizi
# MAGIC    - Energy vs Danceability
# MAGIC 5. Dashboard başlıklarını ve açıklamalarını düzenleyin.
# MAGIC 6. Sunumda Databricks dashboard ekranını gösterin.
