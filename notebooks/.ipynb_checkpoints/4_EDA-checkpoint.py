# -*- coding: utf-8 -*-
# ---
# Adım 4: Keşifsel Veri Analizi (EDA) - Spotify Tracks Dataset
# Bu notebook, Delta Lake Gold katmanındaki Spotify verisi üzerinde
# kapsamlı bir keşifsel veri analizi gerçekleştirir.
# ---

# ============================================================
# CELL 1: Kütüphanelerin Yüklenmesi ve Spark Oturumu
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, when, isnan, isnull,
    mean, stddev, min as spark_min, max as spark_max,
    hour, dayofweek, date_format, round as spark_round,
    desc, asc, lit, sum as spark_sum, avg,
    percentile_approx, skewness, kurtosis
)
from pyspark.sql.types import DoubleType
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd
import numpy as np
import os

# Grafik kayıt dizini
PLOT_DIR = "/home/jovyan/work/notebooks/eda_plots"
os.makedirs(PLOT_DIR, exist_ok=True)

# Matplotlib genel stil ayarları
plt.rcParams.update({
    "figure.facecolor": "#1e1e2f",
    "axes.facecolor": "#1e1e2f",
    "axes.edgecolor": "#444466",
    "axes.labelcolor": "#d0d0e0",
    "text.color": "#d0d0e0",
    "xtick.color": "#a0a0c0",
    "ytick.color": "#a0a0c0",
    "grid.color": "#333355",
    "figure.dpi": 120,
    "font.size": 11,
    "axes.titlesize": 14,
    "axes.labelsize": 12,
})

ACCENT = "#7c4dff"
PALETTE = ["#7c4dff", "#00e5ff", "#ff4081", "#ffab40", "#69f0ae", "#ea80fc"]

spark = (
    SparkSession.builder
    .appName("Spotify EDA - Adim 4")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("Spark oturumu başlatıldı.")


# ============================================================
# CELL 2: Delta Lake Gold Tablosunun Okunması
# ============================================================

GOLD_PATH = "/home/jovyan/work/delta/gold/spotify_tracks_features"
SILVER_PATH = "/home/jovyan/work/delta/silver/spotify_tracks"

# Gold varsa onu oku, yoksa Silver'ı oku
try:
    df = spark.read.format("delta").load(GOLD_PATH)
    print(f"Gold tablosu okundu: {GOLD_PATH}")
except Exception:
    df = spark.read.format("delta").load(SILVER_PATH)
    print(f"Gold bulunamadı, Silver tablosu okundu: {SILVER_PATH}")

print(f"Toplam satır sayısı : {df.count()}")
print(f"Toplam kolon sayısı : {len(df.columns)}")
print(f"\nKolonlar: {df.columns}")
df.printSchema()


# ============================================================
# CELL 3: İlk Birkaç Satırın İncelenmesi
# ============================================================

df.show(10, truncate=False)


# ============================================================
# CELL 4: Temel İstatistikler (describe)
# ============================================================

numeric_cols = [
    "popularity", "duration_ms", "danceability", "energy",
    "loudness", "speechiness", "acousticness", "instrumentalness",
    "liveness", "valence", "tempo"
]
# Gold katmanında üretilen ek özellikler
gold_cols = ["duration_min", "mood_score", "audio_intensity"]
# Mevcut kolonları filtrele
existing_numeric = [c for c in numeric_cols + gold_cols if c in df.columns]

desc_df = df.select(existing_numeric).describe()
desc_df.show()

# Ek istatistikler: medyan, çarpıklık, basıklık
agg_exprs = []
for c in existing_numeric:
    agg_exprs.extend([
        percentile_approx(col(c), 0.5).alias(f"{c}_median"),
        skewness(col(c)).alias(f"{c}_skewness"),
        kurtosis(col(c)).alias(f"{c}_kurtosis"),
    ])
extra_stats = df.agg(*agg_exprs)
print("Medyan / Çarpıklık / Basıklık:")
extra_stats.show(truncate=False)


# ============================================================
# CELL 5: Benzersiz Değer Sayıları
# ============================================================

categorical_cols = ["track_genre", "artists", "album_name", "track_name"]
existing_cat = [c for c in categorical_cols if c in df.columns]

for c in existing_cat:
    n_unique = df.select(countDistinct(col(c))).collect()[0][0]
    print(f"{c:20s} → {n_unique:,} benzersiz değer")

if "user_id" in df.columns:
    n_users = df.select(countDistinct("user_id")).collect()[0][0]
    print(f"{'user_id':20s} → {n_users:,} benzersiz değer")

if "track_id" in df.columns:
    n_tracks = df.select(countDistinct("track_id")).collect()[0][0]
    print(f"{'track_id':20s} → {n_tracks:,} benzersiz değer")


# ============================================================
# CELL 6: Eksik / Null Değer Analizi
# ============================================================

total_rows = df.count()
null_counts = []

for c in df.columns:
    if df.schema[c].dataType in (DoubleType(),):
        n_null = df.filter(col(c).isNull() | isnan(col(c))).count()
    else:
        n_null = df.filter(col(c).isNull()).count()
    pct = round(100.0 * n_null / total_rows, 2)
    null_counts.append((c, n_null, pct))

null_pdf = pd.DataFrame(null_counts, columns=["column", "null_count", "null_pct"])
null_pdf = null_pdf.sort_values("null_count", ascending=False)
print(null_pdf.to_string(index=False))

# Eksik değer grafiği
cols_with_nulls = null_pdf[null_pdf["null_count"] > 0]
if len(cols_with_nulls) > 0:
    fig, ax = plt.subplots(figsize=(10, max(4, len(cols_with_nulls) * 0.5)))
    bars = ax.barh(cols_with_nulls["column"], cols_with_nulls["null_pct"], color=ACCENT, edgecolor="#5533cc")
    ax.set_xlabel("Eksik Değer Oranı (%)")
    ax.set_title("Kolonlara Göre Eksik Değer Oranları")
    ax.invert_yaxis()
    for bar, val in zip(bars, cols_with_nulls["null_pct"]):
        ax.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height()/2,
                f"{val:.1f}%", va="center", fontsize=9, color="#d0d0e0")
    plt.tight_layout()
    plt.savefig(f"{PLOT_DIR}/01_missing_values.png", bbox_inches="tight")
    plt.show()
    print("Eksik değer grafiği kaydedildi.")
else:
    print("Hiçbir kolonda eksik değer bulunmamaktadır ✓")


# ============================================================
# CELL 7: Hedef Değişken Analizi - Popularity Dağılımı
# ============================================================

pop_pdf = df.select("popularity").toPandas()

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# Histogram
axes[0].hist(pop_pdf["popularity"], bins=50, color=ACCENT, edgecolor="#5533cc", alpha=0.85)
axes[0].set_title("Popularity Dağılımı (Histogram)")
axes[0].set_xlabel("Popularity")
axes[0].set_ylabel("Frekans")
axes[0].axvline(pop_pdf["popularity"].mean(), color="#ff4081", linestyle="--", label=f'Ortalama: {pop_pdf["popularity"].mean():.1f}')
axes[0].axvline(pop_pdf["popularity"].median(), color="#00e5ff", linestyle="--", label=f'Medyan: {pop_pdf["popularity"].median():.1f}')
axes[0].legend()

# Box plot
bp = axes[1].boxplot(pop_pdf["popularity"].dropna(), vert=True, patch_artist=True,
                     boxprops=dict(facecolor=ACCENT, color="#5533cc"),
                     medianprops=dict(color="#00e5ff", linewidth=2),
                     whiskerprops=dict(color="#a0a0c0"),
                     capprops=dict(color="#a0a0c0"),
                     flierprops=dict(marker="o", markerfacecolor="#ff4081", markersize=3, alpha=0.4))
axes[1].set_title("Popularity Dağılımı (Box Plot)")
axes[1].set_ylabel("Popularity")

plt.suptitle("Hedef Değişken: popularity", fontsize=15, color="#ea80fc", y=1.02)
plt.tight_layout()
plt.savefig(f"{PLOT_DIR}/02_popularity_distribution.png", bbox_inches="tight")
plt.show()
print("Popularity dağılım grafikleri kaydedildi.")


# ============================================================
# CELL 8: Sayısal Değişkenlerin Dağılım Analizi (Histogramlar)
# ============================================================

plot_cols = [c for c in existing_numeric if c != "popularity"]
n = len(plot_cols)
ncols = 3
nrows = (n + ncols - 1) // ncols

fig, axes = plt.subplots(nrows, ncols, figsize=(16, 4 * nrows))
axes = axes.flatten()

for i, c in enumerate(plot_cols):
    data = df.select(c).dropna().toPandas()
    axes[i].hist(data[c], bins=50, color=PALETTE[i % len(PALETTE)], edgecolor="#333355", alpha=0.85)
    axes[i].set_title(c)
    axes[i].set_xlabel("")
    axes[i].set_ylabel("Frekans")

for j in range(i + 1, len(axes)):
    axes[j].set_visible(False)

plt.suptitle("Sayısal Değişkenlerin Dağılımları", fontsize=15, color="#ea80fc", y=1.01)
plt.tight_layout()
plt.savefig(f"{PLOT_DIR}/03_numeric_distributions.png", bbox_inches="tight")
plt.show()
print("Sayısal değişken dağılımları kaydedildi.")


# ============================================================
# CELL 9: Korelasyon Matrisi
# ============================================================

corr_cols = [c for c in existing_numeric if c in df.columns]
corr_pdf = df.select(corr_cols).toPandas()
corr_matrix = corr_pdf.corr()

fig, ax = plt.subplots(figsize=(12, 10))
im = ax.imshow(corr_matrix.values, cmap="cool", aspect="auto", vmin=-1, vmax=1)
ax.set_xticks(range(len(corr_cols)))
ax.set_yticks(range(len(corr_cols)))
ax.set_xticklabels(corr_cols, rotation=45, ha="right", fontsize=9)
ax.set_yticklabels(corr_cols, fontsize=9)

for i in range(len(corr_cols)):
    for j in range(len(corr_cols)):
        val = corr_matrix.values[i, j]
        color = "white" if abs(val) > 0.5 else "#d0d0e0"
        ax.text(j, i, f"{val:.2f}", ha="center", va="center", fontsize=8, color=color)

plt.colorbar(im, ax=ax, shrink=0.8, label="Korelasyon")
ax.set_title("Sayısal Değişkenler Arası Korelasyon Matrisi", fontsize=14, color="#ea80fc")
plt.tight_layout()
plt.savefig(f"{PLOT_DIR}/04_correlation_matrix.png", bbox_inches="tight")
plt.show()
print("Korelasyon matrisi kaydedildi.")


# ============================================================
# CELL 10: Popularity ile En Güçlü Korelasyonlar
# ============================================================

if "popularity" in corr_matrix.columns:
    pop_corr = corr_matrix["popularity"].drop("popularity").sort_values(key=abs, ascending=False)
    print("Popularity ile korelasyonlar (büyükten küçüğe):\n")
    for feat, val in pop_corr.items():
        bar = "█" * int(abs(val) * 30)
        sign = "+" if val > 0 else "-"
        print(f"  {feat:25s}  {sign}{abs(val):.4f}  {bar}")


# ============================================================
# CELL 11: Kategorik Değişken - track_genre Analizi
# ============================================================

if "track_genre" in df.columns:
    genre_counts = (
        df.groupBy("track_genre")
        .agg(
            count("*").alias("count"),
            spark_round(avg("popularity"), 2).alias("avg_popularity"),
            spark_round(avg("energy"), 2).alias("avg_energy"),
            spark_round(avg("danceability"), 2).alias("avg_danceability"),
        )
        .orderBy(desc("count"))
    )

    genre_pdf = genre_counts.toPandas()
    print(f"Toplam tür sayısı: {len(genre_pdf)}")
    print("\nEn yaygın 20 tür:")
    print(genre_pdf.head(20).to_string(index=False))

    # En yaygın 20 türün dağılımı
    top20 = genre_pdf.head(20)
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    # Bar chart - count
    bars = axes[0].barh(top20["track_genre"][::-1], top20["count"][::-1],
                        color=ACCENT, edgecolor="#5533cc")
    axes[0].set_title("En Yaygın 20 Müzik Türü (Kayıt Sayısı)")
    axes[0].set_xlabel("Kayıt Sayısı")

    # Bar chart - avg popularity
    top20_sorted = top20.sort_values("avg_popularity", ascending=True)
    colors = plt.cm.cool(np.linspace(0.2, 0.9, len(top20_sorted)))
    axes[1].barh(top20_sorted["track_genre"], top20_sorted["avg_popularity"], color=colors)
    axes[1].set_title("En Yaygın 20 Türün Ortalama Popularity Değeri")
    axes[1].set_xlabel("Ortalama Popularity")

    plt.tight_layout()
    plt.savefig(f"{PLOT_DIR}/05_genre_analysis.png", bbox_inches="tight")
    plt.show()
    print("Tür analizi grafikleri kaydedildi.")


# ============================================================
# CELL 12: Explicit İçerik Analizi
# ============================================================

if "explicit" in df.columns or "explicit_int" in df.columns:
    exp_col = "explicit_int" if "explicit_int" in df.columns else "explicit"
    exp_counts = df.groupBy(exp_col).count().toPandas()
    exp_pop = df.groupBy(exp_col).agg(
        spark_round(avg("popularity"), 2).alias("avg_popularity")
    ).toPandas()

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    # Pie chart
    labels = ["Non-Explicit", "Explicit"]
    sizes = exp_counts.sort_values(exp_col)["count"].values
    axes[0].pie(sizes, labels=labels, autopct="%1.1f%%",
                colors=[ACCENT, "#ff4081"], startangle=90,
                textprops={"color": "#d0d0e0"})
    axes[0].set_title("Explicit İçerik Oranı")

    # Popularity karşılaştırma
    exp_pop_sorted = exp_pop.sort_values(exp_col)
    axes[1].bar(labels, exp_pop_sorted["avg_popularity"],
                color=[ACCENT, "#ff4081"], edgecolor="#333355")
    axes[1].set_title("Explicit vs Non-Explicit Ortalama Popularity")
    axes[1].set_ylabel("Ortalama Popularity")

    plt.tight_layout()
    plt.savefig(f"{PLOT_DIR}/06_explicit_analysis.png", bbox_inches="tight")
    plt.show()
    print("Explicit analizi kaydedildi.")


# ============================================================
# CELL 13: Zaman Serisi Analizi
# ============================================================

time_col = None
for tc in ["event_timestamp", "event_time", "gold_ingestion_time"]:
    if tc in df.columns:
        time_col = tc
        break

if time_col:
    print(f"Zaman kolonu: {time_col}")

    # Saatlik trend
    hourly = (
        df.withColumn("hour", hour(col(time_col)))
        .groupBy("hour")
        .agg(
            count("*").alias("event_count"),
            spark_round(avg("popularity"), 2).alias("avg_popularity"),
        )
        .orderBy("hour")
        .toPandas()
    )

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    axes[0].fill_between(hourly["hour"], hourly["event_count"], alpha=0.3, color=ACCENT)
    axes[0].plot(hourly["hour"], hourly["event_count"], color=ACCENT, linewidth=2, marker="o", markersize=4)
    axes[0].set_title("Saatlik Olay Sayısı Trendi")
    axes[0].set_xlabel("Saat")
    axes[0].set_ylabel("Olay Sayısı")
    axes[0].set_xticks(range(0, 24))

    axes[1].plot(hourly["hour"], hourly["avg_popularity"], color="#00e5ff", linewidth=2, marker="s", markersize=4)
    axes[1].set_title("Saatlik Ortalama Popularity Trendi")
    axes[1].set_xlabel("Saat")
    axes[1].set_ylabel("Ortalama Popularity")
    axes[1].set_xticks(range(0, 24))

    plt.tight_layout()
    plt.savefig(f"{PLOT_DIR}/07_time_series.png", bbox_inches="tight")
    plt.show()
    print("Zaman serisi grafikleri kaydedildi.")
else:
    print("Zaman kolonu bulunamadı, zaman serisi analizi atlandı.")


# ============================================================
# CELL 14: Scatter Plot - Energy vs Danceability (Popularity ile renkli)
# ============================================================

sample_pdf = df.select("energy", "danceability", "popularity").dropna().limit(5000).toPandas()

fig, ax = plt.subplots(figsize=(10, 7))
scatter = ax.scatter(
    sample_pdf["energy"], sample_pdf["danceability"],
    c=sample_pdf["popularity"], cmap="cool", alpha=0.5, s=12, edgecolors="none"
)
plt.colorbar(scatter, ax=ax, label="Popularity")
ax.set_xlabel("Energy")
ax.set_ylabel("Danceability")
ax.set_title("Energy vs Danceability (Popularity Renk Skalası)")
plt.tight_layout()
plt.savefig(f"{PLOT_DIR}/08_energy_vs_danceability.png", bbox_inches="tight")
plt.show()
print("Scatter plot kaydedildi.")


# ============================================================
# CELL 15: Gold Katmanı Ek Özelliklerin Analizi
# ============================================================

if "tempo_bucket" in df.columns:
    tb_pdf = df.groupBy("tempo_bucket").agg(
        count("*").alias("count"),
        spark_round(avg("popularity"), 2).alias("avg_popularity"),
    ).toPandas()

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    order = ["slow", "medium", "fast"]
    tb_pdf["tempo_bucket"] = pd.Categorical(tb_pdf["tempo_bucket"], categories=order, ordered=True)
    tb_pdf = tb_pdf.sort_values("tempo_bucket")

    axes[0].bar(tb_pdf["tempo_bucket"], tb_pdf["count"],
                color=PALETTE[:3], edgecolor="#333355")
    axes[0].set_title("Tempo Bucket Dağılımı")
    axes[0].set_ylabel("Kayıt Sayısı")

    axes[1].bar(tb_pdf["tempo_bucket"], tb_pdf["avg_popularity"],
                color=PALETTE[:3], edgecolor="#333355")
    axes[1].set_title("Tempo Bucket vs Ortalama Popularity")
    axes[1].set_ylabel("Ortalama Popularity")

    plt.tight_layout()
    plt.savefig(f"{PLOT_DIR}/09_tempo_bucket.png", bbox_inches="tight")
    plt.show()

if "mood_score" in df.columns:
    mood_pdf = df.select("mood_score", "popularity").dropna().limit(5000).toPandas()
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.scatter(mood_pdf["mood_score"], mood_pdf["popularity"],
               alpha=0.3, s=10, color="#69f0ae", edgecolors="none")
    ax.set_xlabel("Mood Score")
    ax.set_ylabel("Popularity")
    ax.set_title("Mood Score vs Popularity")
    plt.tight_layout()
    plt.savefig(f"{PLOT_DIR}/10_mood_vs_popularity.png", bbox_inches="tight")
    plt.show()

print("Gold katmanı analiz grafikleri kaydedildi.")


# ============================================================
# CELL 16: Popularity Segmentasyonu
# ============================================================

pop_segments = (
    df.withColumn(
        "pop_segment",
        when(col("popularity") <= 20, "Düşük (0-20)")
        .when(col("popularity") <= 50, "Orta (21-50)")
        .when(col("popularity") <= 75, "Yüksek (51-75)")
        .otherwise("Çok Yüksek (76-100)")
    )
    .groupBy("pop_segment")
    .agg(
        count("*").alias("count"),
        spark_round(avg("energy"), 3).alias("avg_energy"),
        spark_round(avg("danceability"), 3).alias("avg_danceability"),
        spark_round(avg("valence"), 3).alias("avg_valence"),
        spark_round(avg("acousticness"), 3).alias("avg_acousticness"),
    )
    .orderBy("pop_segment")
    .toPandas()
)

print("Popularity Segmentleri Özet:")
print(pop_segments.to_string(index=False))

fig, ax = plt.subplots(figsize=(10, 5))
seg_order = ["Düşük (0-20)", "Orta (21-50)", "Yüksek (51-75)", "Çok Yüksek (76-100)"]
pop_segments["pop_segment"] = pd.Categorical(pop_segments["pop_segment"], categories=seg_order, ordered=True)
pop_segments = pop_segments.sort_values("pop_segment")

ax.bar(pop_segments["pop_segment"], pop_segments["count"], color=PALETTE[:4], edgecolor="#333355")
ax.set_title("Popularity Segment Dağılımı")
ax.set_ylabel("Kayıt Sayısı")
ax.set_xlabel("Segment")
plt.tight_layout()
plt.savefig(f"{PLOT_DIR}/11_popularity_segments.png", bbox_inches="tight")
plt.show()
print("Popularity segmentasyonu kaydedildi.")


# ============================================================
# CELL 17: En Popüler Sanatçılar (Top 20)
# ============================================================

if "artists" in df.columns:
    top_artists = (
        df.groupBy("artists")
        .agg(
            count("*").alias("track_count"),
            spark_round(avg("popularity"), 2).alias("avg_popularity"),
        )
        .orderBy(desc("track_count"))
        .limit(20)
        .toPandas()
    )

    fig, ax = plt.subplots(figsize=(12, 6))
    colors = plt.cm.cool(np.linspace(0.2, 0.9, len(top_artists)))
    ax.barh(top_artists["artists"][::-1], top_artists["track_count"][::-1], color=colors)
    ax.set_title("En Çok Şarkıya Sahip 20 Sanatçı")
    ax.set_xlabel("Şarkı Sayısı")
    plt.tight_layout()
    plt.savefig(f"{PLOT_DIR}/12_top_artists.png", bbox_inches="tight")
    plt.show()
    print("Sanatçı analizi kaydedildi.")


# ============================================================
# CELL 18: EDA Özet ve Sonuç
# ============================================================

total = df.count()
n_cols = len(df.columns)
n_genres = df.select(countDistinct("track_genre")).collect()[0][0] if "track_genre" in df.columns else "N/A"
avg_pop = df.agg(spark_round(avg("popularity"), 2)).collect()[0][0]

print("=" * 60)
print("           EDA ÖZET RAPORU - Spotify Tracks Dataset")
print("=" * 60)
print(f"  Toplam kayıt sayısı       : {total:,}")
print(f"  Toplam kolon sayısı       : {n_cols}")
print(f"  Benzersiz tür sayısı      : {n_genres}")
print(f"  Ortalama popularity       : {avg_pop}")
print(f"  Grafik sayısı             : 12")
print(f"  Grafik dizini             : {PLOT_DIR}")
print("=" * 60)
print()
print("Sonraki adım → Adım 5: Özellik Mühendisliği (Feature Engineering)")
print("  Not: Gold katmanında bazı özellikler zaten üretilmiştir.")
print("  ML modeli için ek özellikler ve VectorAssembler hazırlanacak.")
print()
print("Sonraki adım → Adım 6: Makine Öğrenmesi (5 Regresyon Modeli)")
print("  Hedef değişken: popularity")
print("  Modeller: Linear Regression, Decision Tree, Random Forest,")
print("            GBT Regressor, Generalized Linear Regression")
print("=" * 60)

spark.stop()
print("\nSpark oturumu kapatıldı. EDA tamamlandı ✓")
