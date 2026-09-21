import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# =========================================================
# 1. Kleine .env-Datei ohne zusätzliche Python-Pakete laden
# =========================================================


# =========================================================
# 2. Argument von Airflow / Dataproc übernehmen
# =========================================================

if len(sys.argv) < 2:
    raise ValueError(
        "Kein Bronze-Pfad übergeben. "
        "Erwartet: spark_job.py <abfss_raw_path>"
    )

raw_path = sys.argv[1]

print(f"Bronze input: {raw_path}")


# =========================================================
# 3. Secrets / Konfiguration lesen
# =========================================================


API_KEY = os.getenv("YOUTUBE_API_KEY")

tenant_id = os.getenv("AZURE_TENANT_ID")
client_id = os.getenv("AZURE_CLIENT_ID")
client_secret = os.getenv("AZURE_CLIENT_SECRET")

storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
container_name = os.getenv("AZURE_CONTAINER")

tenant_id = AZURE_TENANT_ID
client_id = AZURE_CLIENT_ID
client_secret = AZURE_CLIENT_SECRET
storage_account = AZURE_STORAGE_ACCOUNT
container = AZURE_CONTAINER
mysql_password = MYSQL_PASSWORD


required_values = {
    "AZURE_TENANT_ID": tenant_id,
    "AZURE_CLIENT_ID": client_id,
    "AZURE_CLIENT_SECRET": client_secret,
    "AZURE_STORAGE_ACCOUNT": storage_account,
    "AZURE_CONTAINER": container,
    "MYSQL_PASSWORD": mysql_password,
}

missing = [
    key
    for key, value in required_values.items()
    if not value
]

if missing:
    raise ValueError(
        f"Fehlende Konfiguration: {missing}"
    )


# =========================================================
# 4. Spark Session
# =========================================================

spark = (
    SparkSession.builder
    .appName("youtube_transform")
    .getOrCreate()
)


# =========================================================
# 5. Azure ADLS OAuth konfigurieren
# =========================================================

account_host = (
    f"{storage_account}.dfs.core.windows.net"
)

spark.conf.set(
    f"fs.azure.account.auth.type.{account_host}",
    "OAuth"
)

spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{account_host}",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.id.{account_host}",
    client_id
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{account_host}",
    client_secret
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{account_host}",
    f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
)

print("Azure OAuth-Konfiguration gesetzt.")


# =========================================================
# 6. Bronze JSON lesen
# =========================================================

raw_df = (
    spark.read
    .option("multiline", "true")
    .json(raw_path)
)

print("Bronze erfolgreich gelesen.")


# =========================================================
# 7. YouTube items explodieren
# =========================================================

videos = raw_df.select(
    F.explode("items").alias("video")
)


# =========================================================
# 8. Neue Silver-Daten aus diesem Lauf erstellen
# =========================================================

new_silver_df = videos.select(

    F.col("video.id").alias("video_id"),

    F.col("video.snippet.title").alias("title"),

    F.col("video.snippet.channelId").alias("channel_id"),

    F.col("video.snippet.channelTitle").alias("channel_title"),

    F.to_timestamp(
        F.col("video.snippet.publishedAt")
    ).alias("published_at"),

    F.col("video.statistics.viewCount")
    .cast("long")
    .alias("view_count"),

    F.col("video.statistics.likeCount")
    .cast("long")
    .alias("like_count"),

    F.col("video.statistics.commentCount")
    .cast("long")
    .alias("comment_count")
)


# =========================================================
# 9. Data Quality
# =========================================================

invalid_video_ids = (
    new_silver_df
    .filter(F.col("video_id").isNull())
    .count()
)

if invalid_video_ids > 0:
    raise ValueError(
        f"DQ fehlgeschlagen: "
        f"{invalid_video_ids} video_id NULL"
    )

print("Data Quality Check erfolgreich.")


# =========================================================
# 10. Mit bestehende Silver zusammenführen
# =========================================================

silver_path = (
    f"abfss://{container}@{account_host}/"
    "silver/youtube/videos"
)

try:

    existing_silver = spark.read.parquet(silver_path)

    silver_df = (
        existing_silver
        .unionByName(new_silver_df)
        .dropDuplicates(["video_id"])
    )

except Exception:

    # Erster Lauf: Silver existiert noch nicht
    silver_df = new_silver_df


# Materialisieren, bevor derselbe Pfad überschrieben wird
silver_df = silver_df.cache()
silver_df.count()


(
    silver_df.write
    .mode("overwrite")
    .parquet(silver_path)
)

print(f"Silver gespeichert: {silver_path}")

# =========================================================
# 11. Gold erstellen
# =========================================================

gold_channel = (
    silver_df
    .groupBy(
        "channel_id",
        "channel_title"
    )
    .agg(

        F.countDistinct("video_id")
        .alias("video_count"),

        F.sum("view_count")
        .alias("total_views"),

        F.sum("like_count")
        .alias("total_likes"),

        F.sum("comment_count")
        .alias("total_comments")
    )
)


# =========================================================
# 12. Gold nach ADLS schreiben
# =========================================================

gold_path = (
    f"abfss://{container}@{account_host}/"
    "gold/youtube/channel_summary"
)

(
    gold_channel.write
    .mode("overwrite")
    .parquet(gold_path)
)

print(f"Gold gespeichert: {gold_path}")


# =========================================================
# 13. Gold nach Azure MySQL schreiben
# =========================================================

mysql_url = (
    "jdbc:mysql://adminadmin.mysql.database.azure.com:3306/"
    "german_election_analytics"
    "?sslMode=REQUIRED"
)

mysql_properties = {
    "user": "electionadmin",
    "password": mysql_password,
    "driver": "com.mysql.cj.jdbc.Driver"
}

(
    gold_channel.write
    .mode("overwrite")
    .jdbc(
        url=mysql_url,
        table="youtube_channel_summary",
        properties=mysql_properties
    )
)

print("Gold erfolgreich nach MySQL geschrieben.")


# =========================================================
# 14. Ende
# =========================================================

print("YouTube Spark Pipeline erfolgreich abgeschlossen.")

spark.stop()