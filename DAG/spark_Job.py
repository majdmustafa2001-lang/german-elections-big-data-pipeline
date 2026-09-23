import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
# =========================================================
# 1. KONFIGURATION
# =========================================================
API_KEY = os.getenv("YOUTUBE_API_KEY")
tenant_id = os.getenv("AZURE_TENANT_ID")
client_id = os.getenv("AZURE_CLIENT_ID")
client_secret = os.getenv("AZURE_CLIENT_SECRET")
storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
container_name = os.getenv("AZURE_CONTAINER")
mysql_password= os.getenv("MY_SQL_PASSWORD")
# =========================================================
# 2. Bronze-Pfad von Airflow übernehmen
# =========================================================
if len(sys.argv) < 2:
    raise ValueError(
        "Kein Bronze-Pfad übergeben. "
    )
raw_path = sys.argv[1]
print(
    f"Bronze Input: {raw_path}"
)
# =========================================================
# 3. Spark Session
# =========================================================
spark = (
    SparkSession.builder
    .appName("youtube_transform")
    .getOrCreate()
)
# =========================================================
# 4. Azure ADLS OAuth konfigurieren
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
    f"https://login.microsoftonline.com/"
    f"{tenant_id}/oauth2/token"
)
print(
    "Azure OAuth-Konfiguration gesetzt."
)
# =========================================================
# 5. Bronze lesen
# =========================================================
raw_df = (
    spark.read
    .option("multiline", "true")
    .json(raw_path)
)

print(
    "Bronze erfolgreich gelesen."
)
# =========================================================
# 6. YouTube Items explodieren
# =========================================================
videos = (
    raw_df
    .select(
        F.explode("items")
        .alias("video")
    )
)
# =========================================================
# 7. Neuen Silver-Batch erstellen
# =========================================================
new_silver_df = (
    videos
    .select(

        F.col(
            "video.id"
        ).alias(
            "video_id"
        ),
        F.col(
            "video.snippet.title"
        ).alias(
            "title"
        ),
        F.col(
            "video.snippet.channelId"
        ).alias(
            "channel_id"
        ),
        F.col(
            "video.snippet.channelTitle"
        ).alias(
            "channel_title"
        ),
        F.to_timestamp(
            F.col(
                "video.snippet.publishedAt"
            )
        ).alias(
            "published_at"
        ),
        F.col(
            "video.statistics.viewCount"
        )
        .cast("long")
        .alias(
            "view_count"
        ),
        F.col(
            "video.statistics.likeCount"
        )
        .cast("long")
        .alias(
            "like_count"
        ),
        F.col(
            "video.statistics.commentCount"
        )
        .cast("long")
        .alias(
            "comment_count"
        )
    )
)
# =========================================================
# 8. Data Quality Checks
# =========================================================
new_rows = (
    new_silver_df
    .count()
)
print(
    f"Neue Silver-Zeilen: {new_rows}"
)
if new_rows == 0:
    raise ValueError(
        "Der Bronze-Datensatz enthält "
        "keine verwertbaren Videos."
    )
invalid_video_ids = (
    new_silver_df
    .filter(
        F.col("video_id").isNull()
    )
    .count()
)
if invalid_video_ids > 0:
    raise ValueError(
        f"DQ fehlgeschlagen: "
        f"{invalid_video_ids} video_id = NULL"
    )
negative_views = (
    new_silver_df
    .filter(
        F.col("view_count") < 0
    )
    .count()
)
if negative_views > 0:
    raise ValueError(
        f"DQ fehlgeschlagen: "
        f"{negative_views} negative view_count."
    )
print(
    "Data-Quality-Prüfungen erfolgreich."
)
# =========================================================
# 9. Silver Pfad
# =========================================================
silver_path = (
    f"abfss://{container}@{account_host}/"
    "silver/youtube/videos"
)
# =========================================================
# 10. NEUEN Batch an Silver anhängen
# =========================================================
(
    new_silver_df
    .write
    .mode("append")
    .parquet(silver_path)
)
print(
    "Neuer Batch an Silver angehängt."
)
# =========================================================
# 11. Gesamtes Silver lesen
# =========================================================
silver_df = (
    spark.read
    .parquet(silver_path)
)
# =========================================================
# 12. Für Analyse deduplizieren
# =========================================================
silver_deduplicated = (
    silver_df
    .dropDuplicates(
        ["video_id"]
    )
)
total_unique_videos = (
    silver_deduplicated
    .count()
)
print(
    f"Eindeutige Videos in Silver: "
    f"{total_unique_videos}"
)
# =========================================================
# 13. Gold erstellen
# =========================================================
gold_channel = (
    silver_deduplicated
    .groupBy(
        "channel_id",
        "channel_title"
    )
    .agg(
        F.countDistinct(
            "video_id"
        ).alias(
            "video_count"
        ),
        F.sum(
            F.coalesce(
                F.col("view_count"),
                F.lit(0)
            )
        ).alias(
            "total_views"
        ),
        F.sum(
            F.coalesce(
                F.col("like_count"),
                F.lit(0)
            )
        ).alias(
            "total_likes"
        ),
        F.sum(
            F.coalesce(
                F.col("comment_count"),
                F.lit(0)
            )
        ).alias(
            "total_comments"
        )
    )
)
# =========================================================
# 14. Gold nach ADLS schreiben
# =========================================================
gold_path = (
    f"abfss://{container}@{account_host}/"
    "gold/youtube/channel_summary"
)
(
    gold_channel
    .write
    .mode("overwrite")
    .parquet(gold_path)
)
print(
    f"Gold gespeichert: {gold_path}"
)
# =========================================================
# 15. MySQL Konfiguration
# =========================================================
jdbc_url = (
    "jdbc:mysql://adminadmin.mysql.database.azure.com:3306/"
    "german_election_analytics"
    "?useSSL=true"
    "&requireSSL=true"
    "&characterEncoding=utf8"
    "&connectionCollation=utf8mb4_unicode_ci"
)
mysql_properties = {
    "user":
        "electionadmin",
    "password":
        mysql_password,
    "driver":
        "com.mysql.cj.jdbc.Driver"
}
# =========================================================
# 16. Gold nach MySQL
# =========================================================
(
    gold_channel
    .write
    .mode("overwrite")
    .jdbc(
        url=jdbc_url,
        table="youtube_channel_summary",
        properties=mysql_properties
    )
)
print(
    "Gold erfolgreich nach MySQL geschrieben."
)
# =========================================================
# 17. Abschluss
# =========================================================
print(
    "YouTube Spark Pipeline "
    "erfolgreich abgeschlossen."
)
spark.stop()