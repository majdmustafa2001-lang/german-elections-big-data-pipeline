import os
import json
import requests
import pandas as pd

from datetime import datetime, timezone, timedelta

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator
)


# =========================================================
# 1. Konfiguration
# =========================================================

API_KEY = os.getenv("YOUTUBE_API_KEY")

tenant_id = os.getenv("AZURE_TENANT_ID")
client_id = os.getenv("AZURE_CLIENT_ID")
client_secret = os.getenv("AZURE_CLIENT_SECRET")

storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
container_name = os.getenv("AZURE_CONTAINER")


# =========================================================
# 2. Azure ADLS Verbindung
# =========================================================

credential = ClientSecretCredential(
    tenant_id=tenant_id,
    client_id=client_id,
    client_secret=client_secret
)

service_client = DataLakeServiceClient(
    account_url=f"https://{storage_account}.dfs.core.windows.net",
    credential=credential
)

file_system_client = service_client.get_file_system_client(
    file_system=container_name
)


# =========================================================
# 3. YouTube Daten holen
# =========================================================

def fetch_youtube_data():

    # -----------------------------------------------------
    # 3.1 Zeitfenster festlegen
    # -----------------------------------------------------

    now = datetime.now(timezone.utc)

    # 35 Minuten statt 30:
    # 5 Minuten Überlappung als Sicherheit
    published_after = now - timedelta(minutes=35)

    published_after_str = (
        published_after
        .isoformat()
        .replace("+00:00", "Z")
    )

    print(
        f"Suche YouTube Videos nach: "
        f"{published_after_str}"
    )


    # -----------------------------------------------------
    # 3.2 YouTube Suche
    # -----------------------------------------------------

    search_url = (
        "https://www.googleapis.com/youtube/v3/search"
    )

    search_params = {
        "part": "snippet",
        "q": "Bundestagswahl",
        "type": "video",
        "order": "date",
        "publishedAfter": published_after_str,
        "maxResults": 50,
        "key": API_KEY
    }


    response = requests.get(
        search_url,
        params=search_params,
        timeout=30
    )

    response.raise_for_status()

    search_data = response.json()


    # -----------------------------------------------------
    # 3.3 Video IDs extrahieren
    # -----------------------------------------------------

    video_ids = [
        item["id"]["videoId"]
        for item in search_data.get("items", [])
        if item.get("id", {}).get("videoId")
    ]


    # innerhalb dieses Runs deduplizieren
    video_ids = list(
        dict.fromkeys(video_ids)
    )


    print(
        f"Gefundene neue Video-IDs: "
        f"{len(video_ids)}"
    )


    # -----------------------------------------------------
    # 3.4 Wenn keine neuen Videos vorhanden sind
    # -----------------------------------------------------

    if not video_ids:

        raise AirflowSkipException(
            "Keine neuen YouTube Videos "
            "im letzten Zeitfenster gefunden."
        )


    # -----------------------------------------------------
    # 3.5 Details + Statistiken holen
    # -----------------------------------------------------

    videos_url = (
        "https://www.googleapis.com/youtube/v3/videos"
    )

    video_params = {
        "part": "snippet,statistics",
        "id": ",".join(video_ids),
        "key": API_KEY
    }


    response = requests.get(
        videos_url,
        params=video_params,
        timeout=30
    )

    response.raise_for_status()


    video_data = response.json()


    print(
        f"Videos mit Details geladen: "
        f"{len(video_data.get('items', []))}"
    )


    # -----------------------------------------------------
    # 3.6 Bronze Dateiname erzeugen
    # -----------------------------------------------------

    timestamp = (
        datetime.now(timezone.utc)
        .strftime("%Y%m%d_%H%M%S")
    )


    file_path = (
        f"bronze/youtube/"
        f"youtube_raw_{timestamp}.json"
    )


    # -----------------------------------------------------
    # 3.7 JSON erzeugen
    # -----------------------------------------------------

    json_data = json.dumps(
        video_data,
        ensure_ascii=False,
        indent=2
    )


    # -----------------------------------------------------
    # 3.8 Bronze nach ADLS schreiben
    # -----------------------------------------------------

    file_client = (
        file_system_client
        .get_file_client(file_path)
    )


    file_client.upload_data(
        json_data,
        overwrite=True
    )


    print(
        f"Raw YouTube JSON gespeichert: "
        f"{file_path}"
    )


    # -----------------------------------------------------
    # 3.9 Optional: DataFrame nur fürs Logging
    # -----------------------------------------------------

    rows = []


    for item in video_data.get(
        "items",
        []
    ):

        snippet = item.get(
            "snippet",
            {}
        )

        statistics = item.get(
            "statistics",
            {}
        )


        rows.append({

            "video_id":
                item.get("id"),

            "title":
                snippet.get("title"),

            "channel_id":
                snippet.get("channelId"),

            "channel_title":
                snippet.get("channelTitle"),

            "published_at":
                snippet.get("publishedAt"),

            "view_count":
                int(
                    statistics.get(
                        "viewCount",
                        0
                    )
                ),

            "like_count":
                int(
                    statistics.get(
                        "likeCount",
                        0
                    )
                ),

            "comment_count":
                int(
                    statistics.get(
                        "commentCount",
                        0
                    )
                )
        })


    df = pd.DataFrame(rows)


    print(
        f"Neue Videos in diesem Run: "
        f"{len(df)}"
    )


    print(
        df.head(10)
    )


    # -----------------------------------------------------
    # 3.10 Vollständigen ABFSS-Pfad bauen
    # -----------------------------------------------------

    abfss_path = (
        f"abfss://{container_name}@"
        f"{storage_account}.dfs.core.windows.net/"
        f"{file_path}"
    )


    print(
        f"Bronze ABFSS Path: "
        f"{abfss_path}"
    )


    # dieser Wert geht über XCom an Dataproc
    return abfss_path


# =========================================================
# 4. Airflow DAG
# =========================================================

with DAG(

    dag_id="youtube_pipeline",

    schedule="*/30 * * * *",

    catchup=False,

    start_date=datetime(
        2026,
        9,
        21
    ),

    max_active_runs=1,

    tags=[
        "youtube",
        "dataproc",
        "adls"
    ]

) as dag:


    # =====================================================
    # 4.1 YouTube → Bronze
    # =====================================================

    fetch_youtube = PythonOperator(

        task_id="fetch_youtube_data",

        python_callable=fetch_youtube_data
    )


    # =====================================================
    # 4.2 Dataproc Spark Job konfigurieren
    # =====================================================

    PYSPARK_JOB = {

        "placement": {

            "cluster_name":
                "cluster-1ddb"
        },

        "pyspark_job": {

            "main_python_file_uri":
                "file:///home/majdmustafa2001/"
                "spark_jobs/spark_job.py",

            "args": [

                "{{ ti.xcom_pull("
                "task_ids='fetch_youtube_data'"
                ") }}"
            ]
        }
    }


    # =====================================================
    # 4.3 Spark Job starten
    # =====================================================

    run_spark = DataprocSubmitJobOperator(

        task_id="run_spark_job",

        job=PYSPARK_JOB,

        region="europe-west1",

        project_id=
            "liquid-crossing-501513-u5"
    )


    # =====================================================
    # 4.4 Reihenfolge
    # =====================================================

    fetch_youtube >> run_spark