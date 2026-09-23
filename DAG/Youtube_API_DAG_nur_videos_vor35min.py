import os
import json
import requests
from datetime import datetime, timezone, timedelta
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# =========================================================
# 1. KONFIGURATION
# =========================================================

API_KEY = os.getenv("YOUTUBE_API_KEY")
tenant_id = os.getenv("AZURE_TENANT_ID")
client_id = os.getenv("AZURE_CLIENT_ID")
client_secret = os.getenv("AZURE_CLIENT_SECRET")
storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
container_name = os.getenv("AZURE_CONTAINER")
# =========================================================
# 2. ADLS VERBINDUNG
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
# 3. PRÜFEN OB ES SCHON YOUTUBE-DATEIEN IN BRONZE GIBT ?
# =========================================================
def bronze_has_youtube_files():
    try:
        paths = file_system_client.get_paths(
            path="bronze/youtube"
        )
        for path in paths:
            if (
                not path.is_directory
                and path.name.endswith(".json")
            ):
                return True
        return False
    except Exception:
        # Wenn der Ordner noch nicht existiert:
        # behandeln wir es als ersten Lauf.
        return False
# =========================================================
# 4. YOUTUBE DATEN HOLEN
# =========================================================
def fetch_youtube_data():
    # -----------------------------------------------------
    # 4.1 Prüfen: erster Lauf oder Incremental?
    # -----------------------------------------------------
    first_run = not bronze_has_youtube_files()
    print(
        f"Erster Lauf: {first_run}"
    )
    # -----------------------------------------------------
    # 4.2 Search API
    # -----------------------------------------------------
    search_url = (
        "https://www.googleapis.com/youtube/v3/search"
    )
    search_params = {
        "part": "snippet",
        "q": "Bundestagswahl",
        "type": "video",
        "order": "date",
        "maxResults": 50,
        "key": API_KEY
    }
    # =====================================================
    # ERSTER LAUF
    #
    # Keine Zeitbegrenzung.
    # Einfach die neuesten 50 Videos holen.
    # =====================================================
    if first_run:
        print(
            "Bronze ist leer."
        )
        print(
            "Hole die neuesten 50 "
            "YouTube-Videos."
        )
    # =====================================================
    # SPÄTERE LÄUFE
    # Nur neue Videos der letzten 35 Minuten.
    # =====================================================
    else:
        now = datetime.now(
            timezone.utc
        )
        published_after = (
            now - timedelta(minutes=35)
        )
        published_after_str = (
            published_after
            .isoformat()
            .replace("+00:00", "Z")
        )
        search_params[
            "publishedAfter"
        ] = published_after_str

        print(
            "Bronze enthält bereits Daten."
        )
        print(
            f"Suche Videos nach: "
            f"{published_after_str}"
        )
    # -----------------------------------------------------
    # 4.3 YouTube Search Request
    # -----------------------------------------------------
    response = requests.get(
        search_url,
        params=search_params,
        timeout=30
    )
    response.raise_for_status()
    search_data = response.json()
    # -----------------------------------------------------
    # 4.4 Video IDs
    # -----------------------------------------------------
    video_ids = [
        item["id"]["videoId"]

        for item in search_data.get(
            "items",
            []
        )

        if item.get(
            "id",
            {}
        ).get("videoId")
    ]
    # doppelte IDs innerhalb dieses Requests entfernen
    video_ids = list(
        dict.fromkeys(video_ids)
    )
    print(
        f"Gefundene Videos: "
        f"{len(video_ids)}"
    )
    # -----------------------------------------------------
    # 4.5 Keine Videos gefunden
    # -----------------------------------------------------
    if not video_ids:

        raise AirflowSkipException(
            "Keine neuen YouTube-Videos gefunden."
        )
    # =====================================================
    # 5. VIDEO-DETAILS + STATISTIK
    # =====================================================
    videos_url = (
        "https://www.googleapis.com/youtube/v3/videos"
    )
    video_params = {

        "part": "snippet,statistics",

        "id": ",".join(
            video_ids
        ),

        "key": API_KEY
    }
    response = requests.get(
        videos_url,
        params=video_params,
        timeout=30
    )
    response.raise_for_status()
    video_data = response.json()
    item_count = len(
        video_data.get(
            "items",
            []
        )
    )
    print(
        f"Videos mit Statistik: "
        f"{item_count}"
    )
    if item_count == 0:
        raise AirflowSkipException(
            "Keine Video-Details erhalten."
    # =====================================================
    # 6. BRONZE JSON ERSTELLEN
    # =====================================================
    timestamp = (
        datetime.now(
            timezone.utc
        )
        .strftime(
            "%Y%m%d_%H%M%S"
        )
    )
    file_path = (
        f"bronze/youtube/"
        f"youtube_raw_{timestamp}.json"
    )
    json_data = json.dumps(
        video_data,
        ensure_ascii=False,
        indent=2
    )
    # =====================================================
    # 7. NACH ADLS BRONZE SCHREIBEN
    # =====================================================
    file_client = (
        file_system_client
        .get_file_client(
            file_path
        )
    )
    file_client.upload_data(
        json_data,
        overwrite=True
    )
    print(
        f"Bronze gespeichert: "
        f"{file_path}"
    )
    # =====================================================
    # 8. ABFSS PFAD FÜR SPARK
    # =====================================================
    abfss_path = (
        f"abfss://{container_name}@"
        f"{storage_account}.dfs.core.windows.net/"
        f"{file_path}"
    )
    print(
        f"Spark Input: "
        f"{abfss_path}"
    )
    return abfss_path
# =========================================================
# 9. DAG
# =========================================================
with DAG(
    dag_id="youtube_pipeline",
    schedule="*/30 * * * *",
    catchup=False,
    start_date=datetime(
        2026,
        9,
        23,
        tzinfo=timezone.utc
    ),
    max_active_runs=1,
    tags=[
        "youtube",
        "dataproc",
        "adls"
    ]
) as dag:
    # =====================================================
    # 9.1 YOUTUBE → BRONZE
    # =====================================================
    fetch_youtube = PythonOperator(
        task_id="fetch_youtube_data",
        python_callable=fetch_youtube_data
    )
    # =====================================================
    # 9.2 DATAPROC SPARK JOB
    # =====================================================
    PYSPARK_JOB = {
        "placement": {
            "cluster_name":
                "cluster-1ddb"
        },
        "pyspark_job": {
            "main_python_file_uri":
                "gs://german-election-spark-jobs-majd/"
                "spark_jobs/spark_Job.py",
            "args": [

                "{{ ti.xcom_pull("
                "task_ids='fetch_youtube_data'"
                ") }}"
            ]
        }
    }
    run_spark = DataprocSubmitJobOperator(
        task_id="run_spark_job",
        job=PYSPARK_JOB,
        region="europe-west1",
        project_id=(
            "liquid-crossing-501513-u5"
        )
    )
    fetch_youtube >> run_spark