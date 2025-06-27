from airflow.decorators import dag, task
from datetime import datetime
import subprocess

@dag(
    dag_id="test_cleaning_db",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "cleaning_db"],
)
def clean_airflow_db_dag():

    @task
    def run_db_clean():
        command = [
            "airflow", "db", "clean",
            "--clean-before-timestamp", "2024-12-07 20:03:22+00:00",
            "-y",
            "--skip-archive"
        ]
        env = {"PYTHONWARNINGS": "ignore::sqlalchemy.exc.SAWarning"}

        try:
            result = subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True,
                env={**env, **dict(subprocess.os.environ)}  # preserve current env
            )
            print("STDOUT:\n", result.stdout)
            print("STDERR:\n", result.stderr)
        except subprocess.CalledProcessError as e:
            print("ERROR:\n", e.stderr)
            raise RuntimeError(f"airflow db clean failed with return code {e.returncode}")

    run_db_clean()

clean_airflow_db_dag()
