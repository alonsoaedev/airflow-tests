from datetime import datetime, UTC
from logging import Logger, getLogger

from airflow.sdk import dag, task

def notify_error_with_email(
    environment: str,
    emails: list[str] | str,
    logger: Logger | None = None
):
    from functools import wraps

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            from airflow.utils.email import send_email
            from airflow.exceptions import AirflowFailException

            try:
                return func(*args, **kwargs)
            except Exception as error:
                if logger:
                    logger.info(f"The error was sent by email")

                dag_id = kwargs["ti"].dag_id
                task_id: str = kwargs["ti"].task_id
                send_email(
                    to=emails,
                    subject=f"[{environment}] Airflow alert: error on {dag_id}.{task_id}",
                    html_content=f"""
                        <p>Airflow alert: error on {dag_id}.{task_id}.</p>
                        <p>Error: {error}.</p>
                    """
                )
                raise AirflowFailException(error)
        return wrapper
    return decorator

@dag(
    start_date=datetime.now(UTC),
    catchup=False,
    tags=["test", "decorator"],
)
def test_decorators():
    logger: Logger = getLogger("airflow.task")

    @task
    @notify_error_with_email(
        environment="dev".upper(),
        emails=["alonso.ae@hotmail.com", "alonso.ae@icloud.com"],
        logger=logger
    )
    def throw_error(**context) -> None:
        return 1/0

    throw_error()
test_decorators()
