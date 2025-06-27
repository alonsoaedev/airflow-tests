from datetime import datetime, UTC

from airflow.sdk import dag, task

@dag(
    start_date=datetime.now(UTC),
    catchup=False,
    tags=["test", "email_notifier"],
)
def test_email_notifier():
    from airflow.exceptions import AirflowFailException
    from airflow.operators.python import PythonOperator
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

    # THE NOTIFICATION HAPPENS IN THE "LISTENER PLUGIN" (/plugins/email_notifier_*)"

    @task
    def fail_task_decorator(**kwargs) -> None:
        return
        # raise AirflowFailException("Failed from task decorator")

    def fail_python_operator(**kwargs):
        return
        # raise AirflowFailException("Failed from python operator")
    
    fail_python_operator = PythonOperator(
        task_id="fail_python_operator",
        python_callable=fail_python_operator,
    )
    
    trigger = TriggerDagRunOperator(
        task_id="trigger_pydantic_models_dag",
        trigger_dag_id="test_pydantic_models",
        conf=datetime.now(UTC)
    )
    fail_task_decorator() >> fail_python_operator >> trigger
test_email_notifier()
