from airflow.listeners import hookimpl
from airflow.models import TaskInstance
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
from airflow.utils.state import TaskInstanceState
# from airflow.models.dag_run import DagRun

@hookimpl
def on_task_instance_failed(
    previous_state: TaskInstanceState,
    task_instance: RuntimeTaskInstance | TaskInstance,
    error: None | str | BaseException,
):
    from datetime import datetime, UTC
    from json import loads

    from airflow.configuration import conf
    from airflow.sdk import Variable
    from airflow.utils.email import send_email

    # context = task_instance.get_template_context() # example of how to get the context
    base_url = conf.get(
        "api",
        "base_url",
        fallback="http://localhost:8080/"
    )
    error_log_url = f"{base_url}dags/{task_instance.dag_id}/runs/{task_instance.run_id}/tasks/{task_instance.task_id}"
    error = error if error else f"Can't find the error"
    task_emails = loads(
        Variable.get(
            f"{task_instance.dag_id}_emails",
            default='{"dev": null, "qa": ["alonso.ae@hotmail.com"], "prod": ["alonso.ae@icloud.com"]}'
        )
    )

    if not task_emails.get("prod"):
        return

    send_email(
        to=task_emails["prod"],
        subject=f"[test] Airflow error on {task_instance.dag_id}.{task_instance.task_id}",
        html_content=f"""
            <h3>Details</h3>
            <p>Environment: test</p>
            <p>Dag ID: {task_instance.dag_id}</p>
            <p>Task ID: {task_instance.task_id}</p>
            <p>Date: {datetime.now(UTC).isoformat()}</p>
            <p>Exception: {error} ({error_log_url})</p>
        """
    )

# @hookimpl
# def on_dag_run_failed(dag_run: DagRun, msg: str):
#     """
#     This method is called when dag run state changes to FAILED.
#     """
#     print("Dag run  in failure state")
#     dag_id = dag_run.dag_id
#     run_id = dag_run.run_id
#     run_type = dag_run.run_type

#     print(f"Dag information:{dag_id} Run id: {run_id} Run type: {run_type}")
#     print(f"Failed with message: {msg}")

# @hookimpl
# def on_dag_run_failed(dag_run: DagRun, msg: str):
#     """
#     This method is called when dag run state changes to FAILED.
#     """
#     print("Dag run  in failure state")
#     dag_id = dag_run.dag_id
#     run_id = dag_run.run_id
#     run_type = dag_run.run_type

#     print(f"Dag information:{dag_id} Run id: {run_id} Run type: {run_type}")
#     print(f"Failed with message: {msg}")
