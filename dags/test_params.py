from datetime import date, datetime, timedelta, UTC

from airflow.sdk import dag, task
from airflow.models.param import Param

@dag(
    start_date=datetime.now(UTC),
    catchup=False,
    tags=["test", "params"],
    params={
        "execution_type": Param(
            "automatic",
            title="Execution type",
            description="Type of execution for the DAG.",
            type="string",
            enum=["automatic", "manual"],
            values_display={
                "automatic": "Automatic. It'll apply some rules to the date field.",
                "manual": "Manual. It'llwill use the date as provided.",
            },
        ),
        "date": Param(
            f"{date.today()}",
            title="Date",
            description="Date to retrieve the commerce data.",
            type="string",
            format="date",
        ),
        "use_date_range": Param(
            False,
            title="Use date range",
            description="If it's on the DAG will use the fields \"From\" and \"To\" and avoid the field \"Date\".",
            type="boolean",
        ),
        "from": Param(
            f"{date.today()}",
            title="From",
            description="Starting date to retrieve the commerce data.",
            type="string",
            format="date",
        ),
        "to": Param(
            f"{date.today()}",
            title="To",
            description="Ending date to retrieve the commerce data.",
            type="string",
            format="date",
        ),
        "codes": Param(
            None,
            title="Codes",
            description="List of codes to filter the data.",
            type=["array", "null"],
            items={"type": "string"}
        ),
    },
)
def test_params():
    def get_dates_given_execution_type(date_: date, execution_type: str) -> tuple[date, date]:
        if execution_type == "automatic" and date_.weekday() == 0:
            return (
                date_ - timedelta(days=3),
                date_ - timedelta(days=1)
            )

        if execution_type == "automatic":
            return (
                date_ - timedelta(days=1),
                date_ - timedelta(days=1)
            )

        return date_, date_

    @task
    def print_params(**context) -> None:
        execution_type = context["params"].get("execution_type")
        use_date_range = context["params"].get("use_date_range")
        start_date, end_date = get_dates_given_execution_type(
            datetime.strptime(context["params"].get("date"), "%Y-%m-%d").date(),
            context["params"].get("execution_type")
        )
        if use_date_range:
            start_date = datetime.strptime(context["params"].get("from"), "%Y-%m-%d").date()
            end_date = datetime.strptime(context["params"].get("to"), "%Y-%m-%d").date()

        print(
            "Processed parameters:",
            f"Execution type: {execution_type} ({type(execution_type)})",
            f"Start date: {start_date} ({type(start_date)})",
            f"End date: {end_date} ({type(end_date)})",
            sep="\n",
        )

        print(context["params"].get("codes"))

    print_params()
test_params()
