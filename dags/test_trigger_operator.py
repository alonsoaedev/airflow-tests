from datetime import date, datetime, timedelta, UTC

from airflow.sdk import dag, task

default_args = {
    "email": ["alonso.ae@hotmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
}

@dag(
    start_date=datetime.now(UTC),
    catchup=False,
    tags=["test", "trigger operator", "deserialization", "pydantic models"],
    default_args=default_args,
)
def test_trigger_operator():
    from pydantic import BaseModel, field_serializer
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

    class User(BaseModel):
        name: str
        email: str | None = None
        birthdate: date
        created_at: datetime | None = datetime.now(UTC)

        @field_serializer("birthdate")
        def serialize_birthdate(self, birthdate: date, _info) -> str:
            if not birthdate:
                return birthdate
            return birthdate.isoformat()

        @field_serializer("created_at")
        def serialize_created_at(self, created_at: datetime, _info) -> str | None:
            if not created_at:
                return created_at
            return created_at.isoformat()

    @task
    def generate_users() -> list[dict]:
        users: list[User] = [
            User(
                name="Alonso",
                email="alonso.ae@hotmail.com",
                birthdate=date(1995, 9, 7),
            ),
            User(
                name="Arturo",
                birthdate=date(1995, 9, 7),
            ),
            User(
                name="Rosa",
                birthdate=date(1995, 9, 10),
            ),
        ]
        return [
            user.model_dump()
            for user in users
        ]

    @task
    def generate_trigger_kwags(users: list[dict]) -> list[dict]:
        users: User = [User.model_validate(user) for user in users]
        return [
            {
                "trigger_run_id": f"test_pydantic_models__test__{user.name}_{user.created_at.isoformat()}",
                "conf": user.model_dump()
            }
            for user in users
        ]

    TriggerDagRunOperator.partial(
        task_id="trigger_dag",
        trigger_dag_id="test_pydantic_models",
        wait_for_completion=True,
        poke_interval=30,
    # ).expand(
    #     conf=generate_users()
    # )
    ).expand_kwargs(
        generate_trigger_kwags(
            users=generate_users()
        )
    )
test_trigger_operator()
