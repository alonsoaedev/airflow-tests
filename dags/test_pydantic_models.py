from datetime import date, datetime, timedelta, UTC

from airflow.sdk import dag, task

@dag(
    start_date=datetime.now(UTC),
    catchup=False,
    tags=["test", "pydantic models"],
)
def test_pydantic_models():
    from typing import Generic, TypeVar
    from pydantic import BaseModel

    T = TypeVar("T")

    class Location(BaseModel):
        city: str
        country: str

    class Friend(BaseModel):
        name: str
        age: int | None = None

    class User(BaseModel, Generic[T]):
        name: str
        email: str | None = None
        birthdate: date | None = None
        data: T | None = None

    @task
    def create_user(**kwargs):
        print(kwargs["dag_run"].conf)
        # user = User[Location](
        # user = User[Friend](
        user = User[dict](
            name="Alonso Acosta",
            email="alonso.ae@hotmail.com",
            birthdate=date(1995, 9, 7),
            # data=Location(
            #     city="Zacatecas",
            #     country="México"
            # )
            # data=Friend(
            #     name="Martín",
            #     age=28
            # )
            data={
                "location": {
                    "city": "Zacatecas",
                    "country": "México",
                },
                "friend": {
                    "name": "Martín",
                    "age": 28
                }
            }
        )
        return user.model_dump()
    
    @task
    def modify_user(user: dict):
        user = User.model_validate(user)
        # change the data types of the fields will produce a validation error. e.g.:
        # user.name = 1
        user.name = "María Rosa Acosta"
        user.email = None
        user.birthdate = user.birthdate + timedelta(days=3)
        return user.model_dump()

    @task
    def validate_user(user: dict):
        # user = User[Location].model_validate(user)
        # return f"User {user.name} with email {user.email if user.email else "N/A"} and from {user.data.city}, {user.data.country} is valid."
        # user = User[Friend].model_validate(user)
        # return f"User {user.name} with email {user.email if user.email else "N/A"} and friend {user.data.name} is valid."
        user = User[dict].model_validate(user)
        return f"User {user.name} with email {user.email if user.email else "N/A"} and generic data {user.data} is valid."

    validate_user(
        modify_user(
            create_user()
        )
    )
test_pydantic_models = test_pydantic_models()
