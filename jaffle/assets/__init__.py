from dagster import asset
from jaffle.duckpond import SQL
import pandas as pd

@asset
def stg_customers() -> SQL:
    df = pd.read_csv(
        "https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/raw_customers.csv"
    )
    df.rename(columns={"id": "customer_id"}, inplace=True)
    return SQL("select * from $df", df=df)

@asset
def stg_orders() -> SQL:
    df = pd.read_csv(
        "https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/raw_orders.csv"
    )
    df.rename(columns={"id": "order_id", "user_id": "customer_id"}, inplace=True)
    return SQL("select * from $df", df=df)

@asset
def stg_payments() -> SQL:
    df = pd.read_csv(
        "https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/raw_payments.csv"
    )
    df.rename(columns={"id": "payment_id"}, inplace=True)
    df["amount"] = df["amount"].map(lambda amount: amount / 100)
    return SQL("select * from $df", df=df)