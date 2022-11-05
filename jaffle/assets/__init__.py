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
