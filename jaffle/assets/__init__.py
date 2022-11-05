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

@asset
def customers(stg_customers: SQL, stg_orders: SQL, stg_payments: SQL) -> SQL:
    return SQL(
        """
with customers as (
    select * from $stg_customers
),
orders as (
    select * from $stg_orders
),
payments as (
    select * from $stg_payments
),
customer_orders as (
    select
        customer_id,
        min(order_date) as first_order,
        max(order_date) as most_recent_order,
        count(order_id) as number_of_orders
    from orders
    group by customer_id
),
customer_payments as (
    select
        orders.customer_id,
        sum(amount) as total_amount
    from payments
    left join orders on
         payments.order_id = orders.order_id
    group by orders.customer_id
),
final as (
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order,
        customer_orders.most_recent_order,
        customer_orders.number_of_orders,
        customer_payments.total_amount as customer_lifetime_value
    from customers
    left join customer_orders
        on customers.customer_id = customer_orders.customer_id
    left join customer_payments
        on  customers.customer_id = customer_payments.customer_id
)

select * from final
    """,
        stg_customers=stg_customers,
        stg_orders=stg_orders,
        stg_payments=stg_payments,
    )