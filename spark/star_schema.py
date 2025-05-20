from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, expr, year, quarter, month, dayofmonth,
    dayofweek, weekofyear, row_number
)

APPLICATION_NAME = "ETLStarSchemaPipeline"
POSTGRES_JAR = "/opt/spark/jars/postgresql-42.6.0.jar"

POSTGRES_JDBC_URL = "jdbc:postgresql://postgres_db:5432/spark_db"
CONN_PROPS = {
    "user": "spark_user_name",
    "password": "spark_my_secret_password",
    "driver": "org.postgresql.Driver"
}
SOURCE_TABLE = "mock_data"
WRITE_MODE = "append"


def create_spark(app_name, jar_file):
    print(f"Создание SparkSession для {app_name}")
    return SparkSession.builder.appName(app_name).config("spark.jars", jar_file).getOrCreate()


def read_from_postgres(spark, jdbc_url, table_name, properties):
    print(f"Импорт данных из таблицы PostgreSQL: {table_name}")
    return (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", properties["user"])
        .option("password", properties["password"])
        .option("driver", properties["driver"])
        .load()
    )


def write_to_postgres(df, jdbc_url, table_name, mode, properties):
    print(f"Экспорт {df.count()} строк в таблицу {table_name}")
    (
        df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", properties["user"])
        .option("password", properties["password"])
        .option("driver", properties["driver"])
        .mode(mode)
        .save()
    )


def transform_dimension(df, key_column, date_column, mapping, destination):
    print(f"Формирование размерности {destination}")
    window_spec = Window.partitionBy(col(key_column)).orderBy(col(date_column).desc())
    selected_columns = sorted(set([key_column, date_column] + list(mapping.keys())))

    base = df.select(*selected_columns)
    updated = (
        base.withColumn("row_num", row_number().over(window_spec))
            .filter(col("row_num") == 1)
            .drop("row_num", date_column)
    )

    for old, new in mapping.items():
        updated = updated.withColumnRenamed(old, new)

    write_to_postgres(updated, POSTGRES_JDBC_URL, destination, WRITE_MODE, CONN_PROPS)
    return updated


def execute_etl():
    spark = create_spark(APPLICATION_NAME, POSTGRES_JAR)

    raw_df = read_from_postgres(spark, POSTGRES_JDBC_URL, SOURCE_TABLE, CONN_PROPS)
    raw_df.cache()

    transform_dimension(
        raw_df, "sale_customer_id", "sale_date",
        {
            "sale_customer_id": "customer_id",
            "customer_first_name": "first_name",
            "customer_last_name": "last_name",
            "customer_age": "age",
            "customer_email": "email",
            "customer_country": "country",
            "customer_postal_code": "postal_code"
        },
        "dim_customer"
    )

    transform_dimension(
        raw_df, "sale_seller_id", "sale_date",
        {
            "sale_seller_id": "seller_id",
            "seller_first_name": "first_name",
            "seller_last_name": "last_name",
            "seller_email": "email",
            "seller_country": "country",
            "seller_postal_code": "postal_code"
        },
        "dim_seller"
    )

    transform_dimension(
        raw_df, "sale_product_id", "sale_date",
        {
            "sale_product_id": "product_id",
            "product_name": "name",
            "product_category": "category",
            "product_weight": "weight",
            "product_color": "color",
            "product_size": "size",
            "product_brand": "brand",
            "product_material": "material",
            "product_description": "description",
            "product_rating": "rating",
            "product_reviews": "reviews",
            "product_release_date": "release_date",
            "product_expiry_date": "expiry_date",
            "product_price": "unit_price"
        },
        "dim_product"
    )

    transform_dimension(
        raw_df, "store_name", "sale_date",
        {
            "store_name": "name",
            "store_location": "location",
            "store_city": "city",
            "store_state": "state",
            "store_country": "country",
            "store_phone": "phone",
            "store_email": "email"
        },
        "dim_store"
    )

    transform_dimension(
        raw_df, "supplier_name", "sale_date",
        {
            "supplier_name": "name",
            "supplier_contact": "contact",
            "supplier_email": "email",
            "supplier_phone": "phone",
            "supplier_address": "address",
            "supplier_city": "city",
            "supplier_country": "country"
        },
        "dim_supplier"
    )

    print("Создание размерности: dim_date")
    date_df = (
        raw_df
        .select("sale_date")
        .dropna()
        .distinct()
        .withColumn("year", year(col("sale_date")))
        .withColumn("quarter", quarter(col("sale_date")))
        .withColumn("month", month(col("sale_date")))
        .withColumn("month_name", expr("date_format(sale_date, 'MMMM')"))
        .withColumn("day_of_month", dayofmonth("sale_date"))
        .withColumn("day_of_week", dayofweek("sale_date"))
        .withColumn("week_of_year", weekofyear("sale_date"))
        .withColumn("is_weekend", dayofweek("sale_date").isin([1, 7]))
    )
    write_to_postgres(date_df, POSTGRES_JDBC_URL, "dim_date", WRITE_MODE, CONN_PROPS)

    print("Загрузка размерностей для формирования fact-таблицы")
    customer_df = read_from_postgres(spark, POSTGRES_JDBC_URL, "dim_customer", CONN_PROPS).select("customer_sk", "customer_id")
    seller_df = read_from_postgres(spark, POSTGRES_JDBC_URL, "dim_seller", CONN_PROPS).select("seller_sk", "seller_id")
    product_df = read_from_postgres(spark, POSTGRES_JDBC_URL, "dim_product", CONN_PROPS).select("product_sk", "product_id")
    store_df = read_from_postgres(spark, POSTGRES_JDBC_URL, "dim_store", CONN_PROPS).select("store_sk", "name")
    supplier_df = read_from_postgres(spark, POSTGRES_JDBC_URL, "dim_supplier", CONN_PROPS).select("supplier_sk", "name")
    date_df_sk = read_from_postgres(spark, POSTGRES_JDBC_URL, "dim_date", CONN_PROPS).select("date_sk", "sale_date")

    print("Создание fact-таблицы: fact_sales")
    fact_df = (
        raw_df
        .join(date_df_sk, "sale_date")
        .join(customer_df, raw_df.sale_customer_id == customer_df.customer_id)
        .join(seller_df, raw_df.sale_seller_id == seller_df.seller_id)
        .join(product_df, raw_df.sale_product_id == product_df.product_id)
        .join(store_df, raw_df.store_name == store_df.name)
        .join(supplier_df, raw_df.supplier_name == supplier_df.name)
        .select(
            "date_sk", "customer_sk", "seller_sk", "product_sk",
            "store_sk", "supplier_sk", "sale_quantity", "sale_total_price",
            raw_df.product_price.alias("transaction_unit_price")
        )
    )
    write_to_postgres(fact_df, POSTGRES_JDBC_URL, "fact_sales", WRITE_MODE, CONN_PROPS)

    raw_df.unpersist()
    print("ETL завершён.")
    spark.stop()


if __name__ == "__main__":
    execute_etl()
