from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as sf

APP_TITLE = "SalesAnalyticsPipeline"
POSTGRES_DRIVER_PATH = "/opt/spark/jars/postgresql-42.6.0.jar"
CLICKHOUSE_DRIVER_PATH = "/opt/spark/jars/clickhouse-jdbc-0.4.6.jar"

POSTGRES_URL = "jdbc:postgresql://postgres_db:5432/spark_db"
POSTGRES_OPTIONS = {
    "user": "spark_user_name",
    "password": "spark_my_secret_password",
    "driver": "org.postgresql.Driver"
}

CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/default"
CLICKHOUSE_OPTIONS = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}
CLICKHOUSE_SAVE_MODE = "overwrite"
CLICKHOUSE_ENGINE_DEFAULT = "ENGINE = MergeTree() ORDER BY tuple()"


def create_session(name, *jar_paths):
    joined_jars = ",".join(jar_paths)
    return (SparkSession.builder
            .appName(name)
            .config("spark.jars", joined_jars)
            .getOrCreate())


def read_table(spark, table):
    return (spark.read.format("jdbc")
            .option("url", POSTGRES_URL)
            .option("dbtable", table)
            .options(**POSTGRES_OPTIONS)
            .load())


def write_to_clickhouse(df, table, order_cols=None):
    engine = CLICKHOUSE_ENGINE_DEFAULT
    if "MergeTree" in engine and order_cols:
        engine = f"ENGINE = MergeTree() ORDER BY ({', '.join(order_cols)})"

    (df.write
        .format("jdbc")
        .mode(CLICKHOUSE_SAVE_MODE)
        .option("url", CLICKHOUSE_URL)
        .option("dbtable", table)
        .option("createTableOptions", engine)
        .options(**CLICKHOUSE_OPTIONS)
        .save())


def build_reports(spark):
    sales = read_table(spark, "fact_sales").cache()
    products = read_table(spark, "dim_product").cache()
    customers = read_table(spark, "dim_customer").cache()
    dates = read_table(spark, "dim_date").cache()

    #Топ 10 товаров по количеству продаж
    top_products = (sales.groupBy("product_sk")
                    .agg(sf.sum("sale_quantity").alias("qty"),
                         sf.sum("sale_total_price").alias("revenue"))
                    .join(products, "product_sk")
                    .select(
                        products.product_id.alias("product_id"),
                        products.name.alias("name"),
                        products.category.alias("category"),
                        "qty", "revenue"
                    )
                    .orderBy(sf.desc("qty"))
                    .limit(10))
    write_to_clickhouse(top_products, "mart_top_products", order_cols=["qty"])

    #Выручка по категориям
    by_category = (sales.join(products, "product_sk")
                   .groupBy(products.category.alias("category"))
                   .agg(sf.sum("sale_total_price").alias("total_revenue"))
                   .orderBy(sf.desc("total_revenue")))
    write_to_clickhouse(by_category, "mart_category_revenue", order_cols=["category"])

    #Обратная связь по продуктам
    feedback = (products.select(
                    products.product_id.alias("product_id"),
                    products.name.alias("name"),
                    products.category.alias("category"),
                    products.rating.alias("avg_rating"),
                    products.reviews.alias("review_count"))
                .orderBy(sf.desc("review_count")))
    write_to_clickhouse(feedback, "mart_product_feedback", order_cols=["product_id"])

    #Топ клиенты по покупкам
    top_customers = (sales.groupBy("customer_sk")
                     .agg(sf.sum("sale_total_price").alias("total_spent"))
                     .join(customers, "customer_sk")
                     .select(customers.customer_id.alias("customer_id"),
                             customers.first_name, customers.last_name,
                             customers.country.alias("country"), "total_spent")
                     .orderBy(sf.desc("total_spent"))
                     .limit(10))
    write_to_clickhouse(top_customers, "mart_top_customers", order_cols=["total_spent"])

    #Средний чек на клиента
    avg_order = (sales.groupBy("customer_sk")
                 .agg((sf.sum("sale_total_price") / sf.countDistinct("sale_sk"))
                      .alias("avg_order_value"))
                 .join(customers, "customer_sk")
                 .select(customers.customer_id.alias("customer_id"),
                         customers.first_name, customers.last_name,
                         "avg_order_value")
                 .orderBy(sf.desc("avg_order_value")))
    write_to_clickhouse(avg_order, "mart_avg_order", order_cols=["customer_id"])

    #Месячные тренды продаж
    sales_with_dates = sales.join(dates, "date_sk")
    monthly_trends = (sales_with_dates.groupBy(dates.year, dates.month, dates.month_name)
                      .agg(sf.sum("sale_total_price").alias("monthly_revenue"),
                           sf.sum("sale_quantity").alias("monthly_quantity"))
                      .orderBy(dates.year, dates.month))
    write_to_clickhouse(monthly_trends, "mart_monthly_trends", order_cols=["year", "month"])


if __name__ == "__main__":
    spark = create_session(APP_TITLE, POSTGRES_DRIVER_PATH, CLICKHOUSE_DRIVER_PATH)
    build_reports(spark)
