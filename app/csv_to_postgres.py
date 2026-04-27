import os
import psycopg2
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F

PGHOST = os.getenv("PGHOST", "postgres")
PGPORT = os.getenv("PGPORT", "5432")
PGDATABASE = os.getenv("PGDATABASE", "app")
PGUSER = os.getenv("PGUSER", "user")
PGPASSWORD = os.getenv("PGPASSWORD", "password")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

JDBC_URL = f"jdbc:postgresql://{PGHOST}:{PGPORT}/{PGDATABASE}"
JDBC_PROPS = {
    "user": PGUSER,
    "password": PGPASSWORD,
    "driver": "org.postgresql.Driver",
}


def t(name: str):
    return F.trim(F.col(name))


def tnull(name: str):
    value = F.trim(F.col(name))
    return F.when(value == "", F.lit(None)).otherwise(value)


def add_surrogate_id(df: DataFrame, order_cols: list[str]) -> DataFrame:
    w = Window.orderBy(*[F.col(c).asc_nulls_first() for c in order_cols])
    return df.withColumn("id", F.row_number().over(w))


def parse_mixed_date(col_name: str):
    c = F.trim(F.col(col_name))
    return F.coalesce(
        F.to_date(c, "M/d/yyyy"),
        F.to_date(c, "MM/d/yyyy"),
        F.to_date(c, "M/dd/yyyy"),
        F.to_date(c, "MM/dd/yyyy"),
    )

def write_table(df: DataFrame, table: str) -> None:
    (
        df.write
        .mode("append")
        .jdbc(url=JDBC_URL, table=table, properties=JDBC_PROPS)
    )


def ensure_target_schema() -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS customer_pet
    (
        id       INTEGER PRIMARY KEY,
        type     VARCHAR NOT NULL,
        name     VARCHAR NOT NULL,
        breed    VARCHAR NOT NULL,
        category VARCHAR NOT NULL
    );

    CREATE TABLE IF NOT EXISTS customer
    (
        id              INTEGER PRIMARY KEY,
        first_name      VARCHAR NOT NULL,
        last_name       VARCHAR NOT NULL,
        age             INTEGER NOT NULL,
        email           VARCHAR NOT NULL,
        country         VARCHAR NOT NULL,
        postal_code     VARCHAR,
        customer_pet_id INTEGER REFERENCES customer_pet (id)
    );

    CREATE TABLE IF NOT EXISTS seller
    (
        id          INTEGER PRIMARY KEY,
        first_name  VARCHAR NOT NULL,
        last_name   VARCHAR NOT NULL,
        email       VARCHAR NOT NULL,
        country     VARCHAR NOT NULL,
        postal_code VARCHAR
    );

    CREATE TABLE IF NOT EXISTS store
    (
        id       INTEGER PRIMARY KEY,
        name     VARCHAR NOT NULL,
        location VARCHAR NOT NULL,
        city     VARCHAR NOT NULL,
        state    VARCHAR,
        country  VARCHAR NOT NULL,
        phone    VARCHAR NOT NULL,
        email    VARCHAR NOT NULL
    );

    CREATE TABLE IF NOT EXISTS supplier
    (
        id      INTEGER PRIMARY KEY,
        name    VARCHAR NOT NULL,
        contact VARCHAR NOT NULL,
        email   VARCHAR NOT NULL,
        phone   VARCHAR NOT NULL,
        address VARCHAR NOT NULL,
        city    VARCHAR NOT NULL,
        country VARCHAR NOT NULL
    );

    CREATE TABLE IF NOT EXISTS product
    (
        id                 INTEGER PRIMARY KEY,
        name               VARCHAR        NOT NULL,
        category           VARCHAR        NOT NULL,
        price              NUMERIC(10, 2) NOT NULL,
        available_quantity INTEGER        NOT NULL,
        weight             NUMERIC(10, 1) NOT NULL,
        color              VARCHAR        NOT NULL,
        size               VARCHAR        NOT NULL,
        brand              VARCHAR        NOT NULL,
        material           VARCHAR        NOT NULL,
        description        VARCHAR        NOT NULL,
        rating             NUMERIC(2, 1)  NOT NULL,
        reviews            INTEGER        NOT NULL,
        release_date       DATE           NOT NULL,
        expiry_date        DATE           NOT NULL,
        supplier_id        INTEGER        NOT NULL REFERENCES supplier (id)
    );

    CREATE TABLE IF NOT EXISTS sales_fact
    (
        id            INTEGER PRIMARY KEY,
        customer_id   INTEGER        NOT NULL REFERENCES customer (id),
        seller_id     INTEGER        NOT NULL REFERENCES seller (id),
        store_id      INTEGER        NOT NULL REFERENCES store (id),
        product_id    INTEGER        NOT NULL REFERENCES product (id),
        date          DATE           NOT NULL,
        sale_quantity INTEGER        NOT NULL,
        total_price   NUMERIC(12, 2) NOT NULL
    );
    """

    truncate_sql = """
    TRUNCATE TABLE sales_fact, product, customer, seller, store, supplier, customer_pet
    RESTART IDENTITY CASCADE;
    """

    with psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
            cur.execute(truncate_sql)
        conn.commit()


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("pg-to-snowflake")
        .master(SPARK_MASTER_URL)
        .config("spark.driver.host", "jupyter")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.executor.instances", "1")
        .config("spark.executor.cores", "1")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    print(spark.sparkContext.getConf().get("spark.jars.packages"))
    spark.sparkContext.setLogLevel("WARN")

    ensure_target_schema()

    src = spark.read.jdbc(url=JDBC_URL, table="input_csv", properties=JDBC_PROPS)

    supplier_base = (
        src.select(
            t("supplier_name").alias("name"),
            t("supplier_contact").alias("contact"),
            t("supplier_email").alias("email"),
            t("supplier_phone").alias("phone"),
            t("supplier_address").alias("address"),
            t("supplier_city").alias("city"),
            t("supplier_country").alias("country"),
        )
        .dropDuplicates()
    )

    supplier_df = (
        add_surrogate_id(
            supplier_base,
            ["name", "contact", "email", "phone", "address", "city", "country"],
        )
        .select("id", "name", "contact", "email", "phone", "address", "city", "country")
    )

    customer_pet_base = (
        src.select(
            t("customer_pet_type").alias("type"),
            t("customer_pet_name").alias("name"),
            t("customer_pet_breed").alias("breed"),
            t("pet_category").alias("category"),
        )
    )

    customer_pet_df = (
        add_surrogate_id(customer_pet_base, ["type", "name", "breed", "category"])
        .select("id", "type", "name", "breed", "category")
    )

    seller_base = (
        src.select(
            t("seller_first_name").alias("first_name"),
            t("seller_last_name").alias("last_name"),
            t("seller_email").alias("email"),
            t("seller_country").alias("country"),
            tnull("seller_postal_code").alias("postal_code"),
        )
        .dropDuplicates()
    )

    seller_df = (
        add_surrogate_id(
            seller_base,
            ["first_name", "last_name", "email", "country", "postal_code"],
        )
        .select("id", "first_name", "last_name", "email", "country", "postal_code")
    )

    store_base = (
        src.select(
            t("store_name").alias("name"),
            t("store_location").alias("location"),
            t("store_city").alias("city"),
            tnull("store_state").alias("state"),
            t("store_country").alias("country"),
            t("store_phone").alias("phone"),
            t("store_email").alias("email"),
        )
        .dropDuplicates()
    )

    store_df = (
        add_surrogate_id(
            store_base,
            ["name", "location", "city", "state", "country", "phone", "email"],
        )
        .select("id", "name", "location", "city", "state", "country", "phone", "email")
    )

    customer_base = (
        src.select(
            t("customer_first_name").alias("first_name"),
            t("customer_last_name").alias("last_name"),
            t("customer_age").cast("int").alias("age"),
            t("customer_email").alias("email"),
            t("customer_country").alias("country"),
            tnull("customer_postal_code").alias("postal_code"),
            t("customer_pet_type").alias("pet_type"),
            t("customer_pet_name").alias("pet_name"),
            t("customer_pet_breed").alias("pet_breed"),
            t("pet_category").alias("pet_category"),
        )
        .dropDuplicates()
    )

    customer_df = (
        customer_base.alias("c")
        .join(
            customer_pet_df.alias("cp"),
            (F.col("c.pet_type") == F.col("cp.type"))
            & (F.col("c.pet_name") == F.col("cp.name"))
            & (F.col("c.pet_breed") == F.col("cp.breed"))
            & (F.col("c.pet_category") == F.col("cp.category")),
            "inner",
        )
        .select(
            F.col("c.first_name").alias("first_name"),
            F.col("c.last_name").alias("last_name"),
            F.col("c.age").alias("age"),
            F.col("c.email").alias("email"),
            F.col("c.country").alias("country"),
            F.col("c.postal_code").alias("postal_code"),
            F.col("cp.id").alias("customer_pet_id"),
        )
    )

    customer_df = (
        add_surrogate_id(
            customer_df,
            ["first_name", "last_name", "age", "email", "country", "postal_code", "customer_pet_id"],
        )
        .select("id", "first_name", "last_name", "age", "email", "country", "postal_code", "customer_pet_id")
    )

    product_base = (
        src.select(
            t("product_name").alias("name"),
            t("product_category").alias("category"),
            t("product_price").cast("decimal(10,2)").alias("price"),
            t("product_quantity").cast("int").alias("available_quantity"),
            t("product_weight").cast("decimal(10,1)").alias("weight"),
            t("product_color").alias("color"),
            t("product_size").alias("size"),
            t("product_brand").alias("brand"),
            t("product_material").alias("material"),
            t("product_description").alias("description"),
            t("product_rating").cast("decimal(2,1)").alias("rating"),
            t("product_reviews").cast("int").alias("reviews"),
            parse_mixed_date("product_release_date").alias("release_date"),
            parse_mixed_date("product_expiry_date").alias("expiry_date"),
            t("supplier_name").alias("supplier_name"),
            t("supplier_contact").alias("supplier_contact"),
            t("supplier_email").alias("supplier_email"),
            t("supplier_phone").alias("supplier_phone"),
            t("supplier_address").alias("supplier_address"),
            t("supplier_city").alias("supplier_city"),
            t("supplier_country").alias("supplier_country"),
        )
        .dropDuplicates()
    )

    product_df = (
        product_base.alias("p")
        .join(
            supplier_df.alias("s"),
            (F.col("p.supplier_name") == F.col("s.name"))
            & (F.col("p.supplier_contact") == F.col("s.contact"))
            & (F.col("p.supplier_email") == F.col("s.email"))
            & (F.col("p.supplier_phone") == F.col("s.phone"))
            & (F.col("p.supplier_address") == F.col("s.address"))
            & (F.col("p.supplier_city") == F.col("s.city"))
            & (F.col("p.supplier_country") == F.col("s.country")),
            "inner",
        )
        .select(
            F.col("p.name").alias("name"),
            F.col("p.category").alias("category"),
            F.col("p.price").alias("price"),
            F.col("p.available_quantity").alias("available_quantity"),
            F.col("p.weight").alias("weight"),
            F.col("p.color").alias("color"),
            F.col("p.size").alias("size"),
            F.col("p.brand").alias("brand"),
            F.col("p.material").alias("material"),
            F.col("p.description").alias("description"),
            F.col("p.rating").alias("rating"),
            F.col("p.reviews").alias("reviews"),
            F.col("p.release_date").alias("release_date"),
            F.col("p.expiry_date").alias("expiry_date"),
            F.col("s.id").alias("supplier_id"),
        )
    )

    product_df = (
        add_surrogate_id(
            product_df,
            [
                "name", "category", "price", "available_quantity", "weight", "color", "size",
                "brand", "material", "description", "rating", "reviews",
                "release_date", "expiry_date", "supplier_id",
            ],
        )
        .select(
            "id", "name", "category", "price", "available_quantity", "weight", "color",
            "size", "brand", "material", "description", "rating", "reviews",
            "release_date", "expiry_date", "supplier_id",
        )
    )

    sales_base = src.select(
        parse_mixed_date("sale_date").alias("sale_date"),
        t("sale_quantity").cast("int").alias("sale_quantity"),
        t("sale_total_price").cast("decimal(12,2)").alias("total_price"),

        t("customer_first_name").alias("customer_first_name"),
        t("customer_last_name").alias("customer_last_name"),
        t("customer_age").cast("int").alias("customer_age"),
        t("customer_email").alias("customer_email"),
        t("customer_country").alias("customer_country"),
        tnull("customer_postal_code").alias("customer_postal_code"),
        t("customer_pet_type").alias("customer_pet_type"),
        t("customer_pet_name").alias("customer_pet_name"),
        t("customer_pet_breed").alias("customer_pet_breed"),
        t("pet_category").alias("pet_category"),

        t("seller_first_name").alias("seller_first_name"),
        t("seller_last_name").alias("seller_last_name"),
        t("seller_email").alias("seller_email"),
        t("seller_country").alias("seller_country"),
        tnull("seller_postal_code").alias("seller_postal_code"),

        t("product_name").alias("product_name"),
        t("product_category").alias("product_category"),
        t("product_price").cast("decimal(10,2)").alias("product_price"),
        t("product_quantity").cast("int").alias("product_quantity"),
        t("product_weight").cast("decimal(10,1)").alias("product_weight"),
        t("product_color").alias("product_color"),
        t("product_size").alias("product_size"),
        t("product_brand").alias("product_brand"),
        t("product_material").alias("product_material"),
        t("product_description").alias("product_description"),
        t("product_rating").cast("decimal(2,1)").alias("product_rating"),
        t("product_reviews").cast("int").alias("product_reviews"),
        parse_mixed_date("product_release_date").alias("product_release_date"),
        parse_mixed_date("product_expiry_date").alias("product_expiry_date"),

        t("supplier_name").alias("supplier_name"),
        t("supplier_contact").alias("supplier_contact"),
        t("supplier_email").alias("supplier_email"),
        t("supplier_phone").alias("supplier_phone"),
        t("supplier_address").alias("supplier_address"),
        t("supplier_city").alias("supplier_city"),
        t("supplier_country").alias("supplier_country"),

        t("store_name").alias("store_name"),
        t("store_location").alias("store_location"),
        t("store_city").alias("store_city"),
        tnull("store_state").alias("store_state"),
        t("store_country").alias("store_country"),
        t("store_phone").alias("store_phone"),
        t("store_email").alias("store_email"),
    )

    sales_df = (
        sales_base.alias("t")
        .join(
            customer_pet_df.alias("cp"),
            (F.col("t.customer_pet_type") == F.col("cp.type"))
            & (F.col("t.customer_pet_name") == F.col("cp.name"))
            & (F.col("t.customer_pet_breed") == F.col("cp.breed"))
            & (F.col("t.pet_category") == F.col("cp.category")),
            "inner",
        )
        .join(
            customer_df.alias("c"),
            (F.col("c.first_name") == F.col("t.customer_first_name"))
            & (F.col("c.last_name") == F.col("t.customer_last_name"))
            & (F.col("c.age") == F.col("t.customer_age"))
            & (F.col("c.email") == F.col("t.customer_email"))
            & (F.col("c.country") == F.col("t.customer_country"))
            & (F.col("c.postal_code").eqNullSafe(F.col("t.customer_postal_code")))
            & (F.col("c.customer_pet_id") == F.col("cp.id")),
            "inner",
        )
        .join(
            seller_df.alias("se"),
            (F.col("se.first_name") == F.col("t.seller_first_name"))
            & (F.col("se.last_name") == F.col("t.seller_last_name"))
            & (F.col("se.email") == F.col("t.seller_email"))
            & (F.col("se.country") == F.col("t.seller_country"))
            & (F.col("se.postal_code").eqNullSafe(F.col("t.seller_postal_code"))),
            "inner",
        )
        .join(
            supplier_df.alias("sup"),
            (F.col("sup.name") == F.col("t.supplier_name"))
            & (F.col("sup.contact") == F.col("t.supplier_contact"))
            & (F.col("sup.email") == F.col("t.supplier_email"))
            & (F.col("sup.phone") == F.col("t.supplier_phone"))
            & (F.col("sup.address") == F.col("t.supplier_address"))
            & (F.col("sup.city") == F.col("t.supplier_city"))
            & (F.col("sup.country") == F.col("t.supplier_country")),
            "inner",
        )
        .join(
            product_df.alias("p"),
            (F.col("p.name") == F.col("t.product_name"))
            & (F.col("p.category") == F.col("t.product_category"))
            & (F.col("p.price") == F.col("t.product_price"))
            & (F.col("p.available_quantity") == F.col("t.product_quantity"))
            & (F.col("p.weight") == F.col("t.product_weight"))
            & (F.col("p.color") == F.col("t.product_color"))
            & (F.col("p.size") == F.col("t.product_size"))
            & (F.col("p.brand") == F.col("t.product_brand"))
            & (F.col("p.material") == F.col("t.product_material"))
            & (F.col("p.description") == F.col("t.product_description"))
            & (F.col("p.rating") == F.col("t.product_rating"))
            & (F.col("p.reviews") == F.col("t.product_reviews"))
            & (F.col("p.release_date") == F.col("t.product_release_date"))
            & (F.col("p.expiry_date") == F.col("t.product_expiry_date"))
            & (F.col("p.supplier_id") == F.col("sup.id")),
            "inner",
        )
        .join(
            store_df.alias("st"),
            (F.col("st.name") == F.col("t.store_name"))
            & (F.col("st.location") == F.col("t.store_location"))
            & (F.col("st.city") == F.col("t.store_city"))
            & (F.col("st.state").eqNullSafe(F.col("t.store_state")))
            & (F.col("st.country") == F.col("t.store_country"))
            & (F.col("st.phone") == F.col("t.store_phone"))
            & (F.col("st.email") == F.col("t.store_email")),
            "inner",
        )
        .select(
            F.col("c.id").alias("customer_id"),
            F.col("se.id").alias("seller_id"),
            F.col("st.id").alias("store_id"),
            F.col("p.id").alias("product_id"),
            F.col("t.sale_date").alias("date"),
            F.col("t.sale_quantity").alias("sale_quantity"),
            F.col("t.total_price").alias("total_price"),
        )
    )

    sales_df = (
        add_surrogate_id(
            sales_df,
            ["date", "customer_id", "seller_id", "store_id", "product_id", "sale_quantity", "total_price"],
        )
        .select("id", "customer_id", "seller_id", "store_id", "product_id", "date", "sale_quantity", "total_price")
    )

    write_table(supplier_df, "supplier")
    write_table(customer_pet_df, "customer_pet")
    write_table(seller_df, "seller")
    write_table(store_df, "store")
    write_table(customer_df, "customer")
    write_table(product_df, "product")
    write_table(sales_df, "sales_fact")

    print("Loaded rows:")
    for name, df in [
        ("supplier", supplier_df),
        ("customer_pet", customer_pet_df),
        ("seller", seller_df),
        ("store", store_df),
        ("customer", customer_df),
        ("product", product_df),
        ("sales_fact", sales_df),
    ]:
        print(f"{name}: {df.count()}")

    spark.stop()


if __name__ == "__main__":
    main()