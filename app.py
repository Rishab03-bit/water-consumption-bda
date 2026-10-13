
import pandas as pd
import streamlit as st
import plotly.express as px
from pathlib import Path

# --------------------------------------------------
# 1. PAGE CONFIGURATION
# --------------------------------------------------
st.set_page_config(
    page_title="Water Demand Analytics",
    page_icon="",
    layout="wide"
)

st.title(" Water Consumption and Demand Pattern Analytics")
st.caption(
    "BDA prototype. Included records are SIMULATED demonstration data, "
)

DATA = Path(__file__).parent / "water_consumption_500k.csv"


# --------------------------------------------------
# 2. LOAD AND CLEAN DATA
# --------------------------------------------------
@st.cache_data
def load_data():
    df = pd.read_csv(DATA)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["consumption_liters"] = pd.to_numeric(
        df["consumption_liters"], errors="coerce"
    )

    df = df.dropna(
        subset=[
            "date",
            "region",
            "consumer_type",
            "consumption_liters"
        ]
    )

    df = df[df["consumption_liters"] >= 0].drop_duplicates()

    df["month"] = df["date"].dt.to_period("M").astype(str)
    df["year"] = df["date"].dt.year.astype(str)

    return df

try:
    df = load_data()

except Exception as e:
    st.error(f"Could not load the dataset: {e}")
    st.info(
        "Ensure water_consumption_500k.csv is in the same "
        "folder as app.py."
    )
    st.stop()

if df.empty:
    st.error("The dataset contains no valid records.")
    st.stop()



# --------------------------------------------------
# 3. SIDEBAR FILTERS
# --------------------------------------------------
with st.sidebar:
    st.header("Filters")

    regions = st.multiselect(
        "Region",
        options=sorted(df["region"].unique()),
        default=sorted(df["region"].unique())
    )

    consumers = st.multiselect(
        "Consumer type",
        options=sorted(df["consumer_type"].unique()),
        default=sorted(df["consumer_type"].unique())
    )

    years = st.multiselect(
        "Year",
        options=sorted(df["year"].unique()),
        default=sorted(df["year"].unique())
    )


data = df[
    df["region"].isin(regions)
    & df["consumer_type"].isin(consumers)
    & df["year"].isin(years)
].copy()

if data.empty:
    st.warning("No records match the selected filters.")
    st.stop()


# --------------------------------------------------
# 4. CREATE A REUSABLE SPARK SESSION
# --------------------------------------------------
@st.cache_resource
def get_spark():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("WaterConsumptionBDA")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark


# --------------------------------------------------
# 5. BIG DATA PROCESSING USING APACHE SPARK
# --------------------------------------------------
engine = "Pandas (fallback)"
spark_error = ""

try:
    from pyspark.sql import functions as F

    spark = get_spark()

    spark_input = data[
        [
            "date",
            "region",
            "consumer_type",
            "consumption_liters"
        ]
    ].copy()

    # Spark expects a consistent date type.
    spark_input["date"] = pd.to_datetime(spark_input["date"])

    # Convert the filtered Pandas data into a Spark DataFrame.
    sdf = spark.createDataFrame(spark_input)

    # Aggregate total consumption by region.
    region_rows = (
        sdf.groupBy("region")
        .agg(
            F.sum("consumption_liters").alias("total_liters")
        )
        .collect()
    )

    # Aggregate total consumption by consumer category.
    consumer_rows = (
        sdf.groupBy("consumer_type")
        .agg(
            F.sum("consumption_liters").alias("total_liters")
        )
        .collect()
    )

    region = pd.DataFrame(
        [row.asDict() for row in region_rows],
        columns=["region", "total_liters"]
    )

    consumer = pd.DataFrame(
        [row.asDict() for row in consumer_rows],
        columns=["consumer_type", "total_liters"]
    )

    engine = "Apache Spark (local mode)"

except Exception as e:
    spark_error = f"{type(e).__name__}: {e}"

    # Pandas fallback allows the dashboard to remain usable.
    region = (
        data.groupby("region", as_index=False)["consumption_liters"]
        .sum()
        .rename(
            columns={"consumption_liters": "total_liters"}
        )
    )

    consumer = (
        data.groupby(
            "consumer_type", as_index=False
        )["consumption_liters"]
        .sum()
        .rename(
            columns={"consumption_liters": "total_liters"}
        )
    )

    st.warning(
        "Spark could not process this selection. "
        "The dashboard is using Pandas as a fallback."
    )

    with st.expander("View Spark error details"):
        st.code(spark_error)


# --------------------------------------------------
# 6. MONTHLY ANALYSIS
# --------------------------------------------------
monthly = (
    data.groupby("month", as_index=False)["consumption_liters"]
    .sum()
    .sort_values("month")
)

peak = monthly.loc[monthly["consumption_liters"].idxmax()]


# --------------------------------------------------
# 7. PROCESSING STATUS
# --------------------------------------------------
st.info(
    f"Processing engine: **{engine}** | "
    f"Records: **{len(data):,}**"
)


# --------------------------------------------------
# 8. KEY PERFORMANCE INDICATORS
# --------------------------------------------------
a, b, c, d = st.columns(4)

a.metric(
    "Total consumption",
    f"{data['consumption_liters'].sum():,.0f} L"
)

b.metric(
    "Average record",
    f"{data['consumption_liters'].mean():,.0f} L"
)

c.metric(
    "Regions",
    str(data["region"].nunique())
)

d.metric(
    "Peak month",
    peak["month"]
)


# --------------------------------------------------
# 9. CONSUMPTION BY REGION AND CONSUMER TYPE
# --------------------------------------------------
x, y = st.columns(2)

with x:
    region_chart = px.bar(
        region.sort_values("total_liters", ascending=False),
        x="region",
        y="total_liters",
        title="Total consumption by region",
        labels={
            "total_liters": "Litres",
            "region": "Region"
        },
        text_auto=".2s"
    )

    st.plotly_chart(
        region_chart,
        use_container_width=True
    )

with y:
    consumer_chart = px.pie(
        consumer,
        names="consumer_type",
        values="total_liters",
        title="Consumption by consumer type"
    )

    st.plotly_chart(
        consumer_chart,
        use_container_width=True
    )


# --------------------------------------------------
# 10. MONTHLY CONSUMPTION TREND
# --------------------------------------------------
monthly_chart = px.line(
    monthly,
    x="month",
    y="consumption_liters",
    markers=True,
    title="Monthly consumption trend",
    labels={
        "month": "Month",
        "consumption_liters": "Litres"
    }
)

st.plotly_chart(
    monthly_chart,
    use_container_width=True
)


# --------------------------------------------------
# 11. KEY INSIGHTS
# --------------------------------------------------
top_region = region.loc[region["total_liters"].idxmax()]
top_consumer = consumer.loc[consumer["total_liters"].idxmax()]

st.subheader("Key insights")

st.write(
    f"- Highest-consumption month: **{peak['month']}** "
    f"({peak['consumption_liters']:,.0f} L)."
)

st.write(
    f"- Highest-consumption region: **{top_region['region']}** "
    f"({top_region['total_liters']:,.0f} L)."
)

st.write(
    f"- Largest consumer category: **{top_consumer['consumer_type']}**."
)


# --------------------------------------------------
# 12. DATA PREVIEW
# --------------------------------------------------
st.subheader("Data preview")

st.dataframe(
    data.drop(columns=["data_type"], errors="ignore").head(100),
    use_container_width=True
)


# --------------------------------------------------
# 13. DOWNLOAD FILTERED DATA
# --------------------------------------------------
csv_data = data.to_csv(index=False).encode("utf-8")

st.download_button(
    label="Download filtered CSV",
    data=csv_data,
    file_name="filtered_water_consumption.csv",
    mime="text/csv"
)

expected_region = (
    data.groupby("region")["consumption_liters"].sum().sort_index()
)

actual_region = (
    region.set_index("region")["total_liters"].sort_index()
)

assert expected_region.index.equals(actual_region.index)
assert (
    (expected_region - actual_region).abs() < 0.01
).all()

st.success("Spark regional aggregation validation passed.")

