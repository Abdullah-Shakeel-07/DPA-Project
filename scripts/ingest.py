# read the rew csv files and convert them to the parquet files
# docker compose exec notebook python project/scripts/ingest.py
import glob
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
RAW = os.path.join(ROOT, "data", "raw")
OUT = os.path.join(ROOT, "data", "parquet")

spark = (SparkSession.builder
         .appName("dpa-ingest")
         .config("spark.driver.memory", "4g")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")


# for flights
# the files have 110 columns but we only needs these ones
flight_types = {
    "Year": IntegerType(), "Month": IntegerType(),
    "DayofMonth": IntegerType(), "DayOfWeek": IntegerType(),
    "FlightDate": StringType(),
    "Reporting_Airline": StringType(), "Tail_Number": StringType(),
    "Flight_Number_Reporting_Airline": IntegerType(),
    "Origin": StringType(), "OriginCityName": StringType(), "OriginState": StringType(),
    "Dest": StringType(), "DestCityName": StringType(), "DestState": StringType(),
    "CRSDepTime": StringType(), "DepTime": StringType(),
    "CRSArrTime": StringType(), "ArrTime": StringType(),
    "DepDelay": DoubleType(), "DepDelayMinutes": DoubleType(), "DepDel15": DoubleType(),
    "TaxiOut": DoubleType(),
    "ArrDelay": DoubleType(), "ArrDelayMinutes": DoubleType(), "ArrDel15": DoubleType(),
    "Cancelled": DoubleType(), "CancellationCode": StringType(), "Diverted": DoubleType(),
    "CRSElapsedTime": DoubleType(), "ActualElapsedTime": DoubleType(),
    "AirTime": DoubleType(), "Distance": DoubleType(),
    "CarrierDelay": DoubleType(), "WeatherDelay": DoubleType(), "NASDelay": DoubleType(),
    "SecurityDelay": DoubleType(), "LateAircraftDelay": DoubleType(),
}

flight_files = sorted(glob.glob(os.path.join(RAW, "bts", "*.csv")))
print("flight files found:", len(flight_files))

with open(flight_files[0], encoding="latin-1") as f:
    header = [h.strip().strip('"') for h in f.readline().split(",")]
header = [h if h else f"unused_{i}" for i, h in enumerate(header)]  # last column has no name

flight_schema = StructType([StructField(h, flight_types.get(h, StringType())) for h in header])

flights = (spark.read
           .option("header", True)
           .option("escape", '"')
           .schema(flight_schema)
           .csv(flight_files)
           .select(list(flight_types.keys())))

flights.write.mode("overwrite").partitionBy("Year", "Month").parquet(os.path.join(OUT, "flights"))
print("flights data done:", spark.read.parquet(os.path.join(OUT, "flights")).count(), "rows")


# airports data
airport_schema = StructType([
    StructField("id", IntegerType()), StructField("ident", StringType()),
    StructField("type", StringType()), StructField("name", StringType()),
    StructField("latitude_deg", DoubleType()), StructField("longitude_deg", DoubleType()),
    StructField("elevation_ft", IntegerType()), StructField("continent", StringType()),
    StructField("iso_country", StringType()), StructField("iso_region", StringType()),
    StructField("municipality", StringType()), StructField("scheduled_service", StringType()),
    StructField("icao_code", StringType()), StructField("iata_code", StringType()),
    StructField("gps_code", StringType()), StructField("local_code", StringType()),
    StructField("home_link", StringType()), StructField("wikipedia_link", StringType()),
    StructField("keywords", StringType()),
])

airports = (spark.read
            .option("header", True)
            .option("escape", '"')
            .schema(airport_schema)
            .csv(os.path.join(RAW, "airports.csv")))

airports.write.mode("overwrite").parquet(os.path.join(OUT, "airports"))
print("airports data done:", airports.count(), "rows")


# weather data
# each station file has a different set of columns, so we read them one by one
# and only keep the columns we need

weather_cols = ["STATION", "DATE", "REPORT_TYPE", "CALL_SIGN",
                "WND", "CIG", "VIS", "TMP", "DEW", "AA1"]

parts = []
for path in sorted(glob.glob(os.path.join(RAW, "noaa", "20*", "*.csv"))):
    df = spark.read.option("header", True).option("escape", '"').csv(path)
    for c in weather_cols:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None))
    parts.append(df.select([F.col(c).cast(StringType()) for c in weather_cols]))


weather = parts[0]
for df in parts[1:]:
    weather = weather.unionByName(df)

weather.write.mode("overwrite").parquet(os.path.join(OUT, "weather"))
print("weather data done:", spark.read.parquet(os.path.join(OUT, "weather")).count(), "rows")


# airport to station mapping
station_schema = StructType([
    StructField("iata", StringType()), StructField("icao", StringType()),
    StructField("station", StringType()),
])
stations = (spark.read.option("header", True).schema(station_schema)
            .csv(os.path.join(RAW, "noaa", "stations.csv")))
stations.write.mode("overwrite").parquet(os.path.join(OUT, "stations"))
print("stations done:", stations.count(), "rows")

spark.stop()
