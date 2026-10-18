# read the rew csv files and convert them to the parquet files
import glob
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
RAW = os.path.join(ROOT, "data", "raw")
OUT = os.path.join(ROOT, "data", "parquet")

spark = (SparkSession.builder.appName("dpa-ingest").config("spark.driver.memory", "2g").getOrCreate())
spark.sparkContext.setLogLevel("WARN")


# for flights
# the files have 110 columns but we only needs these ones
flight_types = {
    "Year": "int", "Month": "int", "FlightDate": "string", "DayOfWeek": "int",
    "Reporting_Airline": "string", "Tail_Number": "string", "Flight_Number_Reporting_Airline": "int",
    "Origin": "string", "Dest": "string", "CRSDepTime": "string", "CRSArrTime": "string",
    "DepDelay": "double", "ArrDelay": "double", "ArrDel15": "double",
    "Cancelled": "double", "Diverted": "double", "AirTime": "double", "Distance": "double",
    "CarrierDelay": "double", "WeatherDelay": "double", "NASDelay": "double", "LateAircraftDelay": "double",
}

flight_files = glob.glob(os.path.join(RAW, "bts", "*.csv"))
print("flight files found:", len(flight_files))

flights = spark.read.option("header", True).option("escape", '"').csv(flight_files)
flights = flights.select([F.col(c).cast(t) for c, t in flight_types.items()])

flights.write.mode("overwrite").partitionBy("Year", "Month").parquet(os.path.join(OUT, "flights"))
print("flights data done:", spark.read.parquet(os.path.join(OUT, "flights")).count(), "rows")


# airports data
airport_types = {
    "type": "string", "name": "string", "iso_country": "string", "iso_region": "string",
    "municipality": "string", "icao_code": "string", "iata_code": "string",
    "latitude_deg": "double", "longitude_deg": "double", "elevation_ft": "int",
}

airports = spark.read.option("header", True).option("escape", '"').csv(os.path.join(RAW, "airports.csv"))
airports = airports.select([F.col(c).cast(t) for c, t in airport_types.items()])

airports.write.mode("overwrite").parquet(os.path.join(OUT, "airports"))
print("airports data done:", airports.count(), "rows")


# weather data
# each station file has a different set of columns so we read them one by one
# and only keep the columns we need
weather_cols = ["STATION", "DATE", "REPORT_TYPE", "WND", "CIG", "VIS", "TMP", "DEW", "AA1"]

weather = None
for path in glob.glob(os.path.join(RAW, "noaa", "20*", "*.csv")):
    df = spark.read.option("header", True).option("escape", '"').csv(path)
    for c in weather_cols:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("string"))
    df = df.select(weather_cols)
    weather = df if weather is None else weather.union(df)

weather.write.mode("overwrite").parquet(os.path.join(OUT, "weather"))
print("weather data done:", spark.read.parquet(os.path.join(OUT, "weather")).count(), "rows")


# airport to station mapping
stations = spark.read.option("header", True).csv(os.path.join(RAW, "noaa", "stations.csv"))
stations.write.mode("overwrite").parquet(os.path.join(OUT, "stations"))
print("stations done:", stations.count(), "rows")

spark.stop()
