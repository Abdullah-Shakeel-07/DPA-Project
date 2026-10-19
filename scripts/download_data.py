import csv
import os
import time
import socket
import urllib.request
import zipfile
from collections import Counter

RAW = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
YEARS = [2023, 2024]
NB_AIRPORTS = 50

# downloading the data
AIRPORTS_URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"
BTS_URL = "https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{}_{}.zip"
STATIONS_URL = "https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv"
WEATHER_URL = "https://noaa-global-hourly-pds.s3.amazonaws.com/{}/{}.csv"

opener = urllib.request.build_opener()
opener.addheaders = [("User-Agent", "Mozilla/5.0")]
urllib.request.install_opener(opener)
socket.setdefaulttimeout(120)   # stop waiting if the server does not answer for 2 minutes


# prints how many MB are downloaded, every 1 MB
def show_progress(blocks, block_size, total_size):
    done = blocks * block_size
    if done % (1024 * 1024) < block_size:
        if total_size > 0:
            print("", done // (1024 * 1024), "MB of", total_size // (1024 * 1024), "MB")
        else:
            print("", done // (1024 * 1024), "MB")


def download(url, path):
    if os.path.exists(path):
        print("already downloaded:", os.path.basename(path))
        return True
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # the servers sometimes fail, so we try 3 times before giving up
    for attempt in range(3):
        try:
            print("downloading:", os.path.basename(path))
            urllib.request.urlretrieve(url, path + ".tmp", show_progress)
            os.rename(path + ".tmp", path)
            print("downloaded:", os.path.basename(path))
            return True
        except Exception as e:
            print("error:", url, e, "- trying again")
            time.sleep(10)
    print("could not download:", url)
    return False


# airport data
download(AIRPORTS_URL, os.path.join(RAW, "airports.csv"))


# flights data and keepojg the one zip file for each month
bts_dir = os.path.join(RAW, "bts")
file_number = 0
for year in YEARS:
    for month in range(1, 13):
        file_number += 1
        print(f"flight file {file_number} of 24 ({year}-{month:02d})")
        csv_file = os.path.join(bts_dir, f"flights_{year}_{month:02d}.csv")
        if os.path.exists(csv_file):
            print("already downloaded:", os.path.basename(csv_file))
            continue
        zip_file = csv_file.replace(".csv", ".zip")
        if download(BTS_URL.format(year, month), zip_file):
            with zipfile.ZipFile(zip_file) as z:
                name = [n for n in z.namelist() if n.endswith(".csv")][0]
                z.extract(name, bts_dir)
            os.rename(os.path.join(bts_dir, name), csv_file)
            os.remove(zip_file)
            print("unzipped:", os.path.basename(csv_file))


# weather data
print("NOAA weather")

# finding the busiest airports by counting departures in the flight files
# this reads all the flight files so it takes a few minutes
counts = Counter()
for f in sorted(os.listdir(bts_dir)):
    if f.endswith(".csv"):
        print("counting flights in", f)
        with open(os.path.join(bts_dir, f), encoding="latin-1") as file:
            for row in csv.DictReader(file):
                counts[row["Origin"]] += 1
top_airports = [code for code, n in counts.most_common(NB_AIRPORTS)]
print("busiest airports:", top_airports)

# getting the airport code from OurAirports data
icao = {}
with open(os.path.join(RAW, "airports.csv"), encoding="utf-8") as file:
    for row in csv.DictReader(file):
        if row["iso_country"] == "US" and row["iata_code"] and row["type"] != "closed":
            icao[row["iata_code"]] = row["icao_code"] or row["gps_code"]

# getting the station id from the NOAA station list and keeping only those station that are working for 2023-2024
stations_file = os.path.join(RAW, "noaa", "isd-history.csv")
download(STATIONS_URL, stations_file)
station_of = {}
with open(stations_file, encoding="latin-1") as file:
    for row in csv.DictReader(file):
        if row["ICAO"] and row["BEGIN"] <= "20230101" and row["END"] >= "20241231":
            station_of[row["ICAO"]] = row["USAF"] + row["WBAN"]

with open(os.path.join(RAW, "noaa", "stations.csv"), "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["iata", "icao", "station"])
    for airport in top_airports:
        code = icao.get(airport)
        if code in station_of:
            print("weather for", airport)
            writer.writerow([airport, code, station_of[code]])
            for year in YEARS:
                download(WEATHER_URL.format(year, station_of[code]),
                         os.path.join(RAW, "noaa", str(year), station_of[code] + ".csv"))
        else:
            print("no weather station found for", airport)

print("downloading is done")
