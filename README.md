# DPA Project: US domestic air traffic

Part 1: Volume

Group: Abdullah Shakee, Salouma

## Question

Which US airports and airlines matter most and how much longer are delays in bad weather?

We look at:
1. flights per airline
2. busiest airports
3. plane turnarounds per airport (same plane landing and leaving again) and whether a late plane makes the next flight late
4. delays in bad weather vs good weather

## Data

| Dataset | What it is | Source |
|---|---|---|
| BTS On-Time Performance | every US domestic flight( 2023-2024) | https://www.transtats.bts.gov |
| OurAirports | list of airports (codes, names, states) | https://ourairports.com/data/ |
| NOAA hourly weather (ISD) | hourly weather of the 50 busiest airports (2023-2024) | https://www.ncei.noaa.gov |

The data is not in the repo because it is too big. `download_data.py` downloads it into `data/raw/`.

## Project structure

```
Dockerfile, compose.yaml    Spark + Jupyter in Docker
scripts/download_data.py    downloads the raw data into data/raw/
scripts/ingest.py           reads the raw csv files and saves them as parquet in data/parquet/
notebooks/data_cleaning.ipynb   cleans the 3 datasets -> data/clean/
notebooks/data_join.ipynb       joins flights with the weather of their departure hour -> data/joined/
notebooks/analysis.ipynb        the analysis, tables and charts -> results/
results/                    csv tables and png charts
```

## How to run

Need **Docker** (Docker Desktop on Windows/Mac) and **git**.

1. Clone the repo and start the container:
   ```
   git clone https://github.com/Abdullah-Shakeel-07/DPA-Project.git
   cd DPA-Project
   docker compose up -d --build
   ```
2. Download the data (this takes a while becasue the flight files are big):
   ```
   docker compose exec notebook python project/scripts/download_data.py
   ```
3. Convert the csv files to parquet:
   ```
   docker compose exec notebook python project/scripts/ingest.py
   ```
4. Open http://localhost:8889 in the browser, go to `project/notebooks/` and run the notebooks in this order (Run All):
   1. `data_cleaning.ipynb`
   2. `data_join.ipynb`
   3. `analysis.ipynb`
5. Stop the container when you are done:
   ```
   docker compose down
   ```

The results (csv files and charts) are saved in `results/`.


## Main steps

- **Ingest:** we keep only the columns we need and save everything as parquet (much smaller and faster than csv).
- **Cleaning:** remove duplicate flights, cancelled and diverted flights, flights with missing delays or tail number, and impossible values. Keep only open US airports with an IATA code. Keep only the hourly weather reports (FM-15), turn the weather text fields into numbers, and mark bad weather (visibility < 4828 m, clouds < 305 m, wind >= 10.3 m/s or rain >= 2.5 mm).
- **Join:** flight times are local time and weather times are UTC, so we convert the departure hour to UTC using the timezone of the airport's state, then join each flight with the weather of its departure hour.
- **Analysis:** Spark SQL queries for each question and results saved as csv and charts.

## Row counts

| Step | Rows |
|---|---|
| flights after cleaning | 13,708,650 (98.4% of raw) |
| US airports after cleaning | 2,036 |
| hourly weather rows | 857,812 |
| flights matched with weather (49 airports) | 10,734,566 |

## Main results

- Southwest has the most flights (20.6%), the top 4 airlines have about 60% of all flights. Frontier and JetBlue are late most often (30% and 28.5% arrive 15+ min late).
- Atlanta (ATL) is the busiest airport (1.34M flights) and has the most turnarounds.
- When a plane arrives late its next flight leaves about 45 min late on average vs about 4 min when it arrives on time.
- In bad weather the average departure delay is 20.3 min vs 12.2 min in good weather. Heavy rain is the worst (45.6 min). Florida airports are hit the most (Miami +29 min) and the West Coast barely (Seattle +1 min).
