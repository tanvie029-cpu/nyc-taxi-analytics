# NYC Taxi Trip Data Analysis using PySpark

A Big Data Analytics mini project that analyzes NYC Yellow Taxi trip data using Apache Spark and PySpark.

## 📌 Project Overview

This project analyzes a large-scale NYC Yellow Taxi dataset containing millions of taxi trip records.

The project demonstrates how PySpark can be used to load, clean, transform, aggregate, and analyze large datasets efficiently.

The analysis focuses on taxi demand patterns, pickup locations, trip characteristics, and the relationship between trip distance and fare.

## 🎯 Objectives

- Process large-scale taxi trip data using PySpark.
- Clean and prepare the dataset for analysis.
- Analyze taxi demand by pickup hour.
- Analyze taxi demand by day of the week.
- Calculate average trip distance and fare by hour.
- Identify the most popular pickup zones.
- Analyze average fare across different trip-distance groups.
- Visualize the results using Matplotlib.

## 🛠️ Technologies Used

- Python
- Apache Spark
- PySpark
- Matplotlib
- VS Code
- Parquet
- CSV

## 📂 Project Structure

```text
NYC Taxi Analysis BDA Mini Project/
│
├── data/
│   ├── taxi_zone_lookup.csv
│
├── outputs/
│   ├── average_fare_by_distance.png
│   ├── top_pickup_zones.png
│   ├── trips_by_day.png
│   └── trips_by_hour.png
│
├── src/
│   └── taxi_analysis.py
│
├── README.md
├── .gitignore
└── spark_test.py
```
The large NYC Taxi Parquet dataset is excluded from the GitHub repository using .gitignore.

## 🔎 Analysis Performed
1. Taxi Trips by Pickup Hour
2. Taxi Trips by Day of the Week
3. Average Trip Distance and Fare by Hour
4. Top 10 Pickup Zones
5. Average Fare by Distance Group

## 📊 Outputs
The project generates four visualizations:
- Taxi Trips by Pickup Hour
- Taxi Trips by Day of the Week
- Top 10 Pickup Zones
- Average Fare by Distance Group
The generated graphs are available in the outputs/ directory.

## 📁 Dataset
The project uses the NYC Yellow Taxi Trip Record Data for January 2025 provided by the New York City Taxi & Limousine Commission (TLC).
The dataset contains information such as:
- Pickup and drop-off timestamps
- Passenger count
- Trip distance
- Pickup and drop-off locations
- Fare amount
- Payment information
A taxi zone lookup table is used to convert location IDs into readable zone names.

## 🧹 Data Cleaning
The dataset was cleaned using PySpark by:
- Removing invalid passenger counts.
- Removing invalid trip distances.
- Removing invalid fare values.
- Removing records with missing values in important columns.
- Removing extreme trip-distance and fare values using defined thresholds.

## 🚀 How to Run
1. Clone the Repository
git clone <your-github-repository-url>
cd "NYC Taxi Analysis BDA Mini Project"

2. Create a Virtual Environment (Optional)
python -m venv venv

Activate it on Windows:
venv\Scripts\activate

3. Install Dependencies
pip install -r requirements.txt

4. Add the Dataset
Download the NYC Yellow Taxi January 2025 dataset from the official NYC TLC Trip Record Data source.
Place the following files inside the data/ directory:
data/
├── yellow_tripdata_2025-01.parquet
└── taxi_zone_lookup.csv

The large Parquet dataset is intentionally excluded from GitHub through .gitignore.

5. Run the Project
From the project root directory:
python src/taxi_analysis.py

The analysis results will be displayed in the terminal.
The generated visualizations will be saved in:
outputs/

## 📈 Key Findings
- 5 PM recorded the highest number of taxi trips.
- Thursday recorded the highest number of trips among the days analyzed.
- Upper East Side South was the most popular pickup zone.
- Average fare increased as the trip-distance group increased.

## 📚 References
- NYC Taxi & Limousine Commission (TLC) — Taxi Trip Record Data
- NYC TLC — Taxi Zone Lookup Table
- Apache Spark Documentation
- PySpark Documentation
- Matplotlib Documentation

## 👩‍💻 Author
Tanvi Kadam
BE Computer Engineering
Atharva College of Engineering