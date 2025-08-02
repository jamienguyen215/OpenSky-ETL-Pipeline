# OpenSky-ETL-Pipeline

This project demonstrates a complete Extract, Transform, Load (ETL) pipeline using Python, PostgreSQL, and Power BI. The goal is to fetch real-time flight data **from the OpenSky Network's REST API**, process it, and load it into a relational database for analysis and visualization.

## Project Overview

The ETL pipeline is designed to:
1.  **Extract**: Fetch real-time flight data from the OpenSky Network's REST API.
2.  **Transform**: Process the raw JSON data using Python and the `pandas` library. This includes data cleaning, filtering, and enriching the data by finding the nearest airport for grounded planes.
3.  **Load**: Store the transformed data into a PostgreSQL database.
4.  **Visualize**: Connect a Power BI dashboard to the PostgreSQL database to create interactive visualizations & Dashboard of the flight data.

The entire ETL process is automated using Windows Task Scheduler to run daily at 11 am.

## Files in this Repository

-   `OpenSky - ETL.py`: The core Python script that performs the ETL process. It uses `requests` to call the API, `pandas` for data manipulation, and `sqlalchemy` to connect to and load data into PostgreSQL.
-   `OpenSky - ETL.sql`: SQL queries used for data analysis and verification within the PostgreSQL database.
-   `OpenSky - ETL.ipynb`: The Jupyter Notebook version of the ETL script, useful for step-by-step development and documentation of the process.
-   `dashboards/OpenSky - ETL.pbix`: The Power BI file containing the data model, report, and visualizations.
-   `OpenSky-ETL-PowerBI-Dashboard-screenshot.png`: screenshot of the Power BI dashboard, providing a visual overview of the final output.
-   `requirements.txt`: A list of all Python libraries required to run the `OpenSky - ETL.py` script.

## Technical Stack

-   **Data Source**: [OpenSky Network REST API](https://opensky-network.org/api/states/all)
-   **Programming Language**: Python
-   **ETL Libraries**: `requests`, `pandas`, `geopy`, `sqlalchemy`, `psycopg2`
-   **Database**: PostgreSQL
-   **Business Intelligence**: Power BI
-   **Automation**: Windows Task Scheduler

## ETL Process Details

### 1. Data Extraction

The script makes an HTTP GET request to the OpenSky Network API to retrieve a snapshot of all flights currently in the air or on the ground.

```python
import requests
import json

url = "[https://opensky-network.org/api/states/all](https://opensky-network.org/api/states/all)"
response = requests.get(url)
data = response.json()

### 2. Data Transformation
The raw JSON data is converted into a pandas DataFrame. Columns are defined according to the OpenSky API documentation. The transformation steps include:

Filtering for planes that are on_ground.

Dropping records with missing latitude or longitude.

Enriching the data by finding the nearest_airport (within a 5-mile radius) using geopy.distance.geodesic.

### 3. Data Loading
The cleaned and transformed pandas DataFrame is then loaded into a PostgreSQL database table named OpenSkyApi. The if_exists='append' parameter ensures that new data is added with each ETL run.

Python

import sqlalchemy as db

engine = db.create_engine('postgresql://postgres:ABC@localhost:5432/OpenSkyApi')
df.to_sql("OpenSkyApi", engine, if_exists='append', index=False)

Note: The database connection string postgresql://postgres:ABC@localhost:5432/OpenSkyApi in the provided script should be replaced with appropriate credentials and host for a production environment.

## Data Analysis (SQL)
The OpenSky - ETL.sql file contains example queries used to analyze the loaded data.

Count of total records and grounded planes.

Count of unique airports identified.

Aggregation of flight counts by airport.

SQL

SELECT nearest_airport, COUNT(*) AS flight_count 
FROM "OpenSkyApi"
WHERE nearest_airport IS NOT NULL
GROUP BY nearest_airport
ORDER BY flight_count DESC;

## Dashboard 
The Power BI dashboard provides a visual summary of the data. The following screenshots show key metrics and visualizations, such as flight counts by airport.
![Power BI Dashboard](screenshots/OpenSky-ETL-PowerBI-Dashboard-screenshot.png)

## Project Limitations
Data Collection Schedule: The ETL pipeline is automated to run once a day, specifically at 11 am, to capture a daily snapshot. This project only collects data for a limited 7-day period, from August 1, 2025, to August 8, 2025.

API Data Nature: The states/all endpoint of the OpenSky Network API provides a snapshot of real-time data. It does not provide historical data for a full 24-hour period. Therefore, the daily run captures only the flights visible at that specific moment in time.


## How to Run the Project
### 1. Clone the repository:

Bash

git clone [https://github.com/](https://github.com/)[Your-Username]/OpenSky-ETL-Pipeline.git
cd OpenSky-ETL-Pipeline

### 2. Install dependencies:

Bash

pip install -r requirements.txt

### 3. Set up PostgreSQL:

Ensure you have a PostgreSQL server running.

Create a database (e.g., OpenSkyApi).

Update the connection string in OpenSky - ETL.py with your database credentials.

### 4. Execute the ETL script:

Bash

python "OpenSky - ETL.py"
### 5. View the Dashboard:

Open the dashboards/OpenSky - ETL.pbix file in Power BI Desktop.

Refresh the data to connect to your PostgreSQL database and see the latest data.
