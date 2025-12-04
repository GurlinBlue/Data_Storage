🎬 Rotten Tomatoes Movie Metadata Pipeline

Course: Data Collection & Preparation
Project: End-to-End Data Pipeline — Dynamic Website → Cleaning → SQLite → Airflow Automation

📌 1. Overview

This project implements a fully automated data pipeline that collects, cleans, and stores movie metadata from Rotten Tomatoes, a highly dynamic website rendered with JavaScript. The system integrates:

Playwright (Chromium) for dynamic scraping

Python + Pandas for preprocessing

SQLite for persistent storage

Apache Airflow for orchestration

asyncio for concurrency

Logging & retries for reliability

The workflow follows:

Scrape movie list → Scrape metadata → Clean → Load into SQLite → Automate with Airflow


All assignment requirements (dynamic scraping, cleaning, automation, DB storage) are fully met.

🌐 2. Website Description

Target URL:
https://www.rottentomatoes.com/browse/movies_at_home

Why this website qualifies:

Uses React components

Metadata and scorecards load dynamically

Data not present in raw HTML

Requires browser execution to access elements

Playwright Chromium is therefore used in headless mode to load and parse 8 paginated pages (?page=0..7).

🧱 3. Pipeline Architecture
```txt
Airflow DAG
│
├── Task 1: scrape_movie_list
│   • Scrapes 8 dynamic pages
│   • Extracts titles + URLs
│   • Saves movies_raw.csv
│
├── Task 2: scrape_movie_details
│   • Opens each movie page with Playwright
│   • Extracts all metadata fields
│   • Cleans + validates data
│   • Saves movies_clean.csv
│
└── Task 3: load_to_sqlite
    • Inserts cleaned data into SQLite
    • Creates indexes
    • Verifies successful load
```




🛠 4. Technology Stack
Component	Tool
Dynamic scraping	Playwright (Chromium)
Concurrency	asyncio + semaphores
Cleaning	Python, Pandas, regex
Storage	SQLite3
Scheduler	Apache Airflow
Deployment	Docker Compose
Logging	Python logging
🎥 5. Data Collection (Scraper)
Tool Used: Playwright

(Assignment requirement: dynamic JS scraping → fulfilled)

Playwright handles:

JavaScript execution

Interactive DOM elements

Lazy-loaded score components

Navigation through pages

Extracting structured data via CSS selectors

Extracted Data
From list pages:

Movie title

Detail page URL

From movie detail pages:

Tomatometer score

Audience score

Genre

Rating

Duration

Release date

Director

Original language

Box office

Distributor

All scraping is asynchronous and batched using semaphores for performance.

🧹 6. Data Cleaning & Normalization

The cleaning system performs:

✔ Duplicate Removal

URL normalization

Title normalization with regex

Removal of repeated titles or URLs

✔ Handling Missing Data

Missing values stay as None → become NULL in SQLite

✔ Text Normalization

Lowercasing

Whitespace cleanup

Punctuation removal

Duration converted to 1h 42m format

Release date cleanup

✔ Type Casting & Validation

Score fields must be 0–100

Invalid values removed

Columns cast to correct types

Output file:

movies_clean.csv

🗄 7. SQLite Database Layer

Database file location:
/opt/airflow/data/movies.db

Table Schema
Column	Type
title	TEXT
url	TEXT
tomatometer_score	INTEGER
audience_score	INTEGER
genre	TEXT
rating	TEXT
duration	TEXT
release_date	TEXT
director	TEXT
original_language	TEXT
box_office	TEXT
distributor	TEXT


🪂 8. Airflow Automation

DAG ID: scraper
Schedule: @daily (meets assignment requirement: no more than once every 24 hours)

Tasks
1. scrape_movie_list

Scrapes 8 dynamic pages

Saves movies_raw.csv

2. scrape_movie_details

Opens each movie page

Extracts + cleans metadata

Saves movies_clean.csv

3. load_to_sqlite

Loads cleaned data

Creates indexes

Confirms row counts

Resilience Features

2 retries

5 minute retry delay

90 minute timeout

Logging for every step

Progress metrics in Airflow logs

▶️ 9. Running the Project
Install dependencies
pip install -r requirements.txt
playwright install chromium

Start Airflow services
airflow db init
airflow webserver -p 8080
airflow scheduler

Enable the DAG

Airflow UI → turn on scraper

Expected Outputs

movies_raw.csv

movies_clean.csv

movies.db

Example Cleaned Row
Field	Example
title	Zootopia
tomatometer_score	98
audience_score	92
genre	Animation, Comedy
duration	1h 48m
director	Byron Howard, Rich Moore

📁 10. Project Structure
```txt

AIRFLOW/
│
├── dags/
│   └── project.py               # Airflow DAG: scraping + cleaning + loading
│
├── data/
│   ├── movies_raw.csv           # Output: scraped movie list (Task 1)
│   ├── movies_clean.csv         # Output: cleaned metadata (Task 2)
│   └── movies.db                # SQLite database (Task 3)
│
├── logs/                        # Airflow execution logs
│
├── plugins/                    
│
├── config/                     
│
├── .env                         # Environment variables (Playwright / Airflow)
│
├── docker-compose.yaml          # Airflow Docker Compose stack
│
├── Dockerfile                   # Custom Dockerfile (Playwright + dependencies)
│
└── requirements.txt             # Python dependencies (Playwright, Pandas, etc.)
```
