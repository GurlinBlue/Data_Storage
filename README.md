Rotten Tomatoes Movie Metadata Pipeline

Course: Data Collection & Preparation
Project: End-to-End Data Pipeline — Dynamic Website → SQLite → Airflow Automation

1. Project Overview

This project implements a complete automated data pipeline that extracts movie metadata from Rotten Tomatoes, a JavaScript-rendered website requiring full browser execution. The system uses:

Playwright for dynamic web scraping

Python for preprocessing and cleaning

SQLite for persistent storage

Apache Airflow for workflow orchestration and scheduling

Logging, retries, and structured DAG design

The pipeline runs scrape → clean → load, producing a fully processed dataset of movies with metadata such as scores, genres, duration, director, release date, and more.

This submission fulfills all requirements from the assignment prompt, including scraping dynamic content, cleaning, storage, and Airflow automation 

SIS2_Assignment

.

2. Website Description

Website: https://www.rottentomatoes.com/browse/movies_at_home

Why it qualifies:
Rotten Tomatoes uses dynamic JavaScript components, browser-rendered React elements, and lazy-loaded metadata. None of the required data (scores, movie lists) is available in plain HTML. Therefore, a real browser engine is required, and the project uses Playwright Chromium in headless mode for correct extraction.

Movies are displayed across multiple paginated pages (0–7). Each page loads movie tiles dynamically.

3. Pipeline Architecture
Airflow DAG
     │
     ├── Task 1: scrape_movie_list
     │         • Playwright scrapes 8 dynamic pages
     │         • Outputs: movies_raw.csv (title, url)
     │
     ├── Task 2: scrape_movie_details
     │         • Playwright opens each movie page
     │         • Extracts metadata: scores, genre, duration, rating, director, etc.
     │         • Cleans & normalizes text
     │         • Outputs: movies_clean.csv
     │
     └── Task 3: load_to_sqlite
               • Writes cleaned data into movies.db
               • Creates indexes for faster queries

4. Technology Stack
Component	Tool
Dynamic scraping	Playwright (Chromium)
Data processing	Python + Pandas
Storage	SQLite3
Scheduler	Apache Airflow
Logging	Python logging
Concurrency	asyncio + Playwright coroutines
5. Data Collection (Scraper)
Tool Used: Playwright (not Selenium)

This pipeline relies exclusively on Playwright for:

Rendering JavaScript

Waiting for dynamic UI components (scorecards)

Extracting structured elements using CSS selectors

Navigating paginated lists

Opening and parsing individual movie pages

Data Extracted

From list pages:

Title

URL

From individual movie pages:

Tomatometer score

Audience score

Genre

Rating (MPAA)

Duration

Release date

Director

Original language

Box office

Distributor

All scraping is asynchronous and uses a semaphore-controlled batch system for stability and speed.

6. Data Cleaning & Preprocessing

The cleaning module performs several steps required by the assignment:

✔ Removing duplicates

URL deduplication and title normalization with regex.

✔ Handling missing values

Missing fields remain None and are later converted to SQLite NULL.

✔ Normalizing text fields

Lowercasing titles

Removing extra whitespace

Removing punctuation

Standardizing duration format (1h 42m)

✔ Type casting

Scores → integers

Invalid scores removed

CSV output encoded UTF-8

✔ Validation rules

Scores must be 0–100

Duration must match h/m format

Empty metadata is logged

All cleaned results are stored in:

movies_clean.csv

7. Database Layer (SQLite)
Database file:
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
Indexes Created
idx_tomatometer ON movies(tomatometer_score)
idx_audience ON movies(audience_score)
idx_genre ON movies(genre)
idx_rating ON movies(rating)


These significantly improve filtering performance during the demo.

8. Airflow Automation
DAG ID: scraper
Schedule: @daily (meets “no more than once every 24h” requirement) 

SIS2_Assignment

Tasks:

scrape_movie_list

Scrapes all 8 Rotten Tomatoes “movies at home” pages

Outputs movies_raw.csv

scrape_movie_details

Scrapes metadata for each movie

Cleans and validates fields

Saves movies_clean.csv

load_to_sqlite

Inserts cleaned data into movies.db

Creates indexes

Verifies row count

Resilience Features

Retries (2 times)

Retry delay (5 minutes)

Execution timeout (90 minutes)

Logging for every step

Progress statistics printed to logs

9. How to Run the Project
1) Install dependencies
pip install -r requirements.txt
playwright install chromium

2) Start Airflow
airflow db init
airflow webserver -p 8080
airflow scheduler

3) Enable the DAG

Open Airflow UI → Turn on scraper.

4) Confirm successful run

In Airflow logs you should see:

Movie list scraped

Metadata extracted

CSV files generated

SQLite table created

Rows inserted successfully

10. Expected Output
Files created:
movies_raw.csv
movies_clean.csv
movies.db

Example cleaned entry:
Field	Example
title	Zootopia
tomatometer_score	98
audience_score	92
genre	Animation, Comedy
duration	1h 48m
director	Byron Howard, Rich Moore

11. Project Structure
AIRFLOW/
│
├── dags/
│   └── project.py                 # Airflow DAG with scraping, cleaning, loading
│
├── data/
│   ├── movies_raw.csv             # Output of Task 1 (list scrape)
│   ├── movies_clean.csv           # Output of Task 2 (metadata scrape + cleaning)
│   └── movies.db                  # SQLite database generated by Task 3
│
├── logs/                          # Airflow-generated logs (scheduler, tasks)
│
├── plugins/                       # Empty (reserved for Airflow plugins)
│
├── config/                        
│
├── .env                           # Environment variables
│
├── docker-compose.yaml            # Airflow Compose environment
│
├── Dockerfile                     # Dockerfile for Airflow worker/webserver
│
└── requirements.txt               # Python dependencies (Playwright, Pandas, etc.)
