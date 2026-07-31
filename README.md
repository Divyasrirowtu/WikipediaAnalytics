Project Title
# Wikipedia Analytics Pipeline
Project Description
## Project Description

This project processes Wikipedia XML edit history using Apache Spark.

The pipeline:

- Reads Wikipedia XML
- Flattens nested XML
- Performs three analytical computations
- Stores results as partitioned Parquet files
- Runs completely using Docker Compose
Technologies
## Technologies

- Python 3.11
- Apache Spark (PySpark)
- Docker
- Docker Compose
- spark-xml
Folder Structure
## Folder Structure

```text
WikipediaAnalytics/

app/
data/
logs/
output/

Dockerfile
docker-compose.yml
requirements.txt
README.md
run.sh
.env.example
```
Pipeline Flow
## Pipeline Flow

Wikipedia XML

↓

Read XML using spark-xml

↓

Transform nested structure

↓

Analysis 1

↓

Analysis 2

↓

Analysis 3

↓

Partitioned Parquet Output
Analyses
## Analyses

### 1 Edit Volume Per Article Per Hour

Calculates hourly edit counts.

### 2 Revert Rate Per Editor

Calculates revert percentage.

### 3 Top 100 Most Contested Articles

Ranks articles based on unique editors.
Output
## Output

The pipeline generates

- edit_volume.parquet
- revert_rate.parquet
- contested_articles.parquet

Each output is partitioned by date.
How to Run
## How to Run

```bash
docker compose up --build
```
Expected Output
## Expected Output

output/

edit_volume.parquet/

revert_rate.parquet/

contested_articles.parquet/