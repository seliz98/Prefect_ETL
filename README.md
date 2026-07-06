Dev.to Articles ETL Pipeline Documentation
Project Overview
This project is a complete ETL (Extract, Transform, Load) pipeline built using:

Python
Prefect
PostgreSQL
SQLAlchemy
Pandas
HTTPX

The pipeline extracts article data from the Dev.to public API, transforms the JSON response into a structured format, and loads the data into PostgreSQL. It also implements incremental loading using a watermark mechanism to avoid reprocessing previously loaded records.
project/
│
├── database.py
├── models.py
├── create_tables.py
└── etlflow.py
