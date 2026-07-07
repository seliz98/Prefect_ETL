from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
import pandas as pd

# from database import SessionLocal
from models import Articles,ETLMetadata
from database import SessionLocal
from datetime import datetime,timezone
from prefect import flow,task,get_run_logger
from audit import (
    create_run,
    get_latest_watermark,
    update_run_success,
    update_run_failure)

# ---------------------------------------------------------------------------
# Extract – fetch a single page of articles
# ---------------------------------------------------------------------------


@task(retries=3, retry_delay_seconds=[2, 5, 15])
def fetch_page(page: int, api_base: str, per_page: int) -> list[dict[str, Any]]:
    """Return a list of article dicts for a given page number."""
    logger = get_run_logger()
    url = f"{api_base}/articles"
    params = {"page": page, "per_page": per_page}
    logger.info(f"Fetching page {page}")
    response = httpx.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


# ---------------------------------------------------------------------------
# Transform – convert list[dict] ➜ pandas DataFrame
# ---------------------------------------------------------------------------


@task
def to_dataframe(raw_articles: list[list[dict[str, Any]]]) -> pd.DataFrame:
    """Flatten & normalise JSON into a tidy DataFrame."""
    # Combine pages, then select fields we care about
    records = [article for page in raw_articles for article in page]
    
    if not records:
            return pd.DataFrame()

    df = pd.json_normalize(records)[
        [
            "id",
            "title",
            "published_at",
            "url",
            "comments_count",
            "positive_reactions_count",
            "tag_list",
            "user.username",
        ]
    ]
    df["published_at"] = pd.to_datetime(df["published_at"],utc=True)
    return df


# ---------------------------------------------------------------------------
# Load – save DataFrame to postgress db
# ---------------------------------------------------------------------------


@task
def load_to_postgres(df):

    logger = get_run_logger()
    session = SessionLocal()
    try:

        df = df.drop_duplicates(subset=["id"])

        for _, row in df.iterrows():

            article = Articles(
                id=row["id"],
                title=row["title"],
                published_at=row["published_at"],
                url=row["url"],
                comments_count=row["comments_count"],
                positive_reactions_count=row["positive_reactions_count"],
                tag_list=",".join(row["tag_list"] or []),
                username=row["user.username"]
            )

            session.merge(article)

        session.commit()

        logger.info(f"Loaded {len(df)} rows into PostgreSQL")


    except Exception as e:
        session.rollback()
        raise e

    finally:
        session.close()


# ---------------------------------------------------------------------------
# Flow – orchestrate the ETL with optional concurrency
# ---------------------------------------------------------------------------



@flow(name="devto_etl", log_prints=True)
def etl(api_base: str, pages: int, per_page: int) -> None:
    """Run the end-to-end ETL for *pages* of articles."""
    logger = get_run_logger()
    run_id = create_run()
    try:

        # Extract – simple loop for clarity
        raw_pages: list[list[dict[str, Any]]] = []
        for page_number in range(1, pages + 1):
            raw_pages.append(fetch_page(page_number, api_base, per_page))
        
        # ---------------------
        # Transform
        # --------------------
        df = to_dataframe(raw_pages)
        records_extracted = len(df)
        logger.info(f"Fetched {records_extracted} records from API")

        # ---------------------
        # Incremental Logic
        # ---------------------

        
        watermark = (get_latest_watermark())


        if watermark:

            logger.info(f"Filtering records newer than {watermark}")

            df = df[df["published_at"] > watermark]
            
            logger.info(f"Records remaining after filter: {len(df)}")


        if not df.empty:

            new_watermark = df["published_at"].max()

            load_to_postgres(df)

            
            update_run_success(
                        run_id=run_id,
                        records_extracted=records_extracted,
                        records_loaded=len(df),
                        watermark=new_watermark
                    )


            logger.info("ETL completed successfully")

        else:

            
            update_run_success(
                            run_id=run_id,
                            records_extracted=records_extracted,
                            records_loaded=0,
                            watermark=watermark
                        )

            logger.info("No new records found.")
            return
        
    except Exception as e:

            update_run_failure(
                run_id,
                str(e)
            )

            raise




# ## Run it!
#
# ```bash
# python 01_getting_started/03_run_api_sourced_etl.py
# ```

if __name__ == "__main__":
    # Configuration – tweak to taste
    api_base = "https://dev.to/api"
    pages = 3  # Number of pages to fetch
    per_page = 30  # Articles per page (max 30 per API docs)
    # output_file = Path("devto_articles.csv")

    etl(api_base=api_base, pages=pages, per_page=per_page)