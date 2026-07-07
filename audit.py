from datetime import datetime, timezone

from database import SessionLocal
from models import ETLMetadata

# ---------------------------------------------------------------------------
# Task – Read Last Run Timestamp
# ---------------------------------------------------------------------------


def create_run():

    session = SessionLocal()

    try:

        metadata = ETLMetadata(
            pipeline_name="devto_etl",
            run_start_time=datetime.now(
                timezone.utc
            ),
            status="RUNNING"
        )

        session.add(metadata)

        session.commit()

        session.refresh(metadata)

        return metadata.id

    finally:

        session.close()


def get_latest_watermark():

    session = SessionLocal()

    try:

        metadata = (
            session.query(ETLMetadata)
            .filter(
                ETLMetadata.pipeline_name ==
                "devto_etl",

                ETLMetadata.status ==
                "SUCCESS"
            )
            .order_by(
                ETLMetadata.id.desc()
            )
            .first()
        )

        if metadata:

            return metadata.watermark

        return None

    finally:

        session.close()


# ==================================================
# Success Audit Update
# ==================================================

def update_run_success(
    run_id,
    records_extracted,
    records_loaded,
    watermark
):

    session = SessionLocal()

    try:

        run = session.get(
            ETLMetadata,
            run_id
        )

        run.status = "SUCCESS"

        run.run_end_time = datetime.now(
            timezone.utc
        )

        run.records_extracted = (records_extracted)

        run.records_loaded = (records_loaded)

        run.watermark = watermark

        session.commit()

    finally:
        session.close()


# ==================================================
# Failure Audit Update
# ==================================================

def update_run_failure(
    run_id,
    error_message
):

    session = SessionLocal()

    try:

        run = session.get(
            ETLMetadata,
            run_id
        )

        run.status = "FAILED"

        run.run_end_time = datetime.now(
            timezone.utc
        )

        run.error_message = error_message

        session.commit()

    finally:
        session.close()



