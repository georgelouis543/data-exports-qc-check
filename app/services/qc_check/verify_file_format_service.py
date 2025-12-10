import logging
import os
import zipfile
from typing import Any

from app.config.celery_db import get_sync_db


async def verify_file_format_using_metadata(
        feed_id: int,
        extracted_file_path: str,
        task_id: str
) -> dict[str, Any]:
    try:
        out_dict = {}

        with get_sync_db() as conn:
            cur = conn.cursor()
            cur.execute(
                f"SELECT file_compression "
                f"FROM dataexports_api.lkp_metadata_feed_list "
                f"WHERE metadata_feed_id = {feed_id};"
            )
            row = cur.fetchone()

            if not row:
                raise Exception(f"No metadata feed found for ID: {feed_id}")

        logging.info(f"[{task_id}] Retrieved {row} metadata record")

        archive_format = row[0].lower().lstrip('.') # e.g., "zip", "gz"
        logging.info(f"[{task_id}] Archive: {archive_format}")

        task_folder = task_id # task_folder is derived from task_id (so makes sense!)
        archive_files = [
            f for f in os.listdir(task_folder)
            if f.lower().endswith(f".{archive_format}")
        ]

        if not archive_files:
            logging.error(f"[{task_id}] No archive files found with format: {archive_format}")
            raise AssertionError(f"No archive files found with format: .{archive_format}")

        archive_file = archive_files[0]
        archive_path = os.path.join(task_folder, archive_file)
        logging.info(f"[{task_id}] Found archive file: {archive_path}")

        # Validate Zip Files now
        if archive_format == "zip":
            if not zipfile.is_zipfile(archive_path):
                raise AssertionError(f"The file {archive_file} is not a valid zip archive.")

        return out_dict

    except Exception as e:
        print(f"[{task_id}] Error retrieving metadata feed: {e}")
        return {}