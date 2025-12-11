import logging
import os
import re
import zipfile
from typing import Any

from app.config.celery_db import get_sync_db
from app.config.file_paths import FEED_FILES_DIR
from app.utils.qc_check.delimiter_set import fetch_all_delimiters, fetch_delimiter_map
from app.utils.qc_check.regex_patterns import date_regex_for_filenames


async def verify_file_format_using_metadata(
        feed_id: int,
        extracted_file_path: str,
        task_id: str
) -> dict[str, Any]:
    audit_step = {}

    try:
        issues = []
        validated_files = []

        with get_sync_db() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT file_compression
                FROM dataexports_api.lkp_metadata_feed_list
                WHERE metadata_feed_id = %s;
                """,
                (feed_id,)
            )
            row = cur.fetchone()

        if not row:
            logging.error(f"[{task_id}] No metadata feed found for ID: {feed_id}")
            raise Exception(f"No metadata feed found for ID: {feed_id}")

        logging.info(f"[{task_id}] Retrieved {row} metadata record")

        archive_format = row[0].lower().lstrip('.') # e.g., "zip", "gz"
        logging.info(f"[{task_id}] Archive: {archive_format}")

        task_folder = FEED_FILES_DIR / task_id # task_folder is named after task_id (so makes sense!)
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

        # Get all files' metadata from DB table 'lkp_metadata_files_included'
        with get_sync_db() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT file_name, file_extension, column_separator
                FROM dataexports_api.lkp_metadata_files_included
                WHERE metadata_feed_id = %s;
                """,
                (feed_id,)
            )
            files_metadata_list = cur.fetchone()

        if not files_metadata_list:
            logging.error(f"[{task_id}] No metadata found for files for feed ID: {feed_id}")
            raise AssertionError(f"No metadata found for files for feed ID: {feed_id}")

        root_path = extracted_file_path
        delimiter_map = fetch_delimiter_map()
        all_delimiters = fetch_all_delimiters()
        date_pattern = re.compile(date_regex_for_filenames("verify_file_format"))

        for file_name, file_extension, expected_delimiter in files_metadata_list:
            expected_full_file_name = f"{file_name}.{file_extension}"
            expected_symbol = delimiter_map.get(
                expected_delimiter.upper(),
                expected_delimiter
            )

            matched_file = None

            for f in os.listdir(root_path):
                name, extension = os.path.splitext(f)
                normalized_file_name = date_pattern.sub("", name) + extension

                if normalized_file_name.lower() == expected_full_file_name.lower():
                    matched_file = f
                    break

            if not matched_file:
                issues.append(f"Expected file not found: {expected_full_file_name}")
                continue

            file_path = os.path.join(root_path, matched_file)
            if not file_path.lower().endswith(file_extension.lower()):
                issues.append(f"File extension mismatch for {matched_file}: Expected .{file_extension}")
                continue

            # Check for Parquet files (just verify extension here)
            if file_extension.lower() == ".parquet":
                if not matched_file.lower().endswith(".parquet"):
                    issues.append(f"{matched_file}: Expected Parquet file extension '.parquet'")
                else:
                    validated_files.append(matched_file)
                continue

        if issues:
            # Audit step failed
            audit_step['step_name'] = 'FILE FORMAT VERIFICATION'
            audit_step['status'] = 'FAILED'
            audit_step['details'] = {
                'issues': issues[:10] # Limit to first 10 issues
            }
            return audit_step

        # Audit step passed
        audit_step['step_name'] = 'FILE FORMAT VERIFICATION'
        audit_step['status'] = 'PASSED'
        audit_step['details'] = {
            'validated_files': validated_files[:10] # Limit to first 10 validated files
        }

        return audit_step

    except AssertionError as e:
        raise e

    except Exception as e:
        audit_step['step_name'] = 'FILE FORMAT VERIFICATION'
        audit_step['status'] = 'ERROR'
        audit_step['details'] = {
            'error': str(e)
        }
        logging.error(f"[{task_id}] File format verification error: {e}")
        return audit_step