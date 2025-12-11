import logging
import os
import re
import shutil
import zipfile
from pathlib import Path

from app.utils.qc_check.regex_patterns import date_regex_for_filenames


def extract_feed_zip(
        task_id: str,
        zip_file_path: str):
    """
    Extracts zip + normal files into: feed_files/<task_id>/extracted/
    Returns the extraction path.
    """

    try:
        task_root = Path(zip_file_path).parent
        extract_to = task_root / "extracted"
        extract_to.mkdir(parents=True, exist_ok=True)

        zip_file_path = Path(zip_file_path)
        logging.info(f"[{task_id}] Extracting {zip_file_path} into {extract_to}")

        # Extract Zip if it exists
        if zip_file_path.suffix.lower() == ".zip":
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
            logging.info(f"[{task_id}] Extracted {zip_file_path} into {extract_to}")
        else:
            logging.warning(f"[{task_id}] No Zip file found to extract")
            raise FileNotFoundError("No Zip file found to extract")

        # Just copy non-zip files to the extracted folder
        for item in task_root.iterdir():
            if (
                item.is_file()
                and item.suffix.lower() != ".zip"
                and not item.name.endswith(":Zone.Identifier")
                and item.name != "extracted"
            ):
                shutil.copy(item, extract_to / item.name)
                logging.info(f"[{task_id}] Copied {item.name} to extracted/")

        # Detect single folder inside zip
        items = os.listdir(extract_to)
        if len(items) == 1:
            child = extract_to / items[0]
            if child.is_dir(): # This is to ensure we only adjust path if it's a directory
                extracted_path = str(child)
            else:
                extracted_path = str(extract_to)
        else:
            extracted_path = str(extract_to)

        # Extract date from filenames
        date_regex = date_regex_for_filenames("extract_zipped_folder")
        date_pattern = re.compile(date_regex)
        detected_date = None

        for name in os.listdir(extracted_path):
            match = date_pattern.search(name)
            if match:
                detected_date = match.group(1)
                logging.info(f"[{task_id}] Detected date {detected_date} in filename {name}")
                break

        if not detected_date:
            logging.info(f"[{task_id}] No date suffix found in filenames.")

        return str(extracted_path)

    except Exception as e:
        logging.error(f"[{task_id}] Error during extraction: {e}")
        raise e