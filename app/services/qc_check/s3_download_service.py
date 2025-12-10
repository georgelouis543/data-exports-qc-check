import logging
from pathlib import Path
from typing import Any

import httpx

BASE_DIR = Path(__file__).resolve().parent.parent
FEED_FILES_DIR = BASE_DIR / "feed_files"
FEED_FILES_DIR.mkdir(exist_ok=True)

async def download_file_from_s3(
        download_data: list[dict[str, Any]],
        task_id: str
) -> str:
    try:
        if not download_data or not isinstance(download_data, list):
            logging.error(f"[{task_id}] Download data is not a valid list.")
            raise Exception(f"[{task_id}] Download data is not a valid list.")

        download_url = download_data[0].get("download_url")
        file_name = download_data[0].get("file_name")

        if not download_url or not file_name:
            logging.warning("Download URL (or) Filename is missing in the provided data.")
            raise Exception("Download URL (or) Filename is missing.")

        logging.info(f"[{task_id}] File download from S3 initiated")
        logging.info(f"[{task_id}] Downloading from URL: {download_url}")
        logging.info(f"[{task_id}] File name: {file_name}")

        # Create folder for this task
        task_folder = FEED_FILES_DIR / task_id
        task_folder.mkdir(parents=True, exist_ok=True)

        target_path = task_folder / file_name

        async with httpx.AsyncClient(timeout=None) as client:
            async with client.stream(
                    "GET",
                    download_url
            ) as response:
                response.raise_for_status()

            # Stream data in chunks to avoid memory issues with large files
            with open(target_path, "wb") as file:
                async for chunk in response.aiter_bytes(chunk_size=1024 * 1024):
                    file.write(chunk)

        logging.info(f"[{task_id}] File downloaded successfully and saved to {target_path}")
        return str(target_path)

    except Exception as e:
        logging.error(f"[{task_id}] An error occurred while downloading the file: {e}")
        raise e