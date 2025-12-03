import logging
from pathlib import Path

import httpx
from fastapi import HTTPException

BASE_DIR = Path(__file__).resolve().parent.parent
FEED_FILES_DIR = BASE_DIR / "feed_files"
FEED_FILES_DIR.mkdir(exist_ok=True)

async def download_file_from_s3(
        download_data: list,
) -> str:
    try:
        if not download_data:
            logging.warning("No download data provided.")
            raise HTTPException(
                status_code=400,
                detail="No download data available."
            )

        download_url = download_data[0].get("download_url")
        file_name = download_data[0].get("file_name")

        if not download_url or not file_name:
            logging.warning("Download URL (or) Filename is missing in the provided data.")
            raise HTTPException(
                status_code=400,
                detail="Download URL (or) Filename is missing."
            )

        logging.info("File download from S3 initiated...")
        logging.info(f"Downloading from URL: {download_url}")
        logging.info(f"File name: {file_name}")

        target_path = FEED_FILES_DIR / file_name

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

        logging.info(f"File downloaded successfully and saved to {target_path}")
        return str(target_path)

    except HTTPException as e:
        raise e

    except Exception as e:
        logging.error(f"An error occurred while downloading the file: {e}")
        raise HTTPException(
            status_code=500,
            detail="An error occurred while downloading the file."
        )