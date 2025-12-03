import logging

from fastapi import HTTPException


async def download_file_from_s3(
        download_urls: list,
) -> None:
    download_urls = download_urls
    if not download_urls:
        logging.warning("No download URLs provided.")
        raise HTTPException(
            status_code=400,
            detail="No download URLs provided."
        )

    logging.info(f"Download URLS: {download_urls}")

    pass