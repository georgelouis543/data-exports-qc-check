from pydantic import BaseModel, Field


class QcCheckRequest(BaseModel):
    metadata_feed_id: int = Field(
        ...,
        description="The ID of the metadata feed to be checked."
    )
    raw_download_data: str = Field(
        ...,
        description="The raw download-data for quality check."
    )

class QcCheckResponse(BaseModel):
    message: str
    feed_id: int
    task_id: int