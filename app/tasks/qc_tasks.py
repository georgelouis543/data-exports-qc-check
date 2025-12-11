from app.config.celery_config import celery_app
from app.services.qc_check.control_file_service import check_control_file_exists, read_filenames_from_control_file
from app.services.qc_check.extract_zipped_folder_service import extract_feed_zip
from app.services.qc_check.s3_download_service import download_file_from_s3
from app.services.qc_check.verify_file_format_service import verify_file_format_using_metadata


@celery_app.task(name="qc_tasks.run_check")
def metadata_validation_task(
        feed_id: int,
        download_data: list
) -> dict:
    # Download the feed file from S3
    # Extract the feed file[s]
    # Verify the metadata (file format, file type, delimiter) against the predefined schema
    # Check if control file present in extracted folder
    # Read file names and rows count from control file
    # Verify if all files listed in control file are present in extracted folder
    # Verify if all files in DB for the feed (with the respective id) are present in extracted folder
    # Validate data types for all files in DB for the feed (with the respective id)
    # Verify if record count of each file matches with control file
    # Ensure all files are not empty after header row
    # Extract unique drug_ids and health_plan_ids and create Excel
    # Fetch active drugs from DB and verify against extracted drug_ids
    # Verify if unique drug count in formulary matches drug file
    # Verify if formularies' row count matches drug-plan combination
    # Verify if nomenclature_set_ids match between nomenclature_data and nomenclature_data_map
    # Verify mapping consistency for multiple files (IDs only)
    # Verify mapping consistency between DB-driven health_plans and custom_accounts/zip_lives (IDs only)
    # Verify mapping consistency between health_plans and related files (IDs only)
    # Verify national lives totals from health_plans
    # Verify exclusions list for health plans
    # Verify the nomenclature data list
    # Verify restrictions list

    # Get the Celery-generated task_id
    task_id = metadata_validation_task.request.id
    final_audit_log = []

    # Download and extract the zipped folder
    downloaded_folder_path = download_file_from_s3(download_data, task_id)
    extracted_folder_path = extract_feed_zip(task_id, downloaded_folder_path)

    final_audit_log.append(
        verify_file_format_using_metadata(
            feed_id,
            extracted_folder_path,
            task_id
        )
    )

    # Control file checks start here
    control_file_check = check_control_file_exists(extracted_folder_path, task_id)
    final_audit_log.append(control_file_check)

    if control_file_check["status"] != "PASSED":
        final_audit_log.append(
            read_filenames_from_control_file(
                None,
                False,
                task_id
            )
        )

    control_file_path = control_file_check.get("control_file_path")
    final_audit_log.append(
        read_filenames_from_control_file(
            control_file_path,
            True,
            task_id
        )
    )
    # Control file checks end here

    return {
        "status": "completed",
        "feed_id": feed_id,
        "task_id": task_id
    }