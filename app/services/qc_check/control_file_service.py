import logging
import os
from typing import Any


def check_control_file_exists(
        extracted_file_path: str,
        task_id: str
) -> dict[str, dict[Any, Any] | str] | dict[str, dict[Any, Any] | None]:
    """
    Check if the control file exists at the given file path.

    Args:
        extracted_file_path (str): The path to the control file.
        :param extracted_file_path:
        :param task_id:
    """
    audit_step = {}
    try:
        if not os.path.exists(extracted_file_path):
            logging.error(f"File {extracted_file_path} does not exist.")
            raise FileNotFoundError(f"[{task_id}] The specified path does not exist.")

        control_file = None
        for file_name in os.listdir(extracted_file_path):
            if file_name.lower().startswith('control'):
                control_file = os.path.join(extracted_file_path, file_name)
                break

        if control_file and os.path.exists(control_file):
            audit_step['step_name'] = 'CHECK CONTROL FILE EXISTS'
            audit_step['status'] = 'PASSED'
            audit_step['details'] = {
                'message': f'Control file found at {control_file}.',
                'file_path': control_file
            }
            logging.info(f"[{task_id}] Control file found at {control_file}.")
            return {
                "audit_step": audit_step,
                "control_file_path": str(control_file)
            }

        audit_step['step_name'] = 'CHECK CONTROL FILE EXISTS'
        audit_step['status'] = 'SKIPPED'
        audit_step['details'] = {
            'message': 'Control file not found in the specified path.'
        }

        logging.info(f"[{task_id}] Control file not found for this feed.")
        return {
            "audit_step": audit_step,
            "control_file_path": None
        }

    except FileNotFoundError as fnf_error:
        raise fnf_error

    except Exception as e:
        logging.error(f"[{task_id}] An error occurred while checking for the control file: {e}")
        raise e


def read_filenames_from_control_file(
        control_file_path: str | None,
        is_control_file_present: bool,
        task_id: str
) -> dict:
    """
    Read filenames from the control file.

    Args:
        control_file_path (str): The path to the control file.
        :param control_file_path:
        :param is_control_file_present:
        :param task_id:
    """
    audit_step = {}

    if not is_control_file_present:
        logging.info(f"[{task_id}] Control file is not present. Skipping filename read.")
        audit_step['status'] = 'SKIPPED'
        audit_step['step_name'] = 'READ FILENAMES FROM CONTROL FILE'
        audit_step['details'] = {
            'message': 'Control file is not present. Skipping filename read.'
        }
        return audit_step

    file_names = []
    file_record_counts = {}

    # Do the following if the control file is of TXT format
    try:
        with open(control_file_path, 'r') as control_file:
            lines = control_file.readlines()[1:]  # Skip header (since file names are from 2nd line)

            for line in lines:
                parts = line.strip().split('|')
                if len(parts) >= 2:
                    file_name = parts[0]
                    record_count = int(parts[1])
                    file_names.append(file_name)
                    file_record_counts[file_name] = record_count

        audit_step['step_name'] = 'READ FILENAMES FROM CONTROL FILE'
        audit_step['status'] = 'PASSED'
        audit_step['details'] = {
            'message': f'Read {len(file_names)} filenames from control file.',
            'file_names': file_names,
            'file_record_counts': file_record_counts
        }
        logging.info(f"[{task_id}] Successfully read filenames from control file.")
        return audit_step

    except Exception as e:
        logging.error(f"[{task_id}] An error occurred while reading filenames from control file.")
        raise e