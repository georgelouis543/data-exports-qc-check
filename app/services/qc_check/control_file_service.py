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