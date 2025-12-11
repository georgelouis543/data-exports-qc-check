def date_regex_for_filenames(
        service_name: str | None = None
) -> str:
    """
    Returns a regex pattern that matches dates in filenames.
    The pattern matches dates in the formats:
    - *_20250101.txt
    - *.2025-01-01.csv
    - *-2025_01_01.parquet
    Returns:
        str: A regex pattern string.
    """
    if service_name == "extract_zipped_folder":
        return r'_(\d{8})(?:\.\w+)?$'

    if service_name == "verify_file_format":
        return r'([_\-\.\/]?\d{1,4}([_\-\.\/]?\d{1,4})*)$'

    return r'_(\d{8})(?:\.\w+)?$'