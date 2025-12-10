def date_regex_for_filenames():
    """
    Returns a regex pattern that matches dates in filenames.
    The pattern matches dates in the formats:
    - *_20250101.txt

    Returns:
        str: A regex pattern string.
    """
    return r'_(\d{8})(?:\.\w+)?$'