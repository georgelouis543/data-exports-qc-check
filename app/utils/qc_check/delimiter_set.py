def fetch_all_delimiters() -> list[str]:
    return [
        ",",
        "|",
        "\t",
        ";",
        ":"
    ]

def fetch_delimiter_map() -> dict[str, str]:
    return {
        "PIPE": "|",
        "COMMA": ",",
        "TAB": "\t",
    }