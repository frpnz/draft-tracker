def safe_max_iso(*values: str) -> str:
    vals = [v for v in values if v]
    return max(vals) if vals else "1970-01-01T00:00:00Z"
