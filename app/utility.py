from datetime import datetime

def format_timestamp(t):
    return datetime.utcfromtimestamp(t).isoformat()
