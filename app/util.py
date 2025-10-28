import math
import time
from datetime import timezone, datetime, timedelta


def now():
    return datetime.now(timezone.utc)

def next_timestamp(interval):
    now_ts = time.time()
    return int(math.ceil(now_ts / interval) * interval)
