import json
import os

import diskcache

# import redis

disk_cache = diskcache.Cache("cache")


def force_refresh_cache(f, *args, **kwargs):
    key = f.__cache_key__(*args, **kwargs)
    disk_cache.delete(key)
    return f(*args, **kwargs)
