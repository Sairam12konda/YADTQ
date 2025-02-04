# result_backend.py

import redis
import config

# Connect to Redis
r = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=0)

# Store initial task status
def store_task(task_id, status):
    r.hset(task_id, "status", status)

# Update task status and store result if available
def update_task_status(task_id, status, result=None):
    r.hset(task_id, "status", status)
    if result is not None:
        r.hset(task_id, "result", result)

# Retrieve task status and result
def get_task_status(task_id):
    status = r.hget(task_id, "status").decode('utf-8')
    result = r.hget(task_id, "result")
    result = result.decode('utf-8') if result else None
    return status, result
