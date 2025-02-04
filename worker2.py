import time
import redis
import json
import sys
import threading
import signal  # For signal handling
import tasks
import result_backend as rb

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0)

worker_id = "worker2"

# Function to gracefully handle shutdown (e.g., on Ctrl+C)
def handle_shutdown(signum, frame):
    print("\nShutting down gracefully...")
    r.set(f"worker:{worker_id}:active", 0)  # Mark worker as inactive
    print(f"Worker {worker_id} marked as inactive.")
    sys.exit(0)  # Exit the program

# Continuous loop to update worker status as "active"
def update_worker_status():
    while True:
        print("Sending HeartBeat")
        r.set(f"worker:{worker_id}:active", 1)  # Mark worker as active
        time.sleep(30)  # Update every 30 seconds

# Task execution function using tasks.py functions
def execute_task(task):
    task_type = task['task_type']
    args = task['args']
    task_id = task['task_id']

    try:
        # Execute the task based on task_type, each returning a JSON string
        if task_type == "character_count":
            result = tasks.character_count(*args, task_id=task_id)
        elif task_type == "word_frequency_count":
            result = tasks.word_frequency_count(*args, task_id=task_id)
        elif task_type == "reverse_string":
            result = tasks.reverse(*args, task_id=task_id)
        elif task_type == "is_palindrome":
            result = tasks.is_palindrome(*args, task_id=task_id)
        else:
            result = json.dumps({"status": "failed", "result": f"Unknown task type: {task_type}"})

        result_dict = json.loads(result)
        
        # Check status and update task status accordingly
        if result_dict.get("status") == "success":
            # Store success status as JSON string in result storage (Redis)
            rb.update_task_status(task_id, "success", result=result)
            print("Task executed successfully")
            return "success"
        else:
            # Store failure status as JSON string in result storage (Redis)
            rb.update_task_status(task_id, "failed", result=result)
            print("Task executed but failed due to invalid input")
            return "failure"

    except Exception as e:
        # On error, update task status to failed with exception info
        error_result = json.dumps({"status": "failed", "result": str(e)})
        print(f"Error executing task {task_id}: {str(e)}")
        rb.update_task_status(task_id, "failed", result=error_result)
        return "failure"

# Main worker function
if __name__ == '__main__':
    # Register the signal handler for graceful shutdown
    signal.signal(signal.SIGINT, handle_shutdown)

    # Start the heartbeat function in a separate thread (in the background)
    heartbeat_thread = threading.Thread(target=update_worker_status, daemon=True)
    heartbeat_thread.start()

    print(f"Worker {worker_id} is running. Press Ctrl+C to stop.")

    while True:
        pass  # Keep the worker running
