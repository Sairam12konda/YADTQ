import time
import json
import redis
from kafka import KafkaProducer, KafkaConsumer
import config
import result_backend as rb
from uuid import uuid4
import threading  # Import threading library for background processing
import worker1
import worker2
import worker3

# Connect to Redis (for heartbeat and worker status management)
r = redis.Redis(host='localhost', port=6379, db=0)

def initialize_workers():
    workers = ["worker1", "worker2", "worker3"]
    for worker in workers:
        # Initialize worker with 'active' status and tasks count
        r.set(f"worker:{worker}:active", 0)  # 0 means inactive
        r.set(f"worker:{worker}:tasks", 0)  # 0 tasks being executed

# Producer (Client) logic to submit task to Kafka
def send_task(task_type, args, task_id):
    producer = KafkaProducer(bootstrap_servers=config.KAFKA_BROKER_URL, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    task = {
        "task_id": task_id,
        "task_type": task_type,
        "args": args
    }
    rb.store_task(task_id, "queued")  # Set initial task status
    producer.send(config.KAFKA_TASK_TOPIC, task)
    producer.flush()

# Consumer (Worker) logic - task delegation to workers
def consume_tasks():
    consumer = KafkaConsumer(
        config.KAFKA_TASK_TOPIC,
        bootstrap_servers=config.KAFKA_BROKER_URL,
        group_id='worker-group',  # Shared consumer group
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    for message in consumer:
        task = message.value
        task_id = task['task_id']
        print(task)
        print(f"Task {task_id} received for execution.")

        # Update the task status to "assigned" before delegating it to the worker
        rb.store_task(task_id, "processing")
        time.sleep(2)
       
        # Assign task to a worker based on load balancing
        worker_id = assign_task_to_worker()
        if worker_id:
            print(f"Assigning task {task_id} to {worker_id} for execution.")
           
            # Start execute_task_in_worker() in a separate thread
            task_thread = threading.Thread(target=execute_task_in_worker, args=(worker_id, task))
            task_thread.start()  # Run task in background, allowing consumer to continue
           
        else:
            print(f"No available workers for task. Retrying...")
            retry_task(task)

# Assign task to a worker based on load balancing logic
def assign_task_to_worker():
    workers = ["worker1", "worker2", "worker3"]
    best_worker = None
    lowest_tasks = float('inf')

    for worker in workers:
        worker_active = r.get(f"worker:{worker}:active")
        worker_tasks = int(r.get(f"worker:{worker}:tasks"))

        if worker_active and worker_active.decode('utf-8') == '1' and worker_tasks < lowest_tasks:
            best_worker = worker
            lowest_tasks = worker_tasks

    if best_worker:
        r.incr(f"worker:{best_worker}:tasks")  # Increment task count
        return best_worker
    else:
        return None

def execute_task_in_worker(worker_id, task):
    try:
        print(f"Delegating task {task['task_id']} to {worker_id} for execution.")
       
        # Call the respective worker's execute_task function
        if worker_id == "worker1":
            result = worker1.execute_task(task)
        elif worker_id == "worker2":
            result = worker2.execute_task(task)
        elif worker_id == "worker3":
            result = worker3.execute_task(task)
        else:
            print(f"Unknown worker ID: {worker_id}")
            return
       
        # Handle success or failure based on result from worker
        if result == "success":
            print(f"Task {task['task_id']} executed successfully by {worker_id}.")
            r.decr(f"worker:{worker_id}:tasks")
        else:
            print("Task executed but invalid input.")
            r.decr(f"worker:{worker_id}:tasks")

    except Exception as e:
        print(f"Error executing task {task['task_id']} by {worker_id}: {e}")
        retry_task(task)

def retry_task(task):
    print(f"Retrying task {task['task_id']}")
    send_task(task['task_type'], task['args'], task['task_id'])

if __name__ == '__main__':
    initialize_workers()
    consume_tasks()
