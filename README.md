# RR-Team-11-yadtq-yet-another-distributed-task-queue-

Kafka and redis should be installed
First run yadtq.py file
Then run all worker.py and then client.py
You can run client.py without running worker, on that time yadtq gives message of no workers available

REQUEST HANDLING
* When a task is submited by client, it sent yadtq model, which then stores in redis with unique task_id
* Initial process state is update to queued

STATUS TRACKING
* client will receives status message continuously
* When a worker is assinged processing status is received at client
* Then success stauts if task executed completely else failed status is sent

LOAD BALANCING:
* we incremented the number of task executed by particular worker when a task is assigned and decremented when it completed
* Using the number of task executed by workers, we select the worker which has least number of currently executing task as load balancing

HEALTH MONITORING
* We have maintain active status of each worker in redis
* We have made thread in worker.py that continuously updates active bit in redis
* yadtq uses that bit from redis to check whether is active or not

FAULT TOLERENCE
* If error is in the client query failed status is updated in redis
* If failed due to any worker mistake, then task is sent to retry

WORKER CONCURRENCY
* After yadtq assigns task to a worker, it doesn't wait for it to complete.
* It consumes next task, check the free worker and assign work to it

