import yadtq
import result_backend as rb
import time
from uuid import uuid4

# Submit a task
def submit_task(task_type, args, task_id):
    yadtq.send_task(task_type, args, task_id)
    print(f"Task {task_id} submitted.")


# Check task status
def check_task_status(task_id):
    status, result = rb.get_task_status(task_id)
    return status, result

# Main function to interact with the user
def main():
    print("Choose a task to submit:")
    print("1. Character Count")
    print("2. Word Frequency Count")
    print("3. Reverse String")
    print("4. Palindrome Check")
    
    # Take the user's choice
    choice = input("Enter the number of your choice (1/2/3/4): ")
    
    # Take the string input from the user for task arguments
    input_string = input("Enter the string for the task: ")

    # Submit the corresponding task based on the user's choice
    if choice == "1":
        task_type = "character_count"
    elif choice == "2":
        task_type = "word_frequency_count"
    elif choice == "3":
        task_type = "reverse_string"
    elif choice == "4":
        task_type = "is_palindrome"
    else:
        print("Invalid choice!")
        return

    args = [input_string]
    # Submit the task
    task_id = str(uuid4())
    submit_task(task_type, args, task_id)

    # Polling loop to check the status of the task continuously
    while True:
        status, result = check_task_status(task_id)
        print(f"Task {task_id} : {status}")
        
        if status == "success":
            print(f"Task {task_id} completed successfully. Result: {result}")
            break  # Exit the loop if the task is successful
        elif status == "failed":
            print(f"Task {task_id} failed. Error: {result}")
            break  # Exit the loop if the task failed
        else:
            # If the task is still in progress, wait for 2 seconds and check again
            print(f"Task {task_id} : {status}")
            time.sleep(2)

if __name__ == "__main__":
    main()

