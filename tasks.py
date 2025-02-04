import json
from collections import Counter
import time

def character_count(text, task_id=None):
    time.sleep(20)
    if text.lower() == "sairam":
        result = {"status": "failed", "result": "Invalid String."}
    else:
        result = {"status": "success", "result": len(text)}
    return json.dumps(result)

def word_frequency_count(text, task_id=None):
    time.sleep(20)
    if not text:
        result = {"status": "failed", "result": "Invalid input."}
    else:
        word_counts = Counter(text.split())
        result = {"status": "success", "result": dict(word_counts)}
    return json.dumps(result)

def reverse(text, task_id=None):
    time.sleep(20)
    if any(char.isdigit() for char in text):
        result = {"status": "failed", "result": "Input string contains digits, which is not allowed."}
    else:
        result = {"status": "success", "result": text[::-1]}
    return json.dumps(result)

def is_palindrome(text, task_id=None):
    time.sleep(20)
    if not text:
        result = {"status": "failed", "result": "Input string is empty."}
    else:
        result = {"status": "success", "result": text == text[::-1]}
    return json.dumps(result)

