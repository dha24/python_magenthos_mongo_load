import random
import pymongo
import threading
import string
from datetime import datetime, timedelta, tzinfo, timezone
mongo_client = None


#### till 2023-07-18T09:05:40.459+00:00 = 190000

#10,000


DAY_DIFFERENCE=40
TOTAL_THREADS_WHICH_INGEST_DATA = 10
NO_OF_EXECUTION_BY_THREAD=10
SIZE_OF_INSERT_RECORDS_IN_ONE_BATCH=100
DB_NAME="ts_old"
COLLECTION_NAME="ts_data_granular_hours"

MONGO_CONNECTION_STRING="mongodb://localhost:27017/"
#Start Time ISO 2023-07-22T18:27:28.480+00:00


def random_port():
    # Create a list with the four numbers
    numbers_list = [443, 80, 27017, 2986]

    # Get the minimum and maximum values from the list
    random_num = random.choice(numbers_list)
    return random_num


def random_Event_direction():
    # Create a list with the four numbers
    numbers_list = ["L2R", "R2U", "LDR", "DUR"]

    # Get the minimum and maximum values from the list
    random_event = random.choice(numbers_list)
    return random_event


def random_event_name():
    # Create a list with the four numbers
    numbers_list = ["Firewall Permit", "Firewall Allow", "Firewall Deny", "Firewall Rejected"]

    # Get the minimum and maximum values from the list
    random_event = random.choice(numbers_list)
    return random_event


def random_tenant_name():
    # Create a list with the four numbers
    numbers_list = ["NE", "HDFC", "ICICI", "IDFC"]

    # Get the minimum and maximum values from the list
    random_tenant = random.choice(numbers_list)
    return random_tenant


def create_payload_json():
    event_direction = random_Event_direction()  # Call the function to get its return value
    port = random_port()
    event_name = random_event_name()
    tenant_name = random_tenant_name()

    current_time = datetime.utcnow()
    # Subtract one day from the current time
    one_day_earlier = current_time - timedelta(days=DAY_DIFFERENCE)

    data = {
        "Start Time ISO": one_day_earlier,
        "Event Direction": event_direction,
        "Event Count": 1,
        "tenant_id": tenant_name,
        "Bytes Received": 263,
        "Report Date": "17/07/2023",
        "Source Network": ''.join(random.choices(string.ascii_letters, k=10)),
        "Start Time": datetime.utcnow(),
        "Rule Name (custom)": None,
        "Source IP": "192.168.86.48",
        "Destination Geographic Country/Region": ''.join(random.choices(string.ascii_letters, k=4)),
        "Application (custom)": None,
        "Source Zone (custom)": None,
        "Log Source Type": "Some Awesome Security Gateway",
        "Firewall module (custom)": "forward",
        "Destination Port": 0,
        "Source Port": port,
        "createdAt": datetime.utcnow(),
        "Log Source": "Some-Awesome_IP.ADDR.78.5_FW",
        "Action": "accept",
        "domainName": "Awesome",
        "Destination Zone (custom)": None,
        "Low Level Category": "Firewall Permit",
        "WeekFrom": "15/07/2023",
        "Domain": 0,
        "Event Name": event_name,
        "Bytes Sent": random.randint(100, 650),
        "Destination IP": "IP.ADD.RES.S00"
    }

    return data


def get_mongo_client():
    global mongo_client
    if not mongo_client:
        # Connect to MongoDB on localhost
        mongo_client = pymongo.MongoClient(MONGO_CONNECTION_STRING)
        #mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")

    return mongo_client


def insert_data_into_collection(data_list):
    # Get the MongoDB client instance
    client = get_mongo_client()

    # Connect to the "sq" database
    db = client[DB_NAME]
    # Get the "test" collection
    collection = db[COLLECTION_NAME]

    # Insert the data into the collection
    result = collection.insert_many(data_list)
    return result.inserted_ids


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


def insert_data_worker(thread_id):
    # Each thread will generate 100 records

    for i in range(NO_OF_EXECUTION_BY_THREAD):

        data_to_load = [create_payload_json() for _ in range(SIZE_OF_INSERT_RECORDS_IN_ONE_BATCH)]

    # Insert the data list into the collection
        inserted_ids = insert_data_into_collection(data_to_load)
    print(f"Thread-{thread_id} inserted {len(inserted_ids)} documents ")


if __name__ == '__main__':
    print_hi(f"Starting ingesting data in batch of 100 by {TOTAL_THREADS_WHICH_INGEST_DATA} Threads")
    print(datetime.utcnow())
    threads = [threading.Thread(target=insert_data_worker, args=(i+1,)) for i in range(TOTAL_THREADS_WHICH_INGEST_DATA)]
    # Start all threads
    for thread in threads:
        thread.start()

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    print("All threads have finished inserting data.")
    print(datetime.utcnow())



