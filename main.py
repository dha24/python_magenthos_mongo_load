import random
import pymongo
import threading
import string
from datetime import datetime, timedelta, tzinfo, timezone

mongo_client = None

#### till 2023-07-18T09:05:40.459+00:00 = 190000

# 10,000


DAY_DIFFERENCE = 40
TOTAL_THREADS_WHICH_INGEST_DATA = 1
NO_OF_EXECUTION_BY_THREAD = 10
SIZE_OF_INSERT_RECORDS_IN_ONE_BATCH = 1
DB_NAME = "growthwell"
COLLECTION_NAME = "classes"

MONGO_CONNECTION_STRING = "mongodb+srv://user:pass@growthwellness.z63ug.mongodb.net/?retryWrites=true&w=majority"


def create_district_payload_json(input_integer):
    district_name = f"district-{input_integer}"
    num_schools = 10
    schools = []

    superadmins = [
        "dorien@gocusdom.com",
        "tech@123wellness.org",
        "zitterpappel99@gmail.com",
        "example1@example.com",
        "example2@example.com",
        "example3@example.com",
        "example4@example.com",
        "example5@example.com"
    ]

    for i in range(1, num_schools + 1):
        school_name = f"{district_name}-school-{i}"
        schools.append(school_name)

    random_superadmins = random.sample(superadmins, 5)  #
    district = {
        "createdAt": datetime.utcnow(),
        "name": district_name,
        "schools": schools,
        "superadmins": random_superadmins,
    }

    return district

#it will create for every 10 district, it will create 10 schools ,and every school has 10 classes
def create_school_documents():
    schools_per_district = 10
    classes_per_school = 10
    superadmins = [
        "schooladmin1@example.com",
        "schooladmin2@example.com",
        "schooladmin3@example.com",
        "schooladmin4@example.com",
        "schooladmin5@example.com"
    ]
    random_schooladmins = random.sample(superadmins, 2)  # Choose 2 random superadmins
    school_documents = []
    for district_num in range(10):  # i from 0 to 9
        district_name = f"district-{district_num}"

        for school_num in range(schools_per_district):  # i from 0 to 9
            school_name = f"{district_name}-school-{school_num}"
            school_classes = []

            for class_num in range(1, classes_per_school + 1):
                class_name = f"{school_name}-class-{class_num}"
                school_classes.append(class_name)

            school_document = {
                "district_name": district_name,
                "name": school_name,
                "classes": school_classes,
                "admins": random_schooladmins
            }
            school_documents.append(school_document)

    return school_documents


def create_class_records():
    classes = []
    districts = 10
    schools_per_district = 10
    classes_per_school = 20
    students_per_class = 30

    for district_num in range(districts):
        district_name = f"district-{district_num}"

        for school_num in range(schools_per_district):
            school_name = f"{district_name}-school-{school_num}"

            for class_num in range(1, classes_per_school + 1):
                class_name = f"{school_name}-class-{class_num}"
                students = [f"Student{i}" for i in range(1, students_per_class + 1)]

                num_teachers = random.randint(1, 5)  # Random number of teachers between 1 and 5
                all_teachers = [f"teacher{i}" for i in range(1, 6)]
                selected_teachers = random.sample(all_teachers, min(2, num_teachers))  # Choose up to 2 random teachers

                class_record = {
                    "name": class_name,
                    "students": students,
                    "teachers": selected_teachers,
                    "school": school_name,
                    "district": district_name
                }
                classes.append(class_record)

    return classes

def get_mongo_client():
    global mongo_client
    if not mongo_client:
        # Connect to MongoDB on localhost
        print("MONGO_CONNECTION_STRING:", MONGO_CONNECTION_STRING)

        mongo_client = pymongo.MongoClient(MONGO_CONNECTION_STRING)
        # mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")

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

    # for i in range(NO_OF_EXECUTION_BY_THREAD):
    #     data_to_load = [create_school_documents() for _ in range(SIZE_OF_INSERT_RECORDS_IN_ONE_BATCH)]
    #
    #     # Insert the data list into the collection
    #     inserted_ids = insert_data_into_collection(data_to_load)
    inserted_ids = insert_data_into_collection(create_class_records())
    print(f"Thread-{thread_id} inserted {len(inserted_ids)} documents ")


if __name__ == '__main__':
    print_hi(f"Starting ingesting data in batch by {TOTAL_THREADS_WHICH_INGEST_DATA} Threads")
    print(datetime.utcnow())
    threads = [threading.Thread(target=insert_data_worker, args=(i + 1,)) for i in
               range(TOTAL_THREADS_WHICH_INGEST_DATA)]
    # Start all threads
    for thread in threads:
        thread.start()

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    print("All threads have finished inserting data.")
    print(datetime.utcnow())
