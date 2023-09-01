import random
import datetime
import threading
from pymongo import MongoClient

# Create a MongoDB connection
MONGO_CONNECTION_STRING = "mongodb+srv://user:pass@growthwellness.z63ug.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(MONGO_CONNECTION_STRING)
db = client["growthwell"]  # Replace with your database name
collection = db["wellness-6m"]  # Replace with your collection name

users_per_thread = 4000
num_threads = 10
total_users = users_per_thread * num_threads
actions = ["Get some sun in my eyes", "Take a walk", "Read a book", "Listen to music", "watch a movie"]
emotions = ["sad", "happy", "not feeling good", "like it", "all Right", "super happy"]
num_days = 6 * 30  # 6 months
num_wellness_docs_per_day = 2


def generate_wellness_documents(thread_id, start_index, end_index):
    wellness_documents = []
    start_date = datetime.datetime(2023, 1, 1)  # Replace with your desired start date

    for i in range(start_index, end_index):
        user_id = f"user-id-{i}"
        current_date = start_date
        for _ in range(num_days):
            for _ in range(num_wellness_docs_per_day):
                action = random.choice(actions) + "-" + user_id
                helpful = random.choice([True, False])
                emotion = random.choice(emotions)

                wellness_document = {
                    "userID": user_id,
                    "date": current_date,
                    "action": action,
                    "helpful": helpful,
                    "emotion": emotion
                }
                wellness_documents.append(wellness_document)

            current_date -= datetime.timedelta(days=1)  # Move to the previous day

    # Insert wellness documents for this thread
    collection.insert_many(wellness_documents)
    print(f"Thread {thread_id} inserted {len(wellness_documents)} wellness documents.")


# Create and start threads
threads = []

for thread_id in range(num_threads):
    start_index = thread_id * users_per_thread + 1
    end_index = (thread_id + 1) * users_per_thread + 1
    thread = threading.Thread(target=generate_wellness_documents, args=(thread_id, start_index, end_index))
    threads.append(thread)
    thread.start()

# Wait for all threads to finish
for thread in threads:
    thread.join()

print("All wellness documents inserted.")
