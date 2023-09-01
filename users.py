import random
from pymongo import MongoClient

# Create a MongoDB connection
MONGO_CONNECTION_STRING = "mongodb+srv://user:pass@growthwellness.z63ug.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(MONGO_CONNECTION_STRING)
db = client["growthwell"]  # Replace with your database name
collection = db["users"]  # Replace with your collection name

districts = 10
schools_per_district = 10
classes_per_school = 20
users_to_insert = 40000
batch_size = 1000


# Function to generate user documents
def generate_user_documents(start_index, end_index):
    users = []
    for i in range(start_index, end_index):
        user_id = f"user-id-{i}"
        email = f"user-email-{i}@example.com"
        district_num = random.randint(0, districts - 1)
        school_num = random.randint(0, schools_per_district - 1)
        class_num = random.randint(1, classes_per_school)
        school_name = f"district-{district_num}-school-{school_num}"
        class_name = f"{school_name}-class-{class_num}"  # Include district prefix

        user_document = {
            "user": user_id,
            "email": email,
            "school": school_name,
            "district": f"district-{district_num}",
            "class": [class_name],
            "agreedToTerms": True,
            "dataUse": False,
            "wellnesstimer": "10 seconds",
            "count": random.randint(10, 100)
        }
        users.append(user_document)
    return users


# Insert users in batches
for batch_start in range(1, users_to_insert + 1, batch_size):
    batch_end = min(batch_start + batch_size, users_to_insert + 1)
    user_batch = generate_user_documents(batch_start, batch_end)
    collection.insert_many(user_batch)
    print(f"Inserted {batch_end - batch_start} users")

print("All users inserted.")
