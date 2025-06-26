import csv
import random
import argparse
import uuid
import os  # Import os for folder creation


def generate_customer_data(num_records):
    first_names = ["John", "Jane", "Alice", "Bob", "Charlie", "Diana"]
    last_names = ["Smith", "Doe", "Johnson", "Williams", "Brown", "Jones"]
    genders = ["Male", "Female"]

    data = []
    for i in range(1, num_records + 1):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        gender = random.choice(genders)
        data.append(
            {
                "id": i,
                "first_name": first_name,
                "last_name": last_name,
                "gender": gender,
            }
        )

    return data


def save_to_csv(data, folder, filename):
    # Ensure the folder exists
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, filename)
    with open(filepath, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)


def generate_transaction_data(customer_ids, num_records):
    transaction_types = ["Purchase", "Refund", "Exchange"]
    data = []
    for _ in range(num_records):
        transaction_id = str(uuid.uuid4())  # Generate a unique transaction ID
        customer_id = random.choice(customer_ids)
        transaction_type = random.choice(transaction_types)
        amount = round(random.uniform(10.0, 500.0), 2)
        data.append(
            {
                "transaction_id": transaction_id,
                "customer_id": customer_id,
                "transaction_type": transaction_type,
                "amounttt": amount,
            }
        )

    return data


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate customer and transaction data."
    )
    parser.add_argument(
        "num_customer_records", type=int, help="Number of customer records to generate"
    )
    parser.add_argument(
        "num_transaction_records",
        type=int,
        help="Number of transaction records to generate",
    )
    args = parser.parse_args()

    # Define the folder to save files
    folder = "test_data"

    # Generate customer data
    customer_data = generate_customer_data(args.num_customer_records)
    save_to_csv(customer_data, folder, "customer_data.csv")

    # Extract customer IDs
    customer_ids = [customer["id"] for customer in customer_data]

    # Generate transaction data
    transaction_data = generate_transaction_data(
        customer_ids, args.num_transaction_records
    )
    save_to_csv(transaction_data, folder, "transaction_data.csv")
