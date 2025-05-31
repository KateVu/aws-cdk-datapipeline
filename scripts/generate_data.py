import csv
import random
import string
import argparse

def generate_customer_data(num_records):
    first_names = ['John', 'Jane', 'Alice', 'Bob', 'Charlie', 'Diana']
    last_names = ['Smith', 'Doe', 'Johnson', 'Williams', 'Brown', 'Jones']
    genders = ['Male', 'Female']
    
    data = []
    for i in range(1, num_records + 1):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        gender = random.choice(genders)
        data.append({'id': i, 'first_name': first_name, 'last_name': last_name, 'gender': gender})
    
    return data

def save_to_csv(data, filename):
    with open(filename, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=['id', 'first_name', 'last_name', 'gender'])
        writer.writeheader()
        writer.writerows(data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate customer data and save to CSV.')
    parser.add_argument('num_records', type=int, help='Number of customer records to generate')
    args = parser.parse_args()

    customer_data = generate_customer_data(args.num_records)
    save_to_csv(customer_data, 'customer_data.csv')