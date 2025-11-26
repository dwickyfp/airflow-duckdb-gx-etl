import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import os

# Initialize Faker
fake = Faker()

def generate_customer_data(num_customers=1000):
    """Generate customer data"""
    customers = []
    
    for i in range(1, num_customers + 1):
        customer = {
            'customer_id': i,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'address': fake.address().replace('\n', ', '),
            'city': fake.city(),
            'country': fake.country(),
            'date_of_birth': fake.date_of_birth(minimum_age=18, maximum_age=80),
            'registration_date': fake.date_between(start_date='-3y', end_date='today'),
            'customer_segment': random.choice(['Premium', 'Standard', 'Basic']),
            'is_active': random.choice([True, False]),
            'created_at': fake.date_time_between(start_date='-3y', end_date='now')
        }
        customers.append(customer)
    
    return pd.DataFrame(customers)

def generate_order_data(num_orders=5000, num_customers=1000):
    """Generate order data"""
    orders = []
    
    products = [
        'Laptop', 'Smartphone', 'Tablet', 'Headphones', 'Monitor', 
        'Keyboard', 'Mouse', 'Webcam', 'Speakers', 'Printer',
        'Router', 'Hard Drive', 'RAM', 'Graphics Card', 'Motherboard'
    ]
    
    categories = [
        'Electronics', 'Computer Hardware', 'Accessories', 
        'Mobile Devices', 'Audio', 'Office Equipment'
    ]
    
    statuses = ['Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled']
    
    for i in range(1, num_orders + 1):
        order_date = fake.date_between(start_date='-2y', end_date='today')
        
        order = {
            'order_id': i,
            'customer_id': random.randint(1, num_customers),
            'product_name': random.choice(products),
            'category': random.choice(categories),
            'quantity': random.randint(1, 10),
            'unit_price': round(random.uniform(10.0, 2000.0), 2),
            'total_amount': 0,  # Will calculate after
            'order_date': order_date,
            'status': random.choice(statuses),
            'payment_method': random.choice(['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer']),
            'shipping_address': fake.address().replace('\n', ', '),
            'created_at': fake.date_time_between(start_date=order_date, end_date='now')
        }
        
        # Calculate total amount
        order['total_amount'] = round(order['quantity'] * order['unit_price'], 2)
        orders.append(order)
    
    return pd.DataFrame(orders)

def main():
    """Main function to generate and save CSV files"""
    print("Generating customer data...")
    customers_df = generate_customer_data(1000)
    
    print("Generating order data...")
    orders_df = generate_order_data(5000, 1000)
    
    # Create dags directory if it doesn't exist
    dags_dir = "dags"
    os.makedirs(dags_dir, exist_ok=True)
    
    # Save to CSV files
    customers_file = os.path.join(dags_dir, "customers_data.csv")
    orders_file = os.path.join(dags_dir, "orders_data.csv")
    
    customers_df.to_csv(customers_file, index=False)
    orders_df.to_csv(orders_file, index=False)
    
    print(f"Customer data saved to: {customers_file}")
    print(f"Order data saved to: {orders_file}")
    print(f"Generated {len(customers_df)} customers and {len(orders_df)} orders")
    
    # Display sample data
    print("\nSample Customer Data:")
    print(customers_df.head())
    print("\nSample Order Data:")
    print(orders_df.head())

if __name__ == "__main__":
    main()