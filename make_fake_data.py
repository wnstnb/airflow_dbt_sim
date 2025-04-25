from faker import Faker
import pandas as pd
import numpy as np
import random
from dateutil.relativedelta import relativedelta
from datetime import timedelta
import datetime
import os

# Create data directory if it doesn't exist
if not os.path.exists('data'):
    os.makedirs('data')

fake = Faker()
# Set random seed for reproducibility
random.seed(42)
fake.seed_instance(42)

num_customers = 1000  # Number of unique customers
max_accounts_per_customer = 3  # Maximum number of accounts per customer
account_types = ['Checking', 'Savings', 'Credit Card']

# --- Customer Data ---
print("Generating customer data...")
customers = []
for i in range(num_customers):
    customers.append({
        'CustomerID': fake.unique.random_number(digits=5),
        'Name': fake.name(),
        'Region': fake.state(),
        'JoinDate': fake.date_between(start_date='-10y', end_date='-1y'),  # Join date between 10 and 1 years ago
        'CreditScore': random.choice([None, random.randint(300, 850)]),  # Credit score with None values
    })

customers_df = pd.DataFrame(customers)
customers_df.to_csv('data/customers.csv', index=False)
print("CSV file 'customers.csv' created successfully.")

# --- Account Data ---
print("Generating account data...")
accounts = []
for _, customer in customers_df.iterrows():
    customer_id = customer['CustomerID']
    join_date = pd.to_datetime(customer['JoinDate'])
    
    # Determine how many accounts this customer has
    num_accounts = random.randint(1, max_accounts_per_customer)
    
    # Randomly select account types without replacement if possible
    account_types_for_customer = random.sample(account_types, min(num_accounts, len(account_types)))
    
    # If customer has more accounts than account types, allow duplicates
    while len(account_types_for_customer) < num_accounts:
        account_types_for_customer.append(random.choice(account_types))
    
    for account_type in account_types_for_customer:
        # Create a unique account number
        account_number = f"{customer_id}-{str(len(accounts) % 999).zfill(3)}"
        
        # Open date should be on or after join date but before now
        min_open_date = join_date
        max_open_date = datetime.datetime.now().date() - timedelta(days=30)  # At least a month old
        open_date = fake.date_between(start_date=min_open_date, end_date=max_open_date)
        
        # Different balance ranges based on account type
        if account_type == 'Checking':
            balance = round(random.uniform(100, 15000), 2)
        elif account_type == 'Savings':
            balance = round(random.uniform(1000, 100000), 2)
        else:  # Credit Card
            balance = round(random.uniform(500, 50000), 2)
        
        accounts.append({
            'AccountNumber': account_number,
            'CustomerID': customer_id,
            'AccountType': account_type,
            'OpenDate': open_date,
            'Balance': balance,
            'IsActive': random.random() < 0.9,  # 90% of accounts are active
            'InterestRate': round(random.uniform(0.01, 0.05), 4) if account_type == 'Savings' else 0.0,
            'CreditLimit': round(random.uniform(1000, 30000), 2) if account_type == 'Credit Card' else None,
        })

accounts_df = pd.DataFrame(accounts)
accounts_df.to_csv('data/accounts.csv', index=False)
print("CSV file 'accounts.csv' created successfully.")

# --- Monthly Balance Data ---
print("Generating monthly balance data...")
monthly_balances = []

for _, account in accounts_df.iterrows():
    account_number = account['AccountNumber']
    customer_id = account['CustomerID']
    account_type = account['AccountType']
    open_date = pd.to_datetime(account['OpenDate'])
    initial_balance = account['Balance']
    
    current_date = pd.to_datetime(datetime.datetime.now())
    end_of_month = open_date.replace(day=28) + timedelta(days=4)
    end_of_month = end_of_month.replace(day=1) - timedelta(days=1)  # Last day of the month
    
    # Start with the initial balance
    running_balance = initial_balance
    
    while end_of_month <= current_date:
        # Simulate balance fluctuations based on account type
        if account_type == 'Checking':
            balance_change = round(random.uniform(-2000, 3000), 2)
        elif account_type == 'Savings':
            # Savings tends to grow steadily
            balance_change = round(random.uniform(-500, 2000), 2)
        else:  # Credit Card
            # Credit card balances fluctuate more
            balance_change = round(random.uniform(-3000, 2500), 2)
        
        # Update running balance
        running_balance = round(running_balance + balance_change, 2)
        
        # Apply account-specific constraints
        if account_type == 'Credit Card':
            credit_limit = account['CreditLimit'] or 10000
            # Credit card balance is debt, so should be negative or zero
            running_balance = max(-credit_limit, min(0, running_balance))
        else:
            # Other accounts can't go below zero
            running_balance = max(0, running_balance)
        
        monthly_balances.append({
            'AccountNumber': account_number,
            'CustomerID': customer_id,
            'AccountType': account_type,
            'MonthEndDate': end_of_month.strftime('%Y-%m-%d'),
            'Balance': running_balance
        })
        
        # Move to next month
        end_of_month += relativedelta(months=1)
        
        # Add some randomness to avoid too regular patterns
        if random.random() < 0.05:  # 5% chance to skip a month
            end_of_month += relativedelta(months=1)

monthly_balances_df = pd.DataFrame(monthly_balances)
monthly_balances_df.to_csv('data/monthly_balances.csv', index=False)
print("CSV file 'monthly_balances.csv' created successfully.")

print("All data files created successfully!") 