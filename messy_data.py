from faker import Faker
from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table
import random

# Create SQLite in-memory (or file) database
engine = create_engine("sqlite:///raw_data.db")
meta = MetaData()

# Define a raw users table
users = Table(
    "users_raw", meta,
    Column("id", Integer, primary_key=True),
    Column("full_name", String),
    Column("email", String),
    Column("age", Integer),
)

meta.create_all(engine)

# Populate with Faker, including some bad rows
fake = Faker()
conn = engine.connect()
for i in range(100):
    # randomly inject a bad email or null name
    name = fake.name() if random.random() > 0.1 else None
    email = fake.email() if random.random() > 0.2 else "not-an-email"
    age = random.choice([fake.random_int(18, 90), -1, None])
    conn.execute(users.insert().values(full_name=name, email=email, age=age))
conn.close()