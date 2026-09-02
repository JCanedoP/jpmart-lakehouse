# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "faker",
# ]
# ///
# MAGIC %md
# MAGIC # JPmart - Fake Data Generator
# MAGIC
# MAGIC Generates synthetic e-commerce data for the fictional JPmart lakehouse project,
# MAGIC with ~8% intentional dirtiness (nulls, duplicates, inconsistent types,
# MAGIC malformed dates, invalid emails) so the Silver layer has real cleaning work to do.
# MAGIC
# MAGIC Entities generated directly into the Bronze volume as CSV:
# MAGIC - customers      (~1,000 rows)
# MAGIC - products       (~200 rows)
# MAGIC - orders         (~5,000 rows)
# MAGIC - order_items    (~1-4 items per order, ~12,500 rows)
# MAGIC - web_events     (~20,000 rows)
# MAGIC
# MAGIC Output: `/Volumes/jpmart/bronze/raw_files/`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install dependencies
# MAGIC Faker is not preinstalled on the Databricks Runtime, so it needs to be
# MAGIC installed on the cluster before importing it.

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import csv
import os
import random
import uuid
from datetime import datetime, timedelta

from faker import Faker

SEED = 42
random.seed(SEED)
fake = Faker()
Faker.seed(SEED)

N_CUSTOMERS = 1_000
N_PRODUCTS = 200
N_ORDERS = 5_000
N_WEB_EVENTS = 20_000

DIRTY_RATE = 0.08

OUTPUT_DIR = "/Volumes/jpmart/bronze/raw_files"
os.makedirs(OUTPUT_DIR, exist_ok=True)

CATEGORIES = [
    "Electronics", "Home & Kitchen", "Clothing", "Books",
    "Sports & Outdoors", "Beauty", "Toys & Games", "Grocery",
]

ORDER_STATUSES = ["pending", "shipped", "delivered", "cancelled", "returned"]

US_STATES = [
    "VA", "MD", "DC", "NY", "CA", "TX", "FL", "IL", "PA", "OH",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Intentional "dirtiness" helpers
# MAGIC
# MAGIC

# COMMAND ----------

def maybe_dirty(value, dirty_value, rate=DIRTY_RATE):
    """Returns dirty_value with probability `rate`, otherwise returns value."""
    r = random.random()
    return dirty_value if r < rate else value


def dirty_email(email):
    """Introduces invalid emails: missing @, malformed domain, or null."""
    r = random.random()
    if r < DIRTY_RATE * 0.4:
        return None
    if r < DIRTY_RATE * 0.7:
        return email.replace("@", "_at_")  # no valid @
    if r < DIRTY_RATE:
        return email.upper() + "  "  # uppercase + extra spaces
    return email


def dirty_date(date_obj, fmt_pool):
    """Returns the date in a random format (sometimes inconsistent)."""
    r = random.random()
    if r < DIRTY_RATE:
        fmt = random.choice(fmt_pool)
        return date_obj.strftime(fmt)
    return date_obj.strftime("%Y-%m-%d")


DATE_FORMATS_DIRTY = ["%d/%m/%Y", "%m-%d-%Y", "%Y/%m/%d", "%B %d, %Y"]


def dirty_price(price):
    """Sometimes turns the price into a string with a currency symbol,
    makes it negative (simulated capture error), or drops it to null."""
    r = random.random()
    if r < DIRTY_RATE * 0.3:
        return f"${price}"  # string instead of float
    if r < DIRTY_RATE * 0.5:
        return -abs(price)  # negative price (capture error)
    if r < DIRTY_RATE:
        return None
    return price

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entity generators

# COMMAND ----------

def generate_customers(n):
    rows = []
    for i in range(1, n + 1):
        first = fake.first_name()
        last = fake.last_name()
        email = f"{first.lower()}.{last.lower()}{random.randint(1, 999)}@{fake.free_email_domain()}"
        signup_date = fake.date_between(start_date="-3y", end_date="today")

        row = {
            "customer_id": f"JPM-CUST-{i:06d}",
            "first_name": first,
            "last_name": last,
            "email": dirty_email(email),
            "signup_date": dirty_date(signup_date, DATE_FORMATS_DIRTY),
            "state": maybe_dirty(random.choice(US_STATES), random.choice(US_STATES).lower()),
            "loyalty_member": maybe_dirty(random.choice([True, False]), None),
        }
        rows.append(row)

    # Inject exact duplicates (~2% of rows)
    n_dupes = int(n * 0.02)
    rows += random.sample(rows, n_dupes)
    random.shuffle(rows)
    return rows


def generate_products(n):
    rows = []
    for i in range(1, n + 1):
        base_price = round(random.uniform(5, 500), 2)
        row = {
            "product_id": f"JPM-PROD-{i:06d}",
            "product_name": fake.catch_phrase(),
            "category": maybe_dirty(random.choice(CATEGORIES), None),
            "unit_price": dirty_price(base_price),
            "supplier": fake.company(),
            "active": maybe_dirty(True, "yes"),  # bool vs string inconsistency
        }
        rows.append(row)
    return rows


def generate_orders(n, customer_ids):
    rows = []
    for i in range(1, n + 1):
        order_date = fake.date_time_between(start_date="-2y", end_date="now")
        row = {
            "order_id": f"JPM-ORD-{i:08d}",
            "customer_id": maybe_dirty(random.choice(customer_ids), None),  # orphan FK
            "order_date": dirty_date(order_date, DATE_FORMATS_DIRTY),
            "status": maybe_dirty(
                random.choice(ORDER_STATUSES), random.choice(ORDER_STATUSES).upper()
            ),
        }
        rows.append(row)

    n_dupes = int(n * 0.015)
    rows += random.sample(rows, n_dupes)
    random.shuffle(rows)
    return rows


def generate_order_items(orders, product_ids):
    rows = []
    item_counter = 1
    for order in orders:
        n_items = random.randint(1, 4)
        for _ in range(n_items):
            qty = random.randint(1, 5)
            unit_price = round(random.uniform(5, 500), 2)
            row = {
                "order_item_id": f"JPM-ORDITEM-{item_counter:08d}",
                "order_id": order["order_id"],
                "product_id": maybe_dirty(random.choice(product_ids), None),
                "quantity": maybe_dirty(qty, -qty),  # negative quantity error
                "unit_price": dirty_price(unit_price),
            }
            rows.append(row)
            item_counter += 1
    return rows


EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "remove_from_cart", "checkout_start", "purchase"]


def generate_web_events(n, customer_ids, product_ids):
    rows = []
    for i in range(1, n + 1):
        event_time = fake.date_time_between(start_date="-6M", end_date="now")
        row = {
            "event_id": f"JPM-EVT-{i:08d}",
            "customer_id": maybe_dirty(random.choice(customer_ids + [None] * 50), None),  # anonymous sessions
            "product_id": maybe_dirty(random.choice(product_ids), None),
            "event_type": random.choice(EVENT_TYPES),
            "event_timestamp": dirty_date(event_time, DATE_FORMATS_DIRTY),
            "session_id": str(uuid.uuid4()),
        }
        rows.append(row)
    return rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to the Bronze volume

# COMMAND ----------

def write_csv(rows, filename):
    if not rows:
        return
    path = os.path.join(OUTPUT_DIR, filename)
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  -> {filename}: {len(rows):,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run generation

# COMMAND ----------

print("Generating fake data for JPmart...\n")

print("Customers...")
customers = generate_customers(N_CUSTOMERS)
write_csv(customers, "customers.csv")
customer_ids = [c["customer_id"] for c in customers]

print("Products...")
products = generate_products(N_PRODUCTS)
write_csv(products, "products.csv")
product_ids = [p["product_id"] for p in products]

print("Orders...")
orders = generate_orders(N_ORDERS, customer_ids)
write_csv(orders, "orders.csv")

print("Order items...")
order_items = generate_order_items(orders, product_ids)
write_csv(order_items, "order_items.csv")

print("Web events...")
web_events = generate_web_events(N_WEB_EVENTS, customer_ids, product_ids)
write_csv(web_events, "web_events.csv")

print(f"\nDone. Files written to: {OUTPUT_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC Quick sanity check: read each CSV back with Spark, confirm row counts,
# MAGIC and check that dirty values (nulls, malformed emails/dates/prices) are
# MAGIC actually showing up at roughly the expected ~8% rate.

# COMMAND ----------

for fname in ["customers.csv", "products.csv", "orders.csv", "order_items.csv", "web_events.csv"]:
    df = spark.read.option("header", "true").csv(f"{OUTPUT_DIR}/{fname}")
    print(f"{fname}: {df.count():,} rows")
    df.display()

# COMMAND ----------

# Example: check the null rate on customers.email to confirm dirtiness landed
customers_df = spark.read.option("header", "true").csv(f"{OUTPUT_DIR}/customers.csv")
total = customers_df.count()
null_emails = customers_df.filter(customers_df.email.isNull()).count()
print(f"Null emails: {null_emails} / {total} ({null_emails / total:.1%})")