import psycopg2
from psycopg2.extras import execute_values
import random
import string
from datetime import datetime, timedelta
import time

def insert_sample_products(cur, n=10):
    for i in range(n):
        name = f"Product_{i+1}"
        desc = 'test'
        price = round(random.uniform(1, 100), 2)
        stock = random.randint(10, 1000)
        cur.execute("""
            INSERT INTO products (name, description, price, stock_quantity)
            VALUES (%s, %s, %s, %s)
        """, (name, desc, price, stock))
        cur.connection.commit()
        print(f"Inserted product {name}")
        time.sleep(1)

def insert_sample_orders(cur, n=5):
    for i in range(n):
        customer_name = f"Customer_{i+1}"
        address = "test"
        phone = '0000000000'
        status = random.choice(["pending", "shipped", "delivered"])
        order_date = datetime.now() - timedelta(days=random.randint(0, 30))
        cur.execute("""
            INSERT INTO orders (customer_name, address, phone, order_date, status)
            VALUES (%s, %s, %s, %s, %s)
        """, (customer_name, address, phone, order_date, status))
        cur.connection.commit()
        print(f"Inserted order for {customer_name}")
        time.sleep(1)

def insert_sample_order_details(cur, n=10):
    cur.execute("SELECT order_id FROM orders ORDER BY order_id")
    order_ids = [row[0] for row in cur.fetchall()]
    cur.execute("SELECT product_id, price FROM products ORDER BY product_id")
    products_info = cur.fetchall()
    if not order_ids or not products_info:
        print("Orders and products data are required first!")
        return
    for _ in range(n):
        order_id = random.choice(order_ids)
        product = random.choice(products_info)
        product_id, unit_price = product
        quantity = random.randint(1, 10)
        discount = round(random.uniform(0, 0.3), 2)
        cur.execute("""
            INSERT INTO order_details (order_id, product_id, unit_price, quantity, discount)
            VALUES (%s, %s, %s, %s, %s)
        """, (order_id, product_id, float(unit_price), quantity, discount))
        cur.connection.commit()
        print(f"Inserted order_detail for order {order_id}, product {product_id}")
        time.sleep(1)

if __name__ == "__main__":
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="ecommerce",
        user="debezium",
        password="6666"
    )
    cur = conn.cursor()

    insert_sample_products(cur, n=100)
    insert_sample_orders(cur, n=100)
    # insert_sample_order_details

    cur.close()
    conn.close()
    print("Sample data inserted automatically!")
