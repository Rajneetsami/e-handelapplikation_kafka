from kafka import KafkaProducer
import random
import json
import time
import sqlite3
from datetime import datetime
import os

MU = 5
SIGMA = 1

def randomly_chosen_products(nr_of_random_products, nr_products_in_db):
    
    conn = sqlite3.connect('products.db')
    cursor = conn.cursor()
    list_of_random_products = []
    
    try:
        for _ in range(nr_of_random_products):
            rand_prod_id = random.randint(1, nr_products_in_db)
            prod = cursor.execute(f"SELECT * FROM products WHERE product_id={rand_prod_id}").fetchone()
            quantity = random.randint(1,10)
            if quantity > prod[5]:
                quantity = 0

            random_product = {
                    "product_id": prod[0],
                    "product_name": prod[1],
                    "product_type": prod[2],
                    "price_type": prod[3],
                    "price": prod[4],
                    "quantity": quantity
                }
            list_of_random_products.append(random_product)
    except sqlite3.Error as e:
        print("An error occurred:", e)
    finally:

        conn.close()

    return list_of_random_products

def generate_order(order_id, nr_products_in_db):
    customer_id = random.randint(1000, 2000)
    products = randomly_chosen_products(random.randint(1, 5), nr_products_in_db)
    order_time = datetime.now().strftime("%m/%d/%Y-%H:%M:%S")

    try:

            conn = sqlite3.connect('products.db')
            cursor = conn.cursor()
            new_order = {}

            for product in products:
        
                product_id = product['product_id']
                quantity = product['quantity']
                prod = cursor.execute(f"SELECT * FROM products WHERE product_id={product_id}").fetchone()

                if prod:
                    quantity = min(quantity, prod[5])
                
                new_order = {'order_id': order_id,
                        'customer_id': customer_id,
                        'order_details': products,
                        'order_time': order_time}
            return new_order
    except sqlite3.Error as e:
            print("An error occurred:", e)
    finally:
            conn.close()

def get_last_order_id():
    try:
        if not os.path.exists("last_order_id.txt"):
            with open("last_order_id.txt", "w") as file:
                file.write("100")
        with open("last_order_id.txt", "r") as file:
                last_order_id = int(file.read().strip())
    except FileNotFoundError:
        last_order_id = 100
    return last_order_id

def update_last_order_id(order_id):
    with open("last_order_id.txt", "w") as file:
        file.write(str(order_id))
    print("Last order ID updated:", order_id)

if __name__ == "__main__":
    
    order_id = get_last_order_id()
    conn = sqlite3.connect('products.db')
    cursor = conn.cursor()
    products = cursor.execute("SELECT * FROM products").fetchall()
    nr_products_in_db = len(products)
    conn.commit()
    conn.close()

    
    producer = KafkaProducer(bootstrap_servers = "localhost:9092",value_serializer = lambda x: json.dumps(x).encode("utf-8"))

    try:
        while True:
            random_whole_numb_gaussian = int(random.gauss(mu=MU, sigma=SIGMA))
            for _ in range(random_whole_numb_gaussian):
                order_id+=1
                update_last_order_id(order_id)
                new_order = generate_order(order_id, nr_products_in_db)
                producer.send("Order3",new_order)
                producer.flush()
                print("order sent:", new_order)
                time.sleep(2)
                
    except KeyboardInterrupt:
        print('Shutting down!')
    
    finally:
        producer.flush()
        producer.close()


