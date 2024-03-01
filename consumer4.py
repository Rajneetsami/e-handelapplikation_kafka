from kafka import KafkaConsumer
import json
import sqlite3
import os

Consumer = KafkaConsumer(
        "Order3",
        bootstrap_servers ='localhost:9092',
        auto_offset_reset = 'earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_commit_interval_ms = 3000
)



def update_stock(quantity, product_id):
        conn = sqlite3.connect('products.db')
        cursor = conn.cursor()
        try:
                cursor.execute("UPDATE products SET quantity = quantity - ? WHERE product_id = ?", ( quantity, product_id,))
                conn.commit()

                cursor.execute("SELECT quantity FROM products WHERE product_id = ?", (product_id,))
                updated_quantity = cursor.fetchone()[0]

                print(f"Stock updated for product id {product_id}, Before quantity: {updated_quantity + quantity} Remaining quantity: {updated_quantity}")
        except sqlite3.Error as e:
                print("An error occurred while updating stock:", e)
        finally:
                conn.close()

def get_processed_order_id():
        try:
                if not os.path.exists("processed_order_id.txt"):
                        with open("processed_order_id.txt", "w") as file:
                                file.write("100")
                        order_id = 100
                else:
                        with open("processed_order_id.txt", "r") as file:
                                order_id = int(file.read().strip())
        except FileNotFoundError:
                order_id = 100
        return order_id

def update_processed_order_id(order_id):
        with open("processed_order_id.txt", "w") as file:
                file.write(str(order_id))
                print("processed order ID updated:", order_id)


try:
        for message in Consumer:
                order = message.value
                order_id = get_processed_order_id()
                order_id+=1
                if order['order_id']== order_id:
                        for product in order['order_details']:
                                product_id = product['product_id']
                                quantity = product['quantity']
                                print(f"\nstock has been updated for order id: {order_id}")
                                update_stock( quantity, product_id)
                        update_processed_order_id(order_id)
                else:
                        os.system('clear')
                        print(f"stock is already updated upto order id {get_processed_order_id()}")

except KeyboardInterrupt:
        print("Script stopped by user")
except Exception as e:
        print("An error occurred:", e)
finally:
        Consumer.close()
        

