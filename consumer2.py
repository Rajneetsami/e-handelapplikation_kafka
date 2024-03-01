from kafka import KafkaConsumer
import json
import datetime
import os


Consumer = KafkaConsumer(
            "Order3",
            bootstrap_servers ='localhost:9092',
            auto_offset_reset = 'earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_commit_interval_ms = 3000
)

last_hour_start =datetime.datetime.now()-datetime.timedelta(hours = 1)
start_of_day = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

total_sales_day = 0
total_sale_last_hour = 0

try:
    for message in Consumer:
        order = message.value
        products = order['order_details']
        order_time = datetime.datetime.strptime(order['order_time'], "%m/%d/%Y-%H:%M:%S")
        
        if order_time >= start_of_day:
            for product in products:
                price = product['price']
                quantity = product['quantity']
                total_sales_day += price * quantity
                
        if order_time >= last_hour_start:
                for product in products:
                    price = product['price']
                    quantity = product['quantity']
                    total_sale_last_hour += price * quantity
                    
                    
        os.system('clear')
        print("Total sales for the day:", total_sales_day)
        print("Total sales for the last hour:", total_sale_last_hour)

                
except KeyboardInterrupt:
    print("Script stopped by user")
except Exception as e:
    print("An error occurred:", e)
finally:
    Consumer.close()


