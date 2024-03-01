
from kafka import KafkaConsumer
import json
import datetime
import os
import time

Consumer = KafkaConsumer(
            "Order3",
            bootstrap_servers ='localhost:9092',
            auto_offset_reset = 'earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_commit_interval_ms = 3000
)

def generate_report(report_data, date):

    report_filename = f'report_{date}.txt'

    with open (report_filename, 'w') as report_file:
        report_file.write(f"Date: {date}\n")
        report_file.write(f"number of orders: {report_data['number_of_orders']}\n")
        report_file.write(f"total sales: {report_data['total_sales']}\n")
        report_file.write("quantity sold per product\n")
        for product_name, quantity in report_data['quantity_sold'].items():
            report_file.write(f"{product_name}:{quantity}\n")
        
    
def process_order(order, report_data, date):

    order_time = datetime.datetime.strptime(order['order_time'], "%m/%d/%Y-%H:%M:%S")
    order_date = order_time.date()

    if order_date == date:
            for product in order['order_details']:
                    price = product['price']
                    quantity = product['quantity']
                    report_data['total_sales'] += price* quantity

                    product_name = product['product_name']
                    report_data['quantity_sold'][product_name] = report_data['quantity_sold'].get(product_name, 0) + quantity
            report_data['number_of_orders'] += 1

while True:
                        
    try:
        current_time = datetime.datetime.now()
        time_remaining = datetime.datetime.combine(current_time.date(), datetime.time(0, 0, 0)) - current_time
        
        report_data = {
                    'number_of_orders': 0,
                    'total_sales': 0,
                    'quantity_sold': {},
                }

        if current_time.hour == 0 and current_time.minute == 0:
            desired_date = current_time.date() - datetime.timedelta(days=1)
            for message in Consumer:
                order = message.value
                
                process_order(order, report_data, desired_date)
                if report_data['number_of_orders']>0:
                        generate_report(report_data,desired_date)
                        os.system('clear')
                        print("Report generated successfully")
        else:
                hours, remainder = divmod(time_remaining.seconds, 3600)
                minutes, _ = divmod(remainder, 60)
                print(f"Next report in {hours} hours, {minutes} minutes")
        
        time.sleep(60)

    except KeyboardInterrupt:
        print("Script stopped by user")
    except Exception as e:
        print("An error occurred:", e)
    finally:
        Consumer.close()