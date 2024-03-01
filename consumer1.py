from kafka import KafkaConsumer
import json
from datetime import datetime
import os


Consumer = KafkaConsumer(
        "Order3",
        bootstrap_servers ='localhost:9092',
        auto_offset_reset = 'earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_commit_interval_ms = 3000
)

order_count = 0
start_of_day = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
start_date = start_of_day.date()


try:
        for message in Consumer:
                order = message.value
                order_time_str = order['order_time']
                order_time = datetime.strptime(order_time_str, "%m/%d/%Y-%H:%M:%S")
                order_date = order_time.date()
                        
                if order_date >= start_date:
                        order_count+=1
                        os.system('clear')
                        print("Today's order count is", order_count)
                else:
                        os.system('clear')
                        print("today's order is ", order_count)

except KeyboardInterrupt:
        print("Script stopped by user")
except Exception as e:
        print("An error occurred:", e)
finally:
        Consumer.close()


