Application Overview

The application simulates e-commerce sales by streaming fake orders to Apache Kafka.
Various consumers can process this data for different purposes, such as tracking sales, generating reports, and updating product inventory.

To Run
1. Create the topic "Orders" on your Kafka cluster.
2. Set up a virtual environment (recommended).
3. Install the requirements:
   ```bash
   pip install -r requirements.txt

1.1 Run producer.py using your preferred method (e.g., VS Code, terminal):

1.2 Additional Tasks: Creating consumers to process the orders for different purposes (e.g., tracking sales, generating reports, updating inventory).Implementing functionalities like daily reports and updating product inventory balances.For these tasks, you may need to write additional code to create consumers that subscribe to the "Orders" topic, process the incoming orders, and perform the required operations.


