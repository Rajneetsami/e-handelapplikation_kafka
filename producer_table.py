import sqlite3
import time

def read_data_from_file(filename):
    try:
        with open("product.txt",'r') as file:
            data = file.readlines()
        return data
    except FileNotFoundError:
        print("file not found", filename)
        return []


def parsed_data(data):
    parsed_data = []
    for line in data:
        line = line.strip().strip("(").strip(")").strip("\n").replace("'", "").split(",")
        parsed_data.append(tuple(line))
    return parsed_data

def create_table():
    try:
        conn = sqlite3.connect('products.db')
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS products
                        (product_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        product_name TEXT,
                        product_type TEXT,
                        price_type TEXT,
                        price INTEGER,
                        quantity INTEGER)''')
        conn.commit()
        print("Table 'products' created successfully.")
    except sqlite3.Error as e:
        print("error occur",e)
    finally:
        conn.close()

def insert_data_into_table(parsed_data):
    try:
        conn = sqlite3.connect('products.db')
        cursor = conn.cursor()
        for row in parsed_data:
            cursor.execute('''SELECT * FROM products
                            WHERE product_name = ?
                            AND product_type = ?
                            AND price_type = ?
                            AND price = ?
                            AND quantity = ?''', row)
            existing_data = cursor.fetchone()
            if not existing_data:
                cursor.execute('''INSERT INTO products ( product_name, product_type, price_type, price, quantity)
                            VALUES (?, ?, ?, ?, ?)''', row)
        print("Data inserted successfully.")
        conn.commit()
    except sqlite3.Error as e:
        print("an error occur", e)
    finally:
        conn.close()

def main_2():
    filename  =  'product.txt'
    print("Reading data from file...")
    time.sleep(.5)
    data = read_data_from_file(filename)
    if data:
        parsed_data_result = parsed_data(data)
        create_table()
        time.sleep(1)
        if parsed_data_result:
            insert_data_into_table(parsed_data_result)
            time.sleep(1)
        print("Process completed successfully.")
        time.sleep(1)
if __name__ == "__main__":
    main_2()
