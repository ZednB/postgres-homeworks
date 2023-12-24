"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv

import psycopg2
import csv

con = psycopg2.connect(host='localhost', database='north', user='postgres', password='1111')
try:
    with con:
        with con.cursor() as cur:
            with open('north_data/employees_data.csv', 'r', encoding='utf-8') as csv_file:
                reader = csv.DictReader(csv_file)
                for read in reader:
                    cur.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)",
                                (read['employee_id'],
                                 read['first_name'],
                                 read['last_name'],
                                 read['title'],
                                 read['birth_date'],
                                 read['notes']))

            with open('north_data/customers_data.csv', 'r', encoding='utf-8') as csv_file:
                reader = csv.DictReader(csv_file)
                for read in reader:
                    cur.execute("INSERT INTO customers VALUES (%s, %s, %s)",
                                (read['customer_id'],
                                 read['company_name'],
                                 read['contact_name']
                                 ))

            with open('north_data/orders_data.csv', 'r', encoding='utf-8') as csv_file:
                reader = csv.DictReader(csv_file)
                for read in reader:
                    cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)",
                                (read['order_id'],
                                 read['customer_id'],
                                 read['employee_id'],
                                 read['order_date'],
                                 read['ship_city']
                                 ))

finally:
    con.close()
