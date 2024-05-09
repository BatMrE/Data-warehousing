import os
import pandas as pd
import mysql.connector

# mydb1 = mysql.connector.connect(user='root', password='root',
#                                       host='127.0.0.1',
#                                       database='grocery_store')

# sql1 = "SELECT * FROM grocery_store.buyer;"
# mycursor1 = mydb1.cursor()
# mycursor1.execute(sql1)
# myresult1 = mycursor1.fetchall()


# mydb2 = mysql.connector.connect(user='root', password='root',
#                                       host='127.0.0.1',
#                                       database='convenience_store')
     
# sql2 = "SELECT * FROM convenience_store.customer;"
# mycursor2 = mydb2.cursor()
# mycursor2.execute(sql2)
# myresult2 = mycursor2.fetchall()


# df1 = pd.DataFrame()
# for x in myresult1:
#     df1_2 = pd.DataFrame(list(x)).T
#     df1 = pd.concat([df1, df1_2])
# print("df1, ", df1)


# df2 = pd.DataFrame()
# for x in myresult2:
#     df2_2 = pd.DataFrame(list(x)).T
#     df2 = pd.concat([df2, df2_2])
# print("df2 , ", df2)


# df.to_html('templates/sql-data.html', classes='styled-table')

# ------------------------------------------------------------------------------------------

import mysql.connector

# Connect to the first database
mydb1 = mysql.connector.connect(user='root', password='root',
                                host='127.0.0.1',
                                database='grocery_store')

# Create cursor object for the first database
cursor1 = mydb1.cursor()

delete_query = "DELETE FROM grocery_dataware_house.customer_dim"

# Execute the delete query
cursor1.execute(delete_query)

# Define the SQL query
sql_query = """
INSERT INTO grocery_dataware_house.customer_dim (customer_name, branch_id)
SELECT customer_name, 
       CASE 
           WHEN source = 'grocery_store' THEN 1 
           WHEN source = 'convenience_store' THEN 2 
           ELSE NULL 
       END AS branch_id
FROM (
    SELECT customer_name, 'grocery_store' AS source FROM grocery_store.buyer
    UNION
    SELECT customer_name, 'convenience_store' AS source FROM convenience_store.customer
) AS combined_customers;
"""

# Execute the query for the first database
cursor1.execute(sql_query)

# Commit the transaction
mydb1.commit()

# Close the cursor and database connection for the first database
cursor1.close()
mydb1.close()

print("Data inserted successfully into grocery_dataware_house.customer_dim table.")

# ------------------------------------------------------------------------------------------

import mysql.connector

# Connect to the first database
mydb1 = mysql.connector.connect(user='root', password='root',
                                host='127.0.0.1',
                                database='grocery_store')

# Create cursor object for the first database
cursor1 = mydb1.cursor()

delete_query = "DELETE FROM grocery_dataware_house.product_dim"

cursor1.execute(delete_query)
sql_query = """
INSERT INTO grocery_dataware_house.product_dim (name, unit_id, price_per_unit, branch_id)
SELECT name, units_id, price_per_unit, 
       CASE 
           WHEN source = 'grocery_store' THEN 1 
           WHEN source = 'convenience_store' THEN 2 
           ELSE NULL 
       END AS branch_id
FROM (
    SELECT name, units_id, price_per_unit, 'grocery_store' AS source FROM grocery_store.products
    UNION
    SELECT name, metric_id AS unit_id, price_per_unit, 'convenience_store' AS source FROM convenience_store.products
) AS combined_products;

"""
cursor1.execute(sql_query)
mydb1.commit()
cursor1.close()
mydb1.close()

print("Data inserted successfully into grocery_dataware_house.product_dim table.")


# ------------------------------------------------------------------------------------------

import mysql.connector

# Connect to the first database
mydb1 = mysql.connector.connect(user='root', password='root',
                                host='127.0.0.1',
                                database='grocery_store')

# Create cursor object for the first database
cursor1 = mydb1.cursor()

# delete_query = "DELETE FROM grocery_dataware_house.unit_dim"

# # Execute the delete query
# cursor1.execute(delete_query)

# Define the SQL query
sql_query = """
INSERT INTO grocery_dataware_house.unit_dim (unit_name, branch_id)
SELECT unit_name,
       CASE 
           WHEN source = 'grocery_store' THEN 1 
           WHEN source = 'convenience_store' THEN 2 
           ELSE NULL 
       END AS branch_id
FROM (
    SELECT units_name AS unit_name, 'grocery_store' AS source FROM grocery_store.units
    UNION
    SELECT metric_name AS unit_name, 'convenience_store' AS source FROM convenience_store.metric
) AS combined_units;


"""

# Execute the query for the first database
cursor1.execute(sql_query)

# Commit the transaction
mydb1.commit()

# Close the cursor and database connection for the first database
cursor1.close()
mydb1.close()

print("Data inserted successfully into grocery_dataware_house.unit_dim table.")


# ------------------------------------------------------------------------------------------

import mysql.connector


# Connect to the first database
mydb1 = mysql.connector.connect(user='root', password='root',
                                host='127.0.0.1',
                                database='grocery_store')

# Create cursor object for the first database
cursor1 = mydb1.cursor()

# delete_query = "DELETE FROM grocery_dataware_house.order_fact"

# # Execute the delete query
# cursor1.execute(delete_query)

# Define the SQL query
sql_query = """
INSERT INTO grocery_dataware_house.order_fact (order_id, customer_id, product_id, quantity, total_price, datetime, branch_id)
SELECT o.order_id, c.customer_id, od.product_id, od.quantity, od.total_price, o.datetime, 1 AS branch_id
FROM grocery_store.buyer o
JOIN grocery_store.order_details od ON o.order_id = od.order_id
JOIN grocery_dataware_house.customer_dim c ON o.customer_name = c.customer_name;
"""

# Execute the query for the first database
cursor1.execute(sql_query)

# Close the cursor and database connection for the first database
cursor1.close()
mydb1.close()

print("Data inserted successfully into grocery_dataware_house.order_fact1 table.")



mydb2 = mysql.connector.connect(user='root', password='root',
                                host='127.0.0.1',
                                database='grocery_store')
mydb2.commit()

cursor2 = mydb2.cursor()

# Define the SQL query
sql_query = """
INSERT INTO grocery_dataware_house.order_fact (order_id, customer_id, product_id, quantity, total_price, datetime, branch_id)
SELECT o.cust_id, c.customer_id, od.product_id, od.quantity, od.agg_price, o.datetime, 2 AS branch_id
FROM convenience_store.customer o
JOIN convenience_store.order_details od ON o.cust_id = od.cust_id
JOIN grocery_dataware_house.customer_dim c ON o.customer_name = c.customer_name;
"""

# Execute the query for the first database
cursor2.execute(sql_query)

# Commit the transaction
mydb2.commit()

# Close the cursor and database connection for the first database
cursor2.close()
mydb2.close()


print("Data inserted successfully into grocery_dataware_house.order_fact2 table.")
