import mysql.connector

cnx = mysql.connector.connect(user = 'root', password = 'root',
                              host = '127.0.0.1',
                              database = 'convenience_store')

cursor = cnx.cursor()

query = "SELECT * FROM convenience_store.customer;"

cursor.execute(query)

for (product_id, name, metric_id, price_per_unit) in cursor:
    print(product_id, name, metric_id, price_per_unit)