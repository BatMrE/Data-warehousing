# from flask import Flask, render_template
# import os
# import pandas as pd
# import mysql.connector

# app = Flask(__name__)

# mydb1 = mysql.connector.connect(user='root', password='root',
#                                       host='127.0.0.1',
#                                       database='grocery_store')

# sql1 = "SELECT * FROM grocery_store.buyer;"
# mycursor1 = mydb1.cursor()
# mycursor1.execute(sql1)
# myresult1 = mycursor1.fetchall()

# df1 = pd.DataFrame(myresult1, columns=mycursor1.column_names)

# mydb2 = mysql.connector.connect(user='root', password='root',
#                                       host='127.0.0.1',
#                                       database='convenience_store')
     
# sql2 = "SELECT * FROM convenience_store.customer;"
# mycursor2 = mydb2.cursor()
# mycursor2.execute(sql2)
# myresult2 = mycursor2.fetchall()

# df2 = pd.DataFrame(myresult2, columns=mycursor2.column_names)

# @app.route('/')
# def query_page():
#     return render_template('query_page.html', df1=df1, df2=df2)

# if __name__ == '__main__':
#     app.run(debug=True)


import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import mysql.connector
import pandas as pd
import subprocess

# subprocess.run(["python", r"D:\A_Raf\MS\Classes\DB\Project\python_projects_grocery_webapp-main\ui\query_results.py"], check=True)
path_trigg = r"D:\A_Raf\MS\Classes\DB\Project\python_projects_grocery_webapp-main\ui\query_results.py"

mydb1 = mysql.connector.connect(user='root', password='root',
                                      host='127.0.0.1',
                                      database='grocery_store')

sql1 = "SELECT * FROM grocery_store.buyer;"
mycursor1 = mydb1.cursor()
mycursor1.execute(sql1)
myresult1 = mycursor1.fetchall()

# df1 = pd.DataFrame(myresult1, columns=mycursor1.column_names)
df = pd.DataFrame(myresult1, columns=mycursor1.column_names)

# df = pd.read_csv(
#     'https://gist.githubusercontent.com/chriddyp/'
#     'c78bf172206ce24f77d6363a2d754b59/raw/'
#     'c353e8ef842413cae56ae3920b8fd78468aa4cb2/'
#     'usa-agricultural-exports-2011.csv')

def generate_table(dataframe, max_rows=10):
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
        ]) for i in range(min(len(dataframe), max_rows))]
    )

app = dash.Dash()

app.layout = html.Div(children=[
    html.H4(children='Queries for database and data warehouse'),
    dcc.Dropdown(id='dropdown', options=[
        {'label': i, 'value': i} for i in ['SELECT * FROM grocery_dataware_house.customer_dim', 'SELECT * FROM grocery_dataware_house.unit_dim',
                                           'SELECT * FROM grocery_dataware_house.product_dim', 'SELECT * FROM grocery_dataware_house.order_fact',
                                           'SELECT MONTH(datetime) AS month, SUM(total_price) AS total_sales FROM grocery_dataware_house.order_fact GROUP BY MONTH(datetime);',
                                           'SELECT product_id, SUM(quantity) AS total_quantity_sold FROM grocery_dataware_house.order_fact GROUP BY product_id ORDER BY total_quantity_sold DESC;',
                                           "SELECT DATE_FORMAT(b.datetime, '%Y-%m') AS month, SUM(b.total) AS total_sakes FROM grocery_store.buyer b GROUP BY month;",
                                           "SELECT grocery_store.products.product_id, grocery_store.products.name, grocery_store.products.units_id, grocery_store.products.price_per_unit, grocery_store.units.units_name FROM grocery_store.products INNER JOIN grocery_store.units ON grocery_store.products.units_id = grocery_store.units.units_id",
                                           "SELECT products.product_id, products.name, products.metric_id, products.price_per_unit, metric.metric_name FROM convenience_store.products INNER JOIN convenience_store.metric ON products.metric_id = metric.metric_id",
                                           "SELECT * FROM grocery_store.products",
                                           "SELECT * FROM convenience_store.order_details"]
    ], multi=True, placeholder='SELECT * FROM grocery_store.buyer;'),
    html.Div(id='table-container')
])

@app.callback(
    dash.dependencies.Output('table-container', 'children'),
    [dash.dependencies.Input('dropdown', 'value')])

def display_table(dropdown_value):
    subprocess.run(["python", path_trigg], check=True)

    if dropdown_value is None:
        return generate_table(df)

    mydb2 = mysql.connector.connect(user='root', password='root',
                                          host='127.0.0.1',
                                          database='convenience_store')
    
    if dropdown_value[0] == 'SELECT * FROM grocery_dataware_house.customer_dim':
        print("IN DB2222222222")
        sql2 = "SELECT * FROM grocery_dataware_house.customer_dim;"
        mycursor2 = mydb2.cursor()
        mycursor2.execute(sql2)
        myresult2 = mycursor2.fetchall()

        df2 = pd.DataFrame(myresult2, columns=mycursor2.column_names)
        return generate_table(df2)
        
    if dropdown_value[0] == 'SELECT * FROM grocery_dataware_house.unit_dim':
        print("IN DB2222222222")
        sql2 = "SELECT * FROM grocery_dataware_house.unit_dim;"
        mycursor2 = mydb2.cursor()
        mycursor2.execute(sql2)
        myresult2 = mycursor2.fetchall()

        df2 = pd.DataFrame(myresult2, columns=mycursor2.column_names)
        return generate_table(df2)
    
    if dropdown_value[0] == 'SELECT * FROM grocery_dataware_house.product_dim':
        print("IN DB2222222222")
        sql2 = "SELECT * FROM grocery_dataware_house.product_dim;"
        mycursor2 = mydb2.cursor()
        mycursor2.execute(sql2)
        myresult2 = mycursor2.fetchall()

        df2 = pd.DataFrame(myresult2, columns=mycursor2.column_names)
        return generate_table(df2)
    
    if dropdown_value[0] == 'SELECT * FROM grocery_dataware_house.order_fact':
        print("IN DB2222222222")
        sql2 = "SELECT * FROM grocery_dataware_house.order_fact ORDER BY index_order DESC;"
        mycursor2 = mydb2.cursor()
        mycursor2.execute(sql2)
        myresult2 = mycursor2.fetchall()

        df2 = pd.DataFrame(myresult2, columns=mycursor2.column_names)
        return generate_table(df2)
    

    if dropdown_value[0] == 'SELECT MONTH(datetime) AS month, SUM(total_price) AS total_sales FROM grocery_dataware_house.order_fact GROUP BY MONTH(datetime);':
        sql2 = "SELECT MONTH(datetime) AS month, SUM(total_price) AS total_sales FROM grocery_dataware_house.order_fact GROUP BY MONTH(datetime);"
        mycursor2 = mydb2.cursor()
        mycursor2.execute(sql2)
        myresult2 = mycursor2.fetchall()

        df2 = pd.DataFrame(myresult2, columns=mycursor2.column_names)
        return generate_table(df2)
    
    # most sold products
    if dropdown_value[0] == 'SELECT product_id, SUM(quantity) AS total_quantity_sold FROM grocery_dataware_house.order_fact GROUP BY product_id ORDER BY total_quantity_sold DESC;':
        sql2 = "SELECT product_id, SUM(quantity) AS total_quantity_sold FROM grocery_dataware_house.order_fact GROUP BY product_id ORDER BY total_quantity_sold DESC;"
        mycursor2 = mydb2.cursor()
        mycursor2.execute(sql2)
        myresult2 = mycursor2.fetchall()

        df2 = pd.DataFrame(myresult2, columns=mycursor2.column_names)
        return generate_table(df2)

    if dropdown_value[0] == "SELECT DATE_FORMAT(b.datetime, '%Y-%m') AS month, SUM(b.total) AS total_sakes FROM grocery_store.buyer b GROUP BY month;":
        sql2 = "SELECT DATE_FORMAT(b.datetime, '%Y-%m') AS month, SUM(b.total) AS total_sakes FROM grocery_store.buyer b GROUP BY month;"
        mycursor2 = mydb2.cursor()
        mycursor2.execute(sql2)
        myresult2 = mycursor2.fetchall()

        df2 = pd.DataFrame(myresult2, columns=mycursor2.column_names)
        return generate_table(df2)
    
    if dropdown_value[0] == "SELECT grocery_store.products.product_id, grocery_store.products.name, grocery_store.products.units_id, grocery_store.products.price_per_unit, grocery_store.units.units_name FROM grocery_store.products INNER JOIN grocery_store.units ON grocery_store.products.units_id = grocery_store.units.units_id":
        sql2 = "SELECT grocery_store.products.product_id, grocery_store.products.name, grocery_store.products.units_id, grocery_store.products.price_per_unit, grocery_store.units.units_name FROM grocery_store.products INNER JOIN grocery_store.units ON grocery_store.products.units_id = grocery_store.units.units_id;"
        mycursor2 = mydb2.cursor()
        mycursor2.execute(sql2)
        myresult2 = mycursor2.fetchall()

        df2 = pd.DataFrame(myresult2, columns=mycursor2.column_names)
        return generate_table(df2)

    if dropdown_value[0] == "SELECT products.product_id, products.name, products.metric_id, products.price_per_unit, metric.metric_name FROM convenience_store.products INNER JOIN convenience_store.metric ON products.metric_id = metric.metric_id":
        sql2 = "SELECT products.product_id, products.name, products.metric_id, products.price_per_unit, metric.metric_name FROM convenience_store.products INNER JOIN convenience_store.metric ON products.metric_id = metric.metric_id;"
        mycursor2 = mydb2.cursor()
        mycursor2.execute(sql2)
        myresult2 = mycursor2.fetchall()

        df2 = pd.DataFrame(myresult2, columns=mycursor2.column_names)
        return generate_table(df2)

    if dropdown_value[0] == "SELECT * FROM grocery_store.products":
        sql2 = "SELECT * FROM grocery_store.products;"
        mycursor2 = mydb2.cursor()
        mycursor2.execute(sql2)
        myresult2 = mycursor2.fetchall()

        df2 = pd.DataFrame(myresult2, columns=mycursor2.column_names)
        return generate_table(df2)
    
    if dropdown_value[0] == "SELECT * FROM convenience_store.order_details":
        sql2 = "SELECT * FROM convenience_store.order_details;"
        mycursor2 = mydb2.cursor()
        mycursor2.execute(sql2)
        myresult2 = mycursor2.fetchall()

        df2 = pd.DataFrame(myresult2, columns=mycursor2.column_names)
        return generate_table(df2)


    print("Noneeeeeeee ", dropdown_value)
    dff = df[df.state.str.contains('|'.join(dropdown_value))]
    return generate_table(df2)

app.css.append_css({"external_url": "https://codepen.io/chriddyp/pen/bWLwgP.css"})

if __name__ == '__main__':
    app.run_server(debug=True)


# import dash
# import dash_core_components as dcc
# import dash_html_components as html
# from dash.dependencies import Input, Output

# import pandas as pd
# import mysql.connector

# app = dash.Dash(__name__)

# # Function to fetch data from MySQL and create DataFrame
# def fetch_data(sql_query):
#     mydb = mysql.connector.connect(user='root', password='root',
#                                    host='127.0.0.1', database='grocery_store')
#     mycursor = mydb.cursor()
#     mycursor.execute(sql_query)
#     myresult = mycursor.fetchall()
#     df = pd.DataFrame(myresult, columns=mycursor.column_names)
#     return df

# # SQL queries
# sql_query1 = "SELECT * FROM grocery_store.buyer;"
# sql_query2 = "SELECT * FROM convenience_store.customer;"

# # Fetch data and create DataFrames
# df1 = fetch_data(sql_query1)
# df2 = fetch_data(sql_query2)

# # Dash app layout
# app.layout = html.Div([
#     html.H4(children='Data from MySQL Databases'),
#     dcc.Dropdown(
#         id='dropdown',
#         options=[
#             {'label': 'Query 1', 'value': 'query1'},
#             {'label': 'Query 2', 'value': 'query2'}
#         ],
#         placeholder='Select a query...',
#         multi=True
#     ),
#     html.Div(id='table-container')
# ])

# # Callback to update table based on dropdown selection
# @app.callback(
#     Output('table-container', 'children'),
#     [Input('dropdown', 'value')]
# )
# def update_table(selected_query):
#     if selected_query == 'query1':
#         return generate_table(df1)
#     elif selected_query == 'query2':
#         return generate_table(df2)
#     else:
#         return ''

# # Function to generate HTML table from DataFrame
# def generate_table(dataframe, max_rows=10):
#     return html.Table(
#         # Header
#         [html.Tr([html.Th(col) for col in dataframe.columns])] +

#         # Body
#         [html.Tr([
#             html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
#         ]) for i in range(min(len(dataframe), max_rows))]
#     )

# if __name__ == '__main__':
#     app.run_server(debug=True)
