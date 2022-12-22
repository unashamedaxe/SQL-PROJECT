#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().run_line_magic('pip', 'install psycopg2')


# In[2]:


import psycopg2
from psycopg2 import Error
from psycopg2.extras import NamedTupleCursor

def execute_query(query, fetch_result=False):
    try:
        connection = psycopg2.connect(
                                database="postgres", 
                                user='postgres',
                                password='UltraAxe2014', 
                                host='localhost', 
                                port='5432'
                     )
        connection.autocommit = True
        cursor = connection.cursor(cursor_factory=NamedTupleCursor)
        cursor.execute(query)
        if fetch_result:
            return cursor.fetchall()
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (connection):
            cursor.close()
            connection.close()


# In[3]:


sql_commands = ["CREATE DATABASE deliverydb;",
                "CREATE USER deliverydbuser with encrypted password 'deliverydb';",
                "GRANT ALL PRIVILEGES ON DATABASE deliverydb TO deliverydbuser;",
                "GRANT USAGE ON SCHEMA public TO deliverydbuser"]

for command in sql_commands:
    print(command)
    execute_query(command)


# In[39]:


sql_commands = ["DROP TABLE IF EXISTS restaurants;",
                "DROP TABLE IF EXISTS clients;",
                "DROP TABLE IF EXISTS orders;",
                "DROP TABLE IF EXISTS order_status;",
                "DROP TABLE IF EXISTS cards_info;",
                "DROP TABLE IF EXISTS payment_types;",
                "DROP TABLE IF EXISTS promocodes;",
                "DROP TABLE IF EXISTS menu;",
                "DROP TABLE IF EXISTS menu_dates;",
                "DROP TABLE IF EXISTS orders;",]

for command in sql_commands:
    print(command)
    execute_query(command)


query = """CREATE TABLE IF NOT EXISTS restaurants (
                id_rest INT NOT NULL PRIMARY KEY,
                rest_name VARCHAR(255) NOT NULL,
                rest_address  VARCHAR(255)  NOT NULL
                         
                  );
        """

execute_query(query)



query = """CREATE TABLE IF NOT EXISTS clients (
                id_client INT PRIMARY KEY, 
                client_name VARCHAR(255) NOT NULL,  
                phone VARCHAR(255) NOT NULL, 
                client_address VARCHAR(255) NOT NULL,  
                id_card INT  
                  );
        """

execute_query(query)



query = """CREATE TABLE IF NOT EXISTS orders (
                id_order INT PRIMARY KEY, 
                id_client INT NOT NULL, 
                id_payment_type INT NOT NULL,
                id_courier INT NOT NULL,
                id_card INT,
                price INT NOT NULL,
                id_rest INT NOT NULL, 
                id_food INT NOT NULL,
                cr_time TIMESTAMP NOT NULL, 
                id_status INT NOT NULL,
                id_code INT 
                  );
        """

execute_query(query)



query = """CREATE TABLE IF NOT EXISTS order_status (
                id_status INT PRIMARY KEY,
                status VARCHAR(255) NOT NULL
                  );
        """

execute_query(query)


query = """CREATE TABLE IF NOT EXISTS cards_info (
                id_card INT PRIMARY KEY, 
                card_number VARCHAR(255) NOT NULL,
                id_client VARCHAR(255) NOT NULL, 
                card_type VARCHAR(255) NOT NULL
                  );
        """

execute_query(query)



query = """CREATE TABLE IF NOT EXISTS couriers (
                id_courier INT PRIMARY KEY, 
                name VARCHAR(255) NOT NULL, 
                phone VARCHAR(255) NOT NULL, 
                id_rest INT NOT NULL,
                status VARCHAR(255) NOT NULL
                  );
        """

execute_query(query)



query = """CREATE TABLE IF NOT EXISTS payment_types (
                id_payment_type INT PRIMARY KEY, 
                name VARCHAR(255) NOT NULL
                  );
        """

execute_query(query)



query = """CREATE TABLE IF NOT EXISTS promocodes (
                id_code INT PRIMARY KEY,
                code VARCHAR(255) NOT NULL, 
                discount INT NOT NULL,
                cr_time TIMESTAMP NOT NULL,
                ending TIMESTAMP NOT NULL
                  );
        """

execute_query(query)



query = """CREATE TABLE IF NOT EXISTS menu (
                id_food INT NOT NULL,
                food_name VARCHAR(255) NOT NULL,
                food_price INT NOT NULL  
                  
                  );
        """

execute_query(query)



query = """CREATE TABLE IF NOT EXISTS menu_dates (
                id_food INT NOT NULL,
                sales_start TIMESTAMP NOT NULL,
                sales_finish TIMESTAMP NULL  
                  
                  );
        """

execute_query(query)






# In[40]:


query = """ INSERT INTO restaurants (id_rest, rest_name, rest_address) 
            VALUES 
                 (1, 'PizzaTime Atrium', 'Zemlyanoy Val, 33'),
                 (2, 'PizzaTime Rivera', 'Autozavodskaya street, 18'),
                 (3, 'PizzaTime CDM', 'Teatralniy drive, 5/2'),
                 (4, 'PizzaTime Aviapark', 'Hodinskiy boulevard., 4')
        """

execute_query(query)

query = """ INSERT INTO clients (id_client, client_name, phone, client_address, id_card) 
            VALUES 
                (1, 'Rumyantsev Ivan', '77008534415', 'Odintsovaya str. 11', 1),
                (2, 'Zykov Dmitriy', '77008534416', 'Ozernaya str. 53', 2),
                (3, 'Belova Kristina', '77008534417', 'Bakuleva str. 14', 3),
                (4, 'Shcherbakova Arina', '77008534418', 'Polskaya str. 10', NULL),
                (5, 'Korneeva Ekaterina', '77008534419', 'Odintsovaya str. 12', NULL),
                (6, 'Evdokimov Aleksey', '77008534420', 'Kirpichniy per. 9', 4),
                (7, 'Kuzmina Miya', '77008534421', 'Lesnaya str. 15', 5),
                (8, 'Kalmykov Robert', '77008534422', 'Petrovka str., 6', 6),
                (9, 'Nazarova Sofya', '77008534423', 'Tverskaya str., 24', 7),
                (10, 'Volkova Kseniya', '77008534424', 'Tihaya str., 2', 8)
        """

execute_query(query) 


query = """ INSERT INTO payment_types (id_payment_type, name) 
            VALUES 
                (1, 'card'),
                (2, 'cash')
                
            
        """

execute_query(query) 


query = """ INSERT INTO cards_info (id_card, card_number, id_client, card_type) 
            VALUES 
                (1, '1234567890123456', 1, 'Visa'),
                (2, '2345678901234567', 2, 'MasterCard'),
                (3, '3456789012345678', 3, 'Mir'),
                (4, '4567890123456789', 6, 'Mir'),
                (5, '5678901234567890', 7, 'Visa'),
                (6, '6789012345678901', 8, 'MasterCard'),
                (7, '7890123456789012', 9, 'Mir'),
                (8, '8901234567890123', 10, 'Visa')
               
            
        """

execute_query(query) 


query = """ INSERT INTO menu (id_food, food_name, food_price) 
            VALUES 
                (1, 'pizza_cheese', 350),
                (2, 'pizza_pepperoni', 400),
                (3, 'pizza_roman', 450),
                (4, 'pizza_4season', 479),
                (5, 'pizza_belissimo', 550),
                (6, 'pizza_doublecheese', 390)  
        """

execute_query(query) 



query = """ INSERT INTO menu_dates (id_food, sales_start, sales_finish) 
            VALUES 
                (1, to_timestamp('15-10-2000', 'dd-mm-yyyy'), NULL),
                (2, to_timestamp('15-10-2000', 'dd-mm-yyyy'), NULL),
                (3, to_timestamp('15-10-2000', 'dd-mm-yyyy'), NULL),
                (4, to_timestamp('1-12-2021', 'dd-mm-yyyy'), to_timestamp('21-12-2022', 'dd-mm-yyyy')),
                (5, to_timestamp('15-10-2000', 'dd-mm-yyyy'), NULL),
                (6, to_timestamp('15-10-2000', 'dd-mm-yyyy'), NULL)  
        """

execute_query(query) 



query = """ INSERT INTO promocodes (id_code, code, discount, cr_time, ending) 
            VALUES 
                (1, 'easy25', 25, to_timestamp('01-11-2021', 'dd-mm-yyyy'), to_timestamp('25-11-2021', 'dd-mm-yyyy')),
                (2, 'happynewyear', 15, to_timestamp('25-12-2021', 'dd-mm-yyyy'), to_timestamp('07-01-2022', 'dd-mm-yyyy')  ),
                (3, 'promo17', 17, to_timestamp('01-11-2021', 'dd-mm-yyyy'), to_timestamp('25-11-2021', 'dd-mm-yyyy'))
                
            
        """

execute_query(query) 


query = """ INSERT INTO couriers (id_courier, name, phone, id_rest, status) 
            VALUES 
                (1, 'Morozov Nikita', '77008544411', 1, 'busy'),
                (2, 'Yudin Yaroslav', '77008544412', 1, 'ready'),
                (3, 'Lazarev Maksim', '77008544413', 2, 'busy'),
                (4, 'Romanov Aleksandr', '77008544414', 2, 'busy'),
                (5, 'Akimov Daniil', '77008544415', 3, 'ready'),
                (6, 'Ivanova Anna', '77008544416', 3, 'ready'),
                (7, 'Grishin Vladislav', '77008544417', 4, 'ready'),
                (8, 'Kolesnikova Marta', '77008544418', 4, 'busy')
                
                
        """

execute_query(query) 
                                                                                                        #cr_time = now() - interval('')

query = """ INSERT INTO orders (id_order, id_client, id_payment_type, id_courier, id_card, price, id_rest, id_food, cr_time, id_status, id_code) 
            VALUES 
                (1, 1, 1, 1, 1, 550, 1, 5, to_timestamp('14-12-2021 11:36:38', 'dd-mm-yyyy hh24:mi:ss'), 1, NULL),
                (2, 1, 1, 1, 1, 400, 1, 2, to_timestamp('19-5-2022 17:45:11', 'dd-mm-yyyy hh24:mi:ss'), 1, NULL),
                (3, 1, 1, 2, 1, 400, 1, 2, to_timestamp('17-7-2022 12:32:11', 'dd-mm-yyyy hh24:mi:ss'), 1, NULL),
                (4, 2, 1, 2, 2, 300, 1, 2, to_timestamp('16-11-2021 17:45:32', 'dd-mm-yyyy hh24:mi:ss'), 1, 1),
                (5, 4, 2, 5, NULL, 479, 3, 4, to_timestamp('17-12-2022 20:05:54', 'dd-mm-yyyy hh24:mi:ss'), 1, NULL),
                (6, 5, 2, 7, NULL, 350, 4, 1, to_timestamp('10-12-2022 10:12:32', 'dd-mm-yyyy hh24:mi:ss'), 1, NULL),
                (7, 7, 1, 3, 5, 390, 2, 6, to_timestamp('19-12-2021 17:24:42', 'dd-mm-yyyy hh24:mi:ss'), 2, NULL),
                (8, 7, 1, 4, 5, 450, 2, 3, to_timestamp('19-12-2022 15:45:10', 'dd-mm-yyyy hh24:mi:ss'), 2, NULL),
                (9, 8, 1, 6, 6, 340, 3, 2, to_timestamp('02-01-2022 12:25:43', 'dd-mm-yyyy hh24:mi:ss'), 1, 2),
                (10, 9, 1, 2, 7, 400, 1, 2, to_timestamp('16-7-2022 14:20:11', 'dd-mm-yyyy hh24:mi:ss'), 1, NULL),
                (11, 10, 1, 8, 8, 350, 4, 1, to_timestamp('19-10-2021 12:45:11', 'dd-mm-yyyy hh24:mi:ss'), 1, NULL),
                (12, 10, 1, 8, 8, 550, 4, 5, to_timestamp('19-12-2022 17:55:23', 'dd-mm-yyyy hh24:mi:ss'), 2, NULL)
                
                
                
                
                
                
        """


execute_query(query) 



query =""" INSERT INTO order_status (id_status, status) 
            VALUES 
                (1, 'delivered'),
                (2, 'not delivered')
                
                
                
       """


execute_query(query) 


print(execute_query("select * from restaurants", fetch_result=True))
print(execute_query("select * from clients", fetch_result=True))
print(execute_query("select * from menu", fetch_result=True))
print(execute_query("select * from promocodes", fetch_result=True))
print(execute_query("select * from couriers", fetch_result=True))
print(execute_query("select * from orders", fetch_result=True))
print(execute_query("select * from cards_info", fetch_result=True))


# In[6]:


year_income = execute_query("""
                            SELECT SUM(price), DATE_PART('year', cr_time) AS year
                            FROM orders
                            GROUP BY year;
                            """, fetch_result=True)
print(year_income)


# In[7]:


notdelivered = execute_query("""
                             SELECT id_order
                             FROM orders
                             WHERE id_status = 2;
                             """, fetch_result=True)
print(notdelivered)


# In[8]:


courier_orders = execute_query("""
                               SELECT name, COUNT(orders.id_courier)
                               FROM orders JOIN couriers on couriers.id_courier = orders.id_courier
                               GROUP BY name;
                               """, fetch_result=True)
print(courier_orders)


# In[9]:


ordersbymonth = execute_query("""
                              SELECT id_order, DATE_PART('month', cr_time) as month
                              FROM orders
                              ORDER BY month ASC;
                              """, fetch_result=True)
print(ordersbymonth)


# In[10]:


ordersbymonth = execute_query("""
                              SELECT id_order
                              FROM orders
                              WHERE DATE_PART('month', cr_time) = 12;
                              """, fetch_result=True)
print(ordersbymonth)


# In[11]:


ordersbymonth = execute_query("""
                              SELECT COUNT(id_order), DATE_PART('month', cr_time) as month
                              FROM orders
                              GROUP BY month
                              ORDER BY month ASC;
                              """, fetch_result=True)
print(ordersbymonth)


# In[35]:


#за последние 7 дней количество заказов в день

last7daysorders = execute_query("""
                                SELECT DATE_PART('day', cr_time), COUNT(id_order)
                                FROM orders
                                WHERE cr_time >= now() - interval'7 days'
                                GROUP BY DATE_PART('day', cr_time)
                                
                                """, fetch_result=True)
print(last7daysorders)


# In[13]:


#самый прибыльный ресторан

bestrest = execute_query("""
                         SELECT rest_name, SUM(orders.price) as profit
                         FROM orders JOIN restaurants ON orders.id_rest = restaurants.id_rest
                         GROUP BY restaurants.rest_name
                         ORDER BY profit DESC
                         LIMIT 3
                         """, fetch_result=True)
print(bestrest)


# In[14]:


#курьер, который доставил больше всего заказов

bestcourier = execute_query("""
                            SELECT couriers.name, COUNT(orders.id_order) as ordersamount
                            FROM orders JOIN couriers ON orders.id_courier = couriers.id_courier
                            GROUP BY couriers.name
                            ORDER BY ordersamount DESC
                            LIMIT 1
                            """, fetch_result=True)
print(bestcourier)


# In[28]:


#способы оплаты по популярности

paymentsranked = execute_query("""
                               SELECT payment_types.name, COUNT(id_order)
                               FROM orders JOIN payment_types on payment_types.id_payment_type = orders.id_payment_type
                               GROUP BY payment_types.name
                               ORDER BY COUNT(id_order) DESC
                               
                               """, fetch_result=True)
print(paymentsranked)


# In[18]:


#все данные из таблицы заказов

all_rows = execute_query("""SELECT *
                            FROM ORDERS
                         """, fetch_result=True)
for row in all_rows:
    print(row)


# In[26]:


#процент заказов с промокодом за каждый день его действия (дата, кол-во заказов, кол-во с промокодом)

all_rows = execute_query("""
            SELECT DATE_TRUNC('day', cr_time) as day, COUNT(id_order) as order_count, COUNT(id_code) as code_count, COUNT(id_code)*1.0/COUNT(id_order) as percent
            FROM orders
            GROUP BY DATE_TRUNC('day', cr_time)
            ORDER BY day
        """, fetch_result=True)


for row in all_rows:
    print(row)


# In[27]:


query = """ INSERT INTO orders (id_order, id_client, id_payment_type, id_courier, id_card, price, id_rest, id_food, cr_time, id_status, id_code) 
            VALUES 
                
                (100, 2, 1, 2, 2, 300, 1, 2, to_timestamp('16-11-2021 17:45:32', 'dd-mm-yyyy hh24:mi:ss'), 1, NULL),
                (101, 2, 1, 2, 2, 300, 1, 2, to_timestamp('16-11-2021 17:45:32', 'dd-mm-yyyy hh24:mi:ss'), 1, NULL),
                (102, 2, 1, 2, 2, 300, 1, 2, to_timestamp('16-11-2021 17:45:32', 'dd-mm-yyyy hh24:mi:ss'), 1, NULL),
                (103, 2, 1, 2, 2, 300, 1, 2, to_timestamp('16-11-2021 17:45:32', 'dd-mm-yyyy hh24:mi:ss'), 1, NULL)

             
        """
execute_query(query) 


# In[30]:


#средний чек

avg_order = execute_query("""
                               SELECT AVG(price)
                               FROM orders 
                               
                               
                               """, fetch_result=True)
print(avg_order)


# In[68]:


#топ пицц по популярности, из тех, что в продаже

toppizza = execute_query("""   WITH actual_menu AS (
                                    SELECT *
                                    FROM menu JOIN menu_dates ON menu.id_food = menu_dates.id_food
                                    WHERE menu_dates.sales_finish IS NULL)
                               SELECT menu.food_name, COUNT(id_order)
                               FROM orders JOIN menu ON orders.id_food = menu.id_food
                               WHERE menu.id_food IN (
                                   SELECT menu.id_food
                                   FROM actual_menu
                               )
                               GROUP BY menu.food_name
                               ORDER BY COUNT(id_order) DESC
                               
                               """, fetch_result=True)
print(toppizza)


# In[ ]:





# In[ ]:




