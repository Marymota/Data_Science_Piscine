# Data Science Piscine - Documentation

# Data Engineer
## EX0: Environment Setup

To get the environment up and running, use the following command:

```bash
docker compose up -d
```

### Docker compose
The ex00 docker compose file includes an environmental variable to enforce password:
* **POSTGRES_INITDB_ARGS**: "--auth=md5 --auth-local=md5"


### Configuration (.env)
You must create a `.env` file in the root directory with the following variables:

* **POSTGRES_USER**: Your student login.
* **POSTGRES_DB**: `piscineds`
* **POSTGRES_PASSWORD**: `mysecretpassword`
* **EMAIL**: Your email address.

---

## EX1: PSQL & pgAdmin

### 1. Connect via PSQL
Run the following command to connect to the database from your terminal:

```bash
docker exec -it postgres psql -U student_login -d piscineds -h localhost -W
```

> **Note:** While `-W` prompts for a password, it may not be required as the user is considered "safe" within the container environment.

### 2. Connect via pgAdmin
Access the pgAdmin interface at [http://localhost:8080/](http://localhost:8080/) and register a new server with these details:

* **Host name/address**: Use the container IP (typically `172.18.0.2` or `172.18.0.3`).
    * *To find the IP:* `docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' postgres`
* **Port**: `5432`
* **Maintenance database**: `postgres`
* **Username**: `POSTGRES_USER`
* **Password**: `PGADMIN_PASSWORD`

---

## EX2: Docker & Manual Table Creation

### File Management
**Copy a folder from Host to Container:**
```bash
docker cp ./customer postgres:/tmp/customer
```

**Access the container shell:**
```bash
docker exec -it postgres sh
```

### Creating Tables Manually
1.  **Directly from Host to PSQL:**
    ```bash
    docker exec -i -e PGPASSWORD='password' postgres psql -U login_name -d piscineds < table.sql
    ```

2.  **Inside the Container:**
    ```bash
    psql -U login_name -d piscineds -f /tmp/customer/table.sql
    ```
    *inside the psql interactive shell:*
    ```sql
    \i /tmp/customer/table.sql
    ```

**View Tables in pgAdmin:**
Navigate to: `Servers` -> `ServerName` -> `Databases` -> `Schemas` -> `Tables`

---

## EX3 - EX4: Automated Table Management

### Data Loading
1. Unzip your data files and ensure the contents are moved to the root project folder.
2. Run the automation script:
   ```bash
   python3 automatic_table.py
   ```

**Bulk Import via PSQL:**
```sql
\copy data_2022_oct(event_time, event_type, product_id, price, user_id, user_session) FROM '/tmp/data_2022_oct.csv' DELIMITER ',' CSV HEADER;
```

### Table Merging (UNION ALL)
To merge multiple tables into a single final table:
```python
union_query = f"""
CREATE TABLE "{final_name}" AS
{" UNION ALL ".join([f'SELECT * FROM "{table}"' for table in table_names])};
"""
```

### Validation
To confirm the number of rows in your customers table:
```sql
SELECT COUNT(*) FROM customers; -- Expected: 20,692,840
```
#  Piscine datascience 1 - Data Warehouse
## Exercice 00: create tables
Successfully copied 380MB to postgres:/tmp/data_2023_feb.csv:
    ```docker cp ./data_2023_feb.csv postgres:/tmp/data_2023_feb.csv```
    ```docker exec -it postgres psql -U your_login -d piscineds -W```
    ```CREATE TABLE data_2023_feb (
        event_time TIMESTAMP,
        event_type VARCHAR,
        product_id INT,
        price FLOAT,
        user_id INT,
        user_session UUID
    );

    \copy data_2023_feb(event_time, event_type, product_id, price, user_id, user_session)
    FROM '/tmp/data_2023_feb.csv' DELIMITER ',' CSV HEADER;
```
## Exercice 01: customers table
The script connects to a PostgreSQL database, scans the schema for any tables starting with the prefix data_20 (representing different months like data_2022_oct, data_2023_jan, etc.), and uses a SQL command to merge them all into one master table named customers.
```sql
union_query = f"""
    CREATE TABLE "{final_table_name}" AS
    {" UNION ALL ".join([f'SELECT * FROM "{table}"' for table in table_list])};
"""
```
### Merging tables into costumers table
    INSERT INTO customers
    SELECT * FROM data_2023_feb;

**Count: 16536158**
---

## Exercice 02: remove duplicates
This script performs data cleaning operation. It uses a temporary table and identifies and removes duplicate entries in the customers table based on specific event criteria, then replaces the original table with the cleaned version.
```sql
-- first try
LAG(event_time) OVER (
    PARTITION BY event_type, product_id, price, user_id, user_session
    ORDER BY event_time
) AS prev_time
-- second try
SELECT DISTINCT 
    event_time, 
    event_type, 
    product_id, 
    price, 
    user_id, 
    user_session 
FROM {table_name};
```
**Count: 15337305** (with only LAG) ✔️
**Count: 15667350** (with only DISTINCT) 

*** Later I discovered that I should have removed 'user_session' from the "LAG(...) PARTITION BY (...)" because it causes errors in the DS2 ex01 


### Test
```sql
-- Test 1 — No exact duplicates remain:
SELECT event_type, price, event_time, user_id, product_id, user_session, COUNT(*)
FROM customers
GROUP BY event_type, price, event_time, user_id, product_id, user_session
HAVING COUNT(*) > 1;
-- Must return 0 rows
-- Test 2 — No near-duplicates within 1 second remain:
SELECT a.event_time, a.event_type, a.product_id, a.user_id
FROM customers a
JOIN customers b
  ON  a.event_type    = b.event_type
  AND a.product_id    = b.product_id
  AND a.user_id       = b.user_id
  AND a.user_session  = b.user_session
  AND a.event_time    < b.event_time
  AND EXTRACT(EPOCH FROM (b.event_time - a.event_time)) <= 1;
-- Must return 0 rows
-- Test 3 - From evaluation sheet
  SELECT * FROM public.customers
  WHERE product_id = 5802443 AND event_type = 'remove_from_cart'
  ORDER BY event_time ASC;
```
---
## Exercice 03: fusion
This script performs a fusion of two tables adding new columns and values to the first table
based on product_id.
```sql
    LEFT JOIN {table_items}
    ON {table_customers}.product_id = {table_items}.product_id;
```
**Count: 15331407** (same as the previous exercise)
---


# Piscine datascience 2 - Data Viz

On this module we will be visualizing and analyzing the costumers data. 
This is the normal process by which we do "Customer Segmentation" using RFM (Recency, Frequency, Monetary) analysis and K-Means clustering.

On Ex00 we can visualize the distribution of the different event types (page_view, add_to_cart, purchase, remove_from_cart) using a pie chart. This gives us an overview of how customers interact with the site.
On Ex01 we analyze how the number of costumers, sales and average spending per costumer evolve over time using line charts. This helps us identify customer behavior changes and trends over time.
On Ex02 we analyze the price distribution of the items purchased using box plots. This allows us to understand the range of prices customers are paying, identify outliers and get insights into the average spending habits of customers.
On Ex03 we perform RFM analysis to segment customers based on their recency, frequency and monetary value. We then visualize the average RFM values for each segment using bar charts, which helps us understand the characteristics of each customer segment and tailor marketing strategies accordingly.
On Ex04 we apply K-Means clustering to further segment customers based on their RFM values. We visualize the clusters using scatter plots, which allows us to see how customers are grouped based on their purchasing behavior and identify distinct customer segments for targeted marketing efforts.

## Ex00
This script creates a pie chart of the actions costumers take on the site.
```sql
    SELECT event_type, COUNT(*) AS total
    FROM customers
    GROUP BY event_type
    ORDER BY total DESC
```
# Query: We count the total number of occurencies for each event_type in the customers table.

``` text
    fig, ax = plt.subplots() 
    ax.pie(
        # The values to be plotted, labels, show percentage explode parameter...
    )
```
![alt text](image.png)

***Output:***
Pie chart - Percentage of ocurrence of each activity
The pie chart shows that the majority of events are page views, followed by add to cart, remove from cart and purchases. This indicates that while many customers are browsing products (50.3%), only a bit more than half (28.6%) is added to the cart and then half of it is removed from the cart after with only a small percentage (6.7%) being actually purchased.
***Insight:***
The quantity of items removed from the cart after being added can be a red flag that indicates problems with the checkout or shipping conditions.

## Ex01
This script creates a line chart showing the number of events per day for each event type.
```sql
    SELECT event_time::date AS date, event_type, COUNT(*) AS total
    FROM customers
    GROUP BY date, event_type
    ORDER BY date ASC
```
 # Query: groups the events by date and event type, counting the total number of events for each combination. The results are ordered by date in ascending order.


```python
    fig, ax = plt.subplots()
    for event_type in df['event_type'].unique():
        subset = df[df['event_type'] == event_type]
        ax.plot(subset['date'], subset['total'], label=event_type)
    ax.set_xlabel('Date')
    ax.set_ylabel('Total Events')
    ax.set_title('Total Events per Day by Event Type')
    ax.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
```
# Number of costumers
![alt text](image-1.png)
# Number of sales
![alt text](image-2.png)
# Average spending costumer
![alt text](image-3.png)

***Output:***

1. Line graph - How activity changes daily/monthly/yearly
Shows the number of customers on the site from October to February. The fluctuations show that the higher picks are at the second half of Novembre while the lower picks are at the end of December and early January.

2. Bar graph - column chart - comparing categories
Shows the total value of sales during each month. It shows that November had the most sales valued at 1.2 millions while december had the lowest sales, less than 0.9 millions.

3. Area chart - focuses on trend and magnitude/volumes
How much customers spend during the different months. It shows that the average spending from customers has some picks during november and lowers below the normal range at the end of December.

***Insight:***
Sales remained relatively stable at around 1 million during October, January, and February. However, they have increased in November, which may be linked to promotional campaigns, seasonal discounts, or events such as Black Friday that encouraged customers to spend more.
In contrast, December experienced a decline in sales. This could be influenced by holiday-related factors, where customers may have reduced online activity, shifted spending toward other retailers, or prioritized holiday travel and expenses.
Overall, the data suggests that seasonal promotions and holiday periods have a noticeable impact on customer purchasing behavior and average spending patterns.

## ex02
The script prints the mean, median, min, max, first, second and third quartile of the price of
the items purchased and displays them as box plots with the price of the items purchased.
```sql
    SELECT price
    FROM customers
    WHERE event_type = 'purchase'
```
```python
    fig, ax = plt.subplots()
    ax.boxplot(df['price'], vert=False)
    ax.set_title('Box Plot of Purchase Prices')
    ax.set_xlabel('Price')
    plt.tight_layout()
```
### Print statistics
![alt text](image-4.png)
### Price box plot of the items purchased
![alt text](image-5.png)
### Price box plot of the items purchased without outliers
![alt text](image-6.png)
###  Box plot with the average basket price per user
![alt text](image-7.png)

***Output***: 
1. 
- Box-and-Whisker Plot - Show the distribution and outliers of values
- Interquartile Range (IQR): This is where the middle 50% of the prices sit. Since the box is very thin and located near 0, it tells us that the vast majority of the items are priced very low.
- The Median: The line inside the box represents the median price.
- The "Whiskers": The lines extending from the box show the range of the bulk of the data
- The Diamonds (Outliers): The green diamond shapes scattered across the chart are outliers. These represent data points that are significantly higher or lower than the rest.
2. Box plot without outliers
- The Median line indicates that half the products are positioned below ~3 in the price range and half of them are above it.
- IQR: The green box shows that 50% of all items are in the range between 2₳ and 5₳.
3. Box Plot - Represents average basket price per user
- The Median: Half the users have a basket average of less or higher than ~28₳.
- IQR: 50% of the baskets are between ~15₳ and ~45₳. 75% of baskets are less than 45₳ with some outliers above 100₳.

***Insight***:
- Heavy Positive Skew:Massive cluster of data near 0, but a long trail of outliers stretching all the way to 300+. This suggests that the website sells mostly inexpensive items, with a few "premium" expensive ones.
- Anomalies or Errors: On the left side there are outliers below 0 (around -75 and -50) and there should not be negative prices. This may indicate data errors such as refunds or system glitches.
- Data Density: The thick "smudge" of green diamonds until 200 indicates that while these are outliers compared to the cheapest products, there is still a very large group of mid-priced items.
The three plots show that although most items bought are inexpensive, customers buy in bulk instead of 1-2 products at a time.

## Ex03 
This script creates bar charts showing the frequency of orders by customers and how much they spend.
This are part of a RFM Analysis (Recency, Frequency, Monetary)

***Output***: 
1. Histogram showing that most customers (70000+) shopped from 0 to 10 times, and that less than 20000 shop more than 20 times.
2. Histogram showing that most customers (75000) spent less than 50₳ and that only around 7000 customers spent more than 100₳.

***Insight***:
With this exercises we can create the Business Profile of the company being analysed.
1. Product: Very affordable items (Median: 3₳)
2. Shopping: Customers buy in bulk (Median Basket: 28₳)
3. Retention: Most customers buy a large bulk of cheap items (spending under 50₳ total), and don't return frequently.

## Ex04
This script creates an Elbow Method to be able to know the optimal number of clusters to group customer types
Elbow Method: Tool used to find the best number of clusters. It runs multiple times incrementing the number of clusters.
K-Means algorithm: Used to find the optimal number of clusters
Inertia: Inertia measures the distance between the center of a cluster and the points inside it. High inertia means that clusters are a mess because the spending habits (SUM(price)) vary too much within the group. When the inertia drops, it means there are a group of people who spend similar amounts of money.

***Output***: 
A Line Chart representing the Elbow Method. It shows the relationship between the number of clusters and the Inertia (the measure of how well the data points are grouped).

***Insight***:
The chart demonstrates that 3 clusters is the "optimal" number of clusterings. This is where the "elbow" of the curve is. Between 1 and 3 clusters, we see a massive drop in Inertia (from ~225,000 to ~75,000), meaning the groups are becoming much more distinct. Beyond that the curve flattens out. This confirms that adding more segments is unecessary.






## Useful Docker Commands

**Stop all running containers:**
```bash
docker stop $(docker ps -a -q)
```

**Remove all containers:**
```bash
docker rm $(docker ps -a -q)
```

**Remove container and volumes:**
```bash
docker compose down -v
```

---

## Resources

* [PostgreSQL + pgAdmin + Docker Guide](https://medium.com/@marvinjungre/get-postgresql-and-pgadmin-4-up-and-running-with-docker-4a8d81048aea)
* [Connecting PostgreSQL with Python](https://neon.com/postgresql/postgresql-python/connect)
* [Python OS Library Snippets](https://www.pythonforbeginners.com/code-snippets-source-code/python-os-listdir-and-endswith)
* [Importing Data to Pandas DataFrames](https://medium.com/@alestamm/importing-data-from-a-postgresql-database-to-a-pandas-dataframe-5f4bffcd8bb2)
* [Pandas: Drop Duplicates Documentation](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.drop_duplicates.html)
* https://github.com/VulpesDev/DataPiscineNotebook/blob/main/src/DataAnalyst.ipynb