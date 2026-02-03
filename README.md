# Data Science Piscine - Documentation

# Data Engineer
## EX0: Environment Setup

To get the environment up and running, use the following command:

```bash
docker compose up
```

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
    docker exec -i postgres psql -U student_login -d piscineds -h localhost -f - < customer/table.sql
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

---

## Useful Docker Commands

**Stop all running containers:**
```bash
docker stop $(docker ps -a -q)
```

**Remove all containers:**
```bash
docker rm $(docker ps -a -q)
```

---

## Resources

* [PostgreSQL + pgAdmin + Docker Guide](https://medium.com/@marvinjungre/get-postgresql-and-pgadmin-4-up-and-running-with-docker-4a8d81048aea)
* [Connecting PostgreSQL with Python](https://neon.com/postgresql/postgresql-python/connect)
* [Python OS Library Snippets](https://www.pythonforbeginners.com/code-snippets-source-code/python-os-listdir-and-endswith)
* [Importing Data to Pandas DataFrames](https://medium.com/@alestamm/importing-data-from-a-postgresql-database-to-a-pandas-dataframe-5f4bffcd8bb2)
* [Pandas: Drop Duplicates Documentation](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.drop_duplicates.html)
