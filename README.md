# EX0
---------ENVIRONMENT------------
$ docker compose up

  POSTGRES_USER: ${POSTGRES_USER}
  POSTGRES_DB: ${POSTGRES_DB}
  POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
  ***   Add .env file:                                    ***
  ***       •The username must be your student login.     ***
  ***       •The name of the database must be piscineds.  ***
  ***       •The password must be mysecretpassword.       ***
  ***       •An Email                                     ***

# EX1
---------PSQL & PGADMIN---------------
1. Connect to PSQL:
  $ docker exec -it postgres psql -U student_login -d piscineds -h localhost -W
    *** ($ docker exec -it container psql -U student_login -d database -h localhost -W) ***
    *** Password is not required because we are already inside a container so we are consedered a "safe" user ***

2. Go to pgadmin and connect a server:
Go to http://localhost:8080/ (Pgadmin container) and connect the DB:
  • Host name/address: ***** (docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' postgres)
    |-> (normaly 172.18.0.3 or 172.18.0.2 )
  •Port: 5432
  •Maintenance database: postgres
  •Username: POSTGRES_USER
  •Password: PGADMIN_PASSWORD

# EX2
-------DOCKER ------------------------
1. Copy folder from Host to Docker container temporary files:
  $ docker cp ./customer postgres:/tmp/costumer
2. Connect to Docker container:
  $ docker exec -it postgres sh

  *** $ docker cp ./my_folder container_name:/tmp/my_folder ***


-------POSTGRES TABLE MANUALY ------------------------
Create a table based on the CSV file:
  Create an sql table (customer/table.sql). Send it to the docker container:
    $ docker exec -i postgres psql -U student_login -d piscineds -h localhost -f - < customer/table.sql
    
  Copy file from host to the container:
    $ docker cp ./my_folder container_name:/tmp/my_folder
  Create a table:
    $ psql -U login_name -d piscineds -f /tmp/costumer/table.sql
  or: 
    $ psql -U login_name -d piscineds
    $ piscineds=# \i /tmp/costumer/table.sql

  See table in PGAdmin:
    Servers -> ServerName -> Databases -> schemas -> tables

# EX3 - EX4
- Unzip file and remove subject folder leaving only its contents
- Run python script (automatic_table.py)

------COMPLETE TABLE AUTO ----------------------------
\copy data_2022_oct(event_time, event_type, product_id, price,user_id, user_session) FROM 'home/student_login/Documents/workstation/DataScience-0/piscineds/ex02/data_2022_oct.csv' DELIMITER ',' CSV HEADER;

------CREATE TABLE by UNION ALL
by joining all the SELECT tables from a list of tables
union_query = f"""
  CREATE TABLE "{final_name}" AS
  {" UNION ALL ".join([f'SELECT * FROM "{table}"' for table in table names])};
 
------Command to confirm customers file numbers 
SELECT COUNT(*) FROM customers; (20692840)

----------DOCKER CMDs----------------
Stop all the containers
$ docker stop $(docker ps -a -q)
Remove all the containers
$ docker rm $(docker ps -a -q)

Resources:
POSTGRES+PAGEADMIN+DOCKER - https://medium.com/@marvinjungre/get-postgresql-and-pgadmin-4-up-and-running-with-docker-4a8d81048aea
CONNECT POSTGRESQL+PYTHON - https://neon.com/postgresql/postgresql-python/connect
GETTING ALL THE CSV FILES - https://www.pythonforbeginners.com/code-snippets-source-code/python-os-listdir-and-endswith
IMPORTING PG TABLES - https://medium.com/@alestamm/importing-data-from-a-postgresql-database-to-a-pandas-dataframe-5f4bffcd8bb2
                      https://www.geeksforgeeks.org/how-to-insert-a-pandas-dataframe-to-an-existing-postgresql-table/
REMOVE DUPLICATES - https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.drop_duplicates.html

Apache Parquet
polars

