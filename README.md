# DOCKER ACCESS

---------ENVIRONMENT------------  
  POSTGRES_USER: ${POSTGRES_USER}
  POSTGRES_DB: ${POSTGRES_DB}
  POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}

---------PSQL
docker exec -it postgres psql -U mmota -d piscineds -h localhost -W


----------PGADMIN---------------
Host name/address: ***** (docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' postgres)
(normaly 172.18.0.3 or 172.18.0.2 )
Port: 5432
Maintenance database: postgres
Username: postgres
Password: PGADMIN_PASSWORD

----------DOCKER CMD----------------
Stop all the containers
docker stop $(docker ps -a -q)
Remove all the containers
docker rm $(docker ps -a -q)

-------POSTGRES TABLE----------------------------MANUALY
docker exec -i postgres psql -U mmota -d piscineds -h localhost  -f - < ex02/table.sql

------COMPLETE TABLE------------------------------AUTO
\copy data_2022_oct(event_time, event_type, product_id, price,user_id, user_session) FROM 'home/mmota/Documents/workstation/DataScience-0/piscineds/ex02/data_2022_oct.csv' DELIMITER ',' CSV HEADER;

------CREATE TABLE by UNION ALL
by joining all the SELECT tables from a list of tables
union_query = f"""
  CREATE TABLE "{final_name}" AS
  {" UNION ALL ".join([f'SELECT * FROM "{table}"' for table in table names])};
 
------Command to confirm customers file numbers 
SELECT COUNT(*) FROM customers; (20692840)


Resources:
POSTGRES+PAGEADMIN+DOCKER - https://medium.com/@marvinjungre/get-postgresql-and-pgadmin-4-up-and-running-with-docker-4a8d81048aea
CONNECT POSTGRESQL+PYTHON - https://neon.com/postgresql/postgresql-python/connect
GETTING ALL THE CSV FILES - https://www.pythonforbeginners.com/code-snippets-source-code/python-os-listdir-and-endswith
IMPORTING PG TABLES - https://medium.com/@alestamm/importing-data-from-a-postgresql-database-to-a-pandas-dataframe-5f4bffcd8bb2
                      https://www.geeksforgeeks.org/how-to-insert-a-pandas-dataframe-to-an-existing-postgresql-table/
REMOVE DUPLICATES - https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.drop_duplicates.html

