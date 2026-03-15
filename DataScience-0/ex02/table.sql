DROP TABLE IF EXISTS data_2022_oct;
CREATE TABLE data_2022_oct (
    event_time TIMESTAMP WITH TIME ZONE,
    event_type VARCHAR(20),
    product_id INTEGER, 
    price DECIMAL(10,2),
    user_id BIGINT,
    user_session UUID
);

--  COMMANDS
--  docker exec -i -e PGPASSWORD='password' postgres psql -U my_login -d piscineds < table.sql
--  docker exec -i -e PGPASSWORD='password' postgres psql -U my_login -d piscineds -c "\COPY data_2022_oct FROM STDIN WITH (FORMAT csv, HEADER true)" < data_2022_oct.csv