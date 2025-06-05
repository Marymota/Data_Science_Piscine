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
--  docker exec -i postgres psql -U mmota -d piscineds < table.sql
--  docker exec -i postgres psql -U mmota -d piscineds -c "\COPY data_2022_oct FROM STDIN WITH (FORMAT csv, HEADER true)" < customer/data_2022_oct.csv