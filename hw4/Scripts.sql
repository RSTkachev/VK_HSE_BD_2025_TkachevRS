CREATE DATABASE IF NOT EXISTS hw_4_database;
SHOW DATABASES;

CREATE TABLE hw_4_database.test_table (
    int_col UInt32,
    uuid_col UUID,
    dt_col DateTime,
    str_col String
) ENGINE = MergeTree()
ORDER BY int_col;

INSERT INTO hw_4_database.test_table
SELECT q.int_val, q.uuid_val, q.dt_val, q.str_val
FROM (
    SELECT modulo(rand(), 999) + 1 AS int_val,
           generateUUIDv4() AS uuid_val,
           now() - interval rand()/1000 second AS dt_val,
           rand() / 500_000 AS int_val_2,
           multiIf(int_val_2 <= 1500, 'A',
                   int_val_2 <= 3000, 'B',
                   int_val_2 <= 4500, 'C',
                   int_val_2 <= 6000, 'D',
                   int_val_2 <= 7300, 'E',
                   'F') AS str_val
    FROM numbers(10_000_000)
) q;


SELECT count(*)
FROM hw_4_database.test_table;

SELECT str_col,
       countDistinct(uuid_col) AS unique_uuids,
       sum(int_col) AS total_int
FROM hw_4_database.test_table
GROUP BY str_col;

SELECT * FROM system.clusters;
SELECT * FROM system.macros;
SELECT * FROM system.zookeeper WHERE path = '/';
SELECT * FROM system.distributed_ddl_queue;
SELECT * FROM system.replication_queue;
SELECT * FROM system.trace_log;

SELECT getMacro('replica');
SELECT * FROM clusterAllReplicas('default', system.one);

SELECT query, query_duration_ms, memory_usage 
FROM system.query_log 
WHERE type = 'QueryFinish'
ORDER BY event_time DESC
LIMIT 5;

SELECT
sum(bytes_on_disk) AS compressed,
sum(data_uncompressed_bytes) AS uncompressed,
sum(primary_key_bytes_in_memory) AS first_index
FROM system.parts
WHERE table = 'test_table';


SELECT
name,
sum(data_compressed_bytes) AS compressed_col,
sum(data_uncompressed_bytes) AS uncompressed_col
FROM system.columns
WHERE table = 'test_table'
GROUP BY name;

CREATE TABLE hw_4_database.merge_small (
    int_col UInt32,
    uuid_col UUID,
    dt_col DateTime,
    str_col String
) ENGINE = MergeTree()
ORDER BY int_col;

CREATE TABLE hw_4_database.merge_large (
    int_col UInt32,
    uuid_col UUID,
    dt_col DateTime,
    str_col String
) ENGINE = MergeTree()
ORDER BY int_col;

CREATE TABLE hw_4_database.buffer_large (
    int_col UInt32,
    uuid_col UUID,
    dt_col DateTime,
    str_col String
) ENGINE = Buffer(hw_4_database, merge_large, 16, 10, 100, 10000, 1000000, 10000000, 100000000);

SELECT count(*)
FROM hw_4_database.buffer_large;

SELECT
    table,
    count() AS parts,
    sum(rows) AS rows
FROM system.parts
WHERE database = 'hw_4_database'
AND table IN ('merge_small', 'merge_large')
AND active
GROUP BY table;

SELECT
    table,
    active,
    count() AS parts_count,
    sum(rows) AS rows_count
FROM system.parts
WHERE database = 'hw_4_database'
AND table IN ('merge_small', 'merge_large')
GROUP BY table, active
ORDER BY table, active DESC;


CREATE TABLE default.person_data (
  id          UInt64,
  region      LowCardinality(String),
  date_birth  Date,
  gender      UInt8,
  is_marital  UInt8,
  dt_create   DateTime default now()
)
ENGINE = MergeTree()
ORDER BY (date_birth);

SELECT *
FROM default.person_data;

INSERT INTO default.person_data(id, region, date_birth, gender, is_marital)
SELECT q.id, q.region, toStartOfDay(q.date_birth) AS date_birth, q.gender, q.is_marital
  FROM (SELECT rand() AS id,
  	   modulo(id, 70) + 20 AS n,
	   toString(n) AS region,
	   floor(randNormal(10000, 1700)) AS k,
	   '1970-01-01' + interval k day AS date_birth,
	   if(modulo(id, 3) = 1, 1, 0) AS gender,
       if((n + k) % 3 = 0 AND date_diff('year', date_birth, now()) > 18, 1, 0) AS is_marital
    FROM numbers(100_000_000)) q;

SELECT count(*)
FROM default.person_data;

OPTIMIZE TABLE default.person_data FINAL;

SELECT
    count() AS active_parts,
    sum(rows) AS rows
FROM system.parts
WHERE database = 'default'
AND table = 'person_data'
AND active;

 SELECT
     table,
     sum(primary_key_bytes_in_memory) AS primary_key
 FROM system.parts
 WHERE database = 'default'
 AND table = 'person_data'
 AND active
 GROUP BY table;
 
 SELECT t.region,
    countIf(gender = 1 AND date_diff('year', t.date_birth, now()) BETWEEN 20 AND 40) AS cnt_male,
    countIf(gender = 0 AND date_diff('year', t.date_birth, now()) BETWEEN 18 AND 30) AS cnt_female
FROM default.person_data t
WHERE t.date_birth BETWEEN toDate('2000-01-01') AND toDate('2000-01-31')
AND t.region IN ('20', '25', '43', '59')
GROUP BY t.region; 

SELECT
    query_duration_ms,
    read_rows,
    read_bytes
FROM system.query_log
WHERE query LIKE '%FROM default.person_data%'
AND type = 'QueryFinish'
ORDER BY event_time DESC
LIMIT 5;

SELECT countIf(gender = 1 AND date_diff('year', t.date_birth, now()) BETWEEN 20 AND 40) AS cnt_male,
       countIf(gender = 0 AND date_diff('year', t.date_birth, now()) BETWEEN 18 AND 30) AS cnt_female
  FROM default.person_data t
 WHERE t.is_marital = 1
   AND t.region IN ('80')
 GROUP BY t.region;

SELECT
    query_duration_ms,
    read_rows,
    read_bytes
FROM system.query_log
WHERE query LIKE '%FROM default.person_data%'
AND type = 'QueryFinish'
ORDER BY event_time DESC
LIMIT 5;


CREATE TABLE default.person_data_opt (
id          UInt64,
region      LowCardinality(String),
date_birth  Date,
gender      UInt8,
is_marital  UInt8,
dt_create   DateTime
)
ENGINE = MergeTree()
ORDER BY (region, date_birth);

INSERT INTO default.person_data_opt
SELECT *
FROM default.person_data;

SELECT t.region,
    countIf(gender = 1 AND date_diff('year', t.date_birth, now()) BETWEEN 20 AND 40) AS cnt_male,
    countIf(gender = 0 AND date_diff('year', t.date_birth, now()) BETWEEN 18 AND 30) AS cnt_female
FROM default.person_data_opt t
WHERE t.date_birth BETWEEN toDate('2000-01-01') AND toDate('2000-01-31')
AND t.region IN ('20', '25', '43', '59')
GROUP BY t.region;

SELECT
    query_duration_ms,
    read_rows,
    read_bytes
FROM system.query_log
WHERE query LIKE '%FROM default.person_data_opt%'
AND type = 'QueryFinish'
ORDER BY event_time DESC
LIMIT 5;

SELECT countIf(gender = 1 AND date_diff('year', t.date_birth, now()) BETWEEN 20 AND 40) AS cnt_male,
       countIf(gender = 0 AND date_diff('year', t.date_birth, now()) BETWEEN 18 AND 30) AS cnt_female
  FROM default.person_data_opt t
 WHERE t.is_marital = 1
   AND t.region IN ('80')
 GROUP BY t.region;

SELECT
    query_duration_ms,
    read_rows,
    read_bytes
FROM system.query_log
WHERE query LIKE '%FROM default.person_data_opt%'
AND type = 'QueryFinish'
ORDER BY event_time DESC
LIMIT 5;

CREATE TABLE default.person_data_codec (
    id          UInt64 CODEC(ZSTD(3)),
    region      LowCardinality(String) CODEC(LZ4),
    date_birth  Date CODEC(Delta, LZ4),
    gender UInt8,
	is_marital UInt8,
    dt_create   DateTime CODEC(Delta, LZ4)
)
ENGINE = MergeTree()
ORDER BY (date_birth);

INSERT INTO default.person_data_codec
SELECT *
FROM default.person_data;

OPTIMIZE TABLE default.person_data FINAL;
OPTIMIZE TABLE default.person_data_codec FINAL;

SELECT
    table,
    sum(bytes_on_disk) AS size
FROM system.parts
WHERE database = 'default'
  AND table IN ('person_data', 'person_data_codec')
  AND active
GROUP BY table;


SELECT t.region,
    countIf(gender = 1 AND date_diff('year', t.date_birth, now()) BETWEEN 20 AND 40) AS cnt_male,
    countIf(gender = 0 AND date_diff('year', t.date_birth, now()) BETWEEN 18 AND 30) AS cnt_female
FROM default.person_data_codec t
WHERE t.date_birth BETWEEN toDate('2000-01-01') AND toDate('2000-01-31')
AND t.region IN ('20', '25', '43', '59')
GROUP BY t.region;

SELECT
    query_duration_ms,
    read_rows,
    read_bytes
FROM system.query_log
WHERE query LIKE '%FROM default.person_data_codec%'
AND type = 'QueryFinish'
ORDER BY event_time DESC
LIMIT 5;

SELECT countIf(gender = 1 AND date_diff('year', t.date_birth, now()) BETWEEN 20 AND 40) AS cnt_male,
       countIf(gender = 0 AND date_diff('year', t.date_birth, now()) BETWEEN 18 AND 30) AS cnt_female
  FROM default.person_data_codec t
 WHERE t.is_marital = 1
   AND t.region IN ('80')
 GROUP BY t.region;

SELECT
    query_duration_ms,
    read_rows,
    read_bytes
FROM system.query_log
WHERE query LIKE '%FROM default.person_data_codec%'
AND type = 'QueryFinish'
ORDER BY event_time DESC
LIMIT 5;
