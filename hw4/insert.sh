#!/bin/bash

CLICKHOUSE="docker exec -i clickhouse clickhouse-client -u admin --password admin"

insert_small() {
for i in {1..300}; do
    $CLICKHOUSE <<'SQL'
INSERT INTO hw_4_database.merge_small
SELECT
    modulo(rand(), 999) + 1,
    generateUUIDv4(),
    now() - INTERVAL rand() / 1000 SECOND,
    multiIf(
        rand() / 500000 <= 1500, 'A',
        rand() / 500000 <= 3000, 'B',
        rand() / 500000 <= 4500, 'C',
        rand() / 500000 <= 6000, 'D',
        rand() / 500000 <= 7300, 'E',
        'F'
    )
FROM numbers(5000);

SQL
    sleep 0.5
done
}

insert_buffer() {
for i in {1..300}; do
    $CLICKHOUSE <<'SQL'
INSERT INTO hw_4_database.buffer_large
SELECT
    modulo(rand(), 999) + 1,
    generateUUIDv4(),
    now() - INTERVAL rand() / 1000 SECOND,
    multiIf(
        rand() / 500000 <= 1500, 'A',
        rand() / 500000 <= 3000, 'B',
        rand() / 500000 <= 4500, 'C',
        rand() / 500000 <= 6000, 'D',
        rand() / 500000 <= 7300, 'E',
        'F'
    )
FROM numbers(5000);
SQL
    sleep 0.5
done
}

insert_small & insert_buffer & wait
