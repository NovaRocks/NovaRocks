-- Test Objective:
-- 1. Validate UNION ALL rewrite behavior when branch outputs are not perfectly aligned.
-- 2. Cover rewrite correctness across unaligned union branches.
-- Source: dev/test/sql/test_materialized_view/T/test_materialized_view_union

-- query 1
-- unaligned partitions: https://github.com/StarRocks/starrocks/issues/42949
CREATE TABLE IF NOT EXISTS t1 (
    leg_id VARCHAR(100) NOT NULL,
    cabin_class VARCHAR(1) NOT NULL,
    observation_date DATE NOT NULL
)
DUPLICATE KEY(leg_id, cabin_class)
PARTITION BY date_trunc('day', observation_date);

-- query 2
CREATE TABLE IF NOT EXISTS t2 (
    leg_id VARCHAR(100) NOT NULL,
    cabin_class VARCHAR(1) NOT NULL,
    observation_date DATE NOT NULL
)
DUPLICATE KEY(leg_id, cabin_class)
PARTITION BY date_trunc('day', observation_date);

-- query 3
CREATE TABLE IF NOT EXISTS t3 (
    leg_id VARCHAR(100) NOT NULL,
    cabin_class VARCHAR(1) NOT NULL,
    observation_date DATE NOT NULL
)
DUPLICATE KEY(leg_id, cabin_class)
PARTITION BY date_trunc('month', observation_date);

-- query 4
CREATE TABLE IF NOT EXISTS t4 (
    leg_id VARCHAR(100) NOT NULL,
    cabin_class VARCHAR(1) NOT NULL,
    observation_date DATE NOT NULL
)
DUPLICATE KEY(leg_id, cabin_class)
PARTITION BY RANGE(observation_date) (
  PARTITION p0 VALUES LESS THAN ('2024-03-01'),
  PARTITION p1 VALUES LESS THAN ('2024-03-02'),
  PARTITION p2 VALUES LESS THAN ('2024-04-02')
);

-- query 5
insert into t1 (leg_id, cabin_class, observation_date) values
('FL_123', 'Y', '2024-03-21'),
('FL_124', 'Y', '2024-03-21'),
('FL_125', 'Y', '2024-03-21'),
('FL_126', 'Y', '2024-03-21');

-- query 6
insert into t2 (leg_id, cabin_class, observation_date) values
('FL_123', 'Y', '2024-03-22'),
('FL_124', 'Y', '2024-03-22'),
('FL_125', 'Y', '2024-03-22'),
('FL_126', 'Y', '2024-03-22');

-- query 7
insert into t3 (leg_id, cabin_class, observation_date) values
('FL_123', 'Y', '2024-03-22'),
('FL_124', 'Y', '2024-03-22'),
('FL_125', 'Y', '2024-03-22'),
('FL_126', 'Y', '2024-03-22');

-- query 8
CREATE MATERIALIZED VIEW v1
PARTITION BY date_trunc('day', observation_date)
DISTRIBUTED BY HASH(leg_id)
REFRESH DEFERRED ASYNC
AS
SELECT * FROM t1
UNION ALL
SELECT * FROM t2;

-- query 9
REFRESH MATERIALIZED VIEW v1 WITH SYNC MODE;

-- query 10
select count(*) from v1;

-- query 11
-- day and month
CREATE MATERIALIZED VIEW v2
PARTITION BY date_trunc('day', observation_date)
DISTRIBUTED BY HASH(leg_id)
REFRESH DEFERRED ASYNC
AS
SELECT * FROM t1
UNION ALL
SELECT * FROM t3;

-- query 12
-- unaligned range
CREATE MATERIALIZED VIEW v3
PARTITION BY date_trunc('day', observation_date)
DISTRIBUTED BY HASH(leg_id)
REFRESH DEFERRED ASYNC
AS
SELECT * FROM t1
UNION ALL
SELECT * FROM t4;
