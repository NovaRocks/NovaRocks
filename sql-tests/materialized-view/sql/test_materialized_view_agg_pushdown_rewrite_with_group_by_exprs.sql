-- Test Objective:
-- 1. Validate aggregate pushdown rewrite with expression-based GROUP BY keys.
-- 2. Cover MV refresh and rewrite behavior across joined dimension filters.
-- Source: dev/test/sql/test_materialized_view/T/test_materialized_view_agg_pushdown_rewrite

-- query 1
CREATE TABLE fact_event_requests
(
    event_date                DATE                        ,
    request_id                VARCHAR(64)                ,
    event_time                DATETIME                  ,
    event_status              VARCHAR(32)                       ,
    region_id                 INT                               ,
    site_id                   INT                               ,
    event_type                VARCHAR(16)                       ,
    location_path             ARRAY<VARCHAR(12)>               ,
    channel                   VARCHAR(8)
)
ENGINE = olap
PARTITION BY RANGE (event_date)
(
    START ("20251001") END ("20251101") EVERY (INTERVAL 1 DAY)
)
DISTRIBUTED BY HASH(channel, request_id) BUCKETS 16
PROPERTIES
(
    "replication_num" = "1"
);

-- query 2
insert into fact_event_requests values('20251001', '1', '20251001', 'SUCCESS', 1, 1, 'TYPE_A', ['1234567890'], 'mobile'),
('20251001', '2', '20251001', 'SUCCESS', 1, 1, 'TYPE_A', ['1234567890'], 'mobile'),
('20251001', '3', '20251001', 'SUCCESS', 1, 1, 'TYPE_A', ['1234567890'], 'mobile'),
('20251001', '4', '20251001', 'SUCCESS', 1, 1, 'TYPE_A', ['1234567890'], 'mobile'),
('20251001', '5', '20251001', 'SUCCESS', 1, 1, 'TYPE_A', ['1234567890'], 'mobile'),
('20251001', '6', '20251001', 'SUCCESS', 1, 1, 'TYPE_A', ['1234567890'], 'mobile'),
('20251001', '7', '20251001', 'SUCCESS', 1, 1, 'TYPE_A', ['1234567890'], 'mobile'),
('20251001', '8', '20251001', 'SUCCESS', 1, 1, 'TYPE_A', ['1234567890'], 'mobile'),
('20251001', '9', '20251001', 'SUCCESS', 1, 1, 'TYPE_A', ['1234567890'], 'mobile');

-- query 3
CREATE TABLE dim_location_area
(
    geohash              VARCHAR(12),
    area_name            VARCHAR(64)
)
DISTRIBUTED BY HASH(geohash) BUCKETS 8
PROPERTIES
(
    "replication_num" = "1"
);

-- query 4
INSERT INTO dim_location_area values('1234567890', 'Zone-1'),
('1234567891', 'Zone-2'),
('1234567892', 'Zone-3'),
('1234567893', 'Zone-4'),
('1234567894', 'Zone-5'),
('1234567895', 'Zone-6'),
('1234567896', 'Zone-7'),
('1234567897', 'Zone-8');

-- query 5
CREATE MATERIALIZED VIEW mv_hourly_events
DISTRIBUTED BY RANDOM
Partition by (event_date)
AS
SELECT
    event_date,
    DATE_TRUNC('hour', event_time) as metric_time_1h,
    region_id,
    site_id,
    channel,
    location_path[cardinality(location_path)] as last_geohash,
    COUNT(request_id) as total_requests
FROM fact_event_requests
WHERE event_type = 'TYPE_A'
GROUP BY event_date, metric_time_1h, region_id, site_id, channel, last_geohash;

-- query 6
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_hourly_events WITH SYNC MODE;

-- query 7
-- @result_contains=mv_hourly_events
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT DATE_TRUNC('hour', event_time) as metric_time_1h, CAST(COUNT(request_id) AS DOUBLE) FROM fact_event_requests AS fact_data_source LEFT JOIN (SELECT geohash, max(area_name) AS area_name FROM dim_location_area GROUP BY 1) AS dim_location_area ON dim_location_area.geohash = location_path[cardinality(location_path)] WHERE event_date = 20251001 AND area_name IN ('Zone-1') AND event_type = 'TYPE_A' GROUP BY 1 ORDER BY 1 limit 3;

-- query 8
SELECT DATE_TRUNC('hour', event_time) as metric_time_1h, CAST(COUNT(request_id) AS DOUBLE) FROM fact_event_requests AS fact_data_source LEFT JOIN (SELECT geohash, max(area_name) AS area_name FROM dim_location_area GROUP BY 1) AS dim_location_area ON dim_location_area.geohash = location_path[cardinality(location_path)] WHERE event_date = 20251001 AND area_name IN ('Zone-1') AND event_type = 'TYPE_A' GROUP BY 1 ORDER BY 1 limit 3;

-- query 9
-- upadte fact_event_requests
INSERT INTO fact_event_requests values('20251001', '10', '20251001 10:00:00', 'SUCCESS', 1, 1, 'TYPE_A', ['1234567890'], 'mobile');

-- query 10
-- @result_not_contains=mv_hourly_events
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT DATE_TRUNC('hour', event_time) as metric_time_1h, CAST(COUNT(request_id) AS DOUBLE) FROM fact_event_requests AS fact_data_source LEFT JOIN (SELECT geohash, max(area_name) AS area_name FROM dim_location_area GROUP BY 1) AS dim_location_area ON dim_location_area.geohash = location_path[cardinality(location_path)] WHERE event_date = 20251001 AND area_name IN ('Zone-1') AND event_type = 'TYPE_A' GROUP BY 1 ORDER BY 1 limit 3;

-- query 11
SELECT DATE_TRUNC('hour', event_time) as metric_time_1h, CAST(COUNT(request_id) AS DOUBLE) FROM fact_event_requests AS fact_data_source LEFT JOIN (SELECT geohash, max(area_name) AS area_name FROM dim_location_area GROUP BY 1) AS dim_location_area ON dim_location_area.geohash = location_path[cardinality(location_path)] WHERE event_date = 20251001 AND area_name IN ('Zone-1') AND event_type = 'TYPE_A' GROUP BY 1 ORDER BY 1 limit 3;

-- query 12
-- update dim_location_area
INSERT INTO dim_location_area values('1234567898', 'Zone-9');

-- query 13
-- @result_not_contains=mv_hourly_events
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT DATE_TRUNC('hour', event_time) as metric_time_1h, CAST(COUNT(request_id) AS DOUBLE) FROM fact_event_requests AS fact_data_source LEFT JOIN (SELECT geohash, max(area_name) AS area_name FROM dim_location_area GROUP BY 1) AS dim_location_area ON dim_location_area.geohash = location_path[cardinality(location_path)] WHERE event_date = 20251001 AND area_name IN ('Zone-1') AND event_type = 'TYPE_A' GROUP BY 1 ORDER BY 1 limit 3;

-- query 14
SELECT DATE_TRUNC('hour', event_time) as metric_time_1h, CAST(COUNT(request_id) AS DOUBLE) FROM fact_event_requests AS fact_data_source LEFT JOIN (SELECT geohash, max(area_name) AS area_name FROM dim_location_area GROUP BY 1) AS dim_location_area ON dim_location_area.geohash = location_path[cardinality(location_path)] WHERE event_date = 20251001 AND area_name IN ('Zone-1') AND event_type = 'TYPE_A' GROUP BY 1 ORDER BY 1 limit 3;

-- query 15
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_hourly_events WITH SYNC MODE;

-- query 16
-- @result_contains=mv_hourly_events
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT DATE_TRUNC('hour', event_time) as metric_time_1h, CAST(COUNT(request_id) AS DOUBLE) FROM fact_event_requests AS fact_data_source LEFT JOIN (SELECT geohash, max(area_name) AS area_name FROM dim_location_area GROUP BY 1) AS dim_location_area ON dim_location_area.geohash = location_path[cardinality(location_path)] WHERE event_date = 20251001 AND area_name IN ('Zone-1') AND event_type = 'TYPE_A' GROUP BY 1 ORDER BY 1 limit 3;

-- query 17
SELECT DATE_TRUNC('hour', event_time) as metric_time_1h, CAST(COUNT(request_id) AS DOUBLE) FROM fact_event_requests AS fact_data_source LEFT JOIN (SELECT geohash, max(area_name) AS area_name FROM dim_location_area GROUP BY 1) AS dim_location_area ON dim_location_area.geohash = location_path[cardinality(location_path)] WHERE event_date = 20251001 AND area_name IN ('Zone-1') AND event_type = 'TYPE_A' GROUP BY 1 ORDER BY 1 limit 3;

-- query 18
drop materialized view mv_hourly_events;

-- query 19
drop table fact_event_requests;

-- query 20
drop table dim_location_area;
