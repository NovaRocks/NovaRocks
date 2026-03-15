-- Migrated from dev/test/sql/test_agg_function/R/test_dict_merge
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: testDictMerge
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `test_dict_merge` (
  `id` int NULL COMMENT "",
  `city` string NOT NULL COMMENT "",
  `city_null` string NULL COMMENT "",
  `city_array` array<string> NOT NULL COMMENT "",
  `city_array_null` array<string> NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 4
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into test_dict_merge values
(1, "beijing", "beijing", ["beijing", "shanghai"], NULL),
(1, "beijing", NULL, ["shenzhen", "shanghai"], ["shenzhen", "shanghai"]),
(1, "shanghai", "shanghai", ["shenzhen", NULL], ["shenzhen", NULL]),
(1, "shanghai", NULL, ["beijing", NULL, "shanghai"], NULL);

-- query 4
USE ${case_db};
select dict_merge(city, 255) from test_dict_merge;

-- query 5
USE ${case_db};
select dict_merge(city_null, 255) from test_dict_merge;

-- query 6
USE ${case_db};
select dict_merge(city_array, 255) from test_dict_merge;

-- query 7
USE ${case_db};
select dict_merge(city_array_null, 255) from test_dict_merge;

-- query 8
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t1 (
    c1 int,
    c2 string
    )
DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1)
BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- query 9
-- @skip_result_check=true
USE ${case_db};
insert into t1 select generate_series, cast(generate_series as int) from table(generate_series(1, 1000));

-- query 10
USE ${case_db};
select dict_merge(c2, 256) from t1;

-- query 11
USE ${case_db};
select dict_merge(c2, 512) from t1;

-- query 12
USE ${case_db};
select dict_merge(c2, 1024) from t1;
