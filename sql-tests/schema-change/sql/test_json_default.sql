-- Test Objective:
-- 1. Verify JSON column default values work for all JSON value types.
-- 2. Verify JSON defaults work with Fast Schema Evolution and Traditional Schema Change.
-- 3. Verify extended column (FlatJSON) optimization correctly uses JSON defaults for old rows.
-- 4. Verify JSON defaults work with PRIMARY KEY partial updates (column mode).
-- 5. Cover FlatJSON field absence optimization for JSON columns.

-- query 1
-- Section 1: Basic JSON Default Values - all JSON value types
USE ${case_db};
CREATE TABLE basic_json_types (
    id INT NOT NULL,
    json_object JSON DEFAULT '{"status": "active", "count": 0}',
    json_array JSON DEFAULT '[1, 2, 3]',
    json_string JSON DEFAULT '"hello"',
    json_number JSON DEFAULT '42',
    json_boolean JSON DEFAULT 'true',
    json_null JSON DEFAULT 'null',
    empty_object JSON DEFAULT '{}',
    empty_array JSON DEFAULT '[]',
    empty_string JSON DEFAULT '',
    no_default JSON
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO basic_json_types (id) VALUES (1);
SELECT * FROM basic_json_types ORDER BY id;

-- query 2
-- Section 1 continued: Compare columns with and without defaults
USE ${case_db};
CREATE TABLE with_default (
    id INT,
    data JSON DEFAULT '{"default": true}'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
CREATE TABLE without_default (
    id INT,
    data JSON
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO with_default (id) VALUES (1);
INSERT INTO without_default (id) VALUES (1);
SELECT
    'with_default' AS table_name,
    data,
    data IS NULL AS is_null
FROM with_default
UNION ALL
SELECT
    'without_default',
    data,
    data IS NULL
FROM without_default order by 1;

-- query 3
-- Section 1 continued: Empty string handling
USE ${case_db};
CREATE TABLE empty_string_test (
    id INT,
    empty_unquoted JSON DEFAULT '',
    empty_quoted JSON DEFAULT '""'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO empty_string_test (id) VALUES (1);
INSERT INTO empty_string_test VALUES (2, '', '""');
SELECT
    id,
    empty_unquoted,
    empty_quoted,
    empty_unquoted = empty_quoted AS are_equal
FROM empty_string_test
ORDER BY id;

-- query 4
-- Section 2: ALTER TABLE ADD COLUMN - Fast Schema Evolution
USE ${case_db};
CREATE TABLE fast_schema_change (
    id INT NOT NULL,
    name VARCHAR(50)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
INSERT INTO fast_schema_change VALUES (1, 'alice'), (2, 'bob');
ALTER TABLE fast_schema_change ADD COLUMN metadata JSON DEFAULT '{"version": 1, "enabled": true}';
SELECT id, name, metadata FROM fast_schema_change ORDER BY id;

-- query 5
-- Section 2 continued: New rows can have actual values
USE ${case_db};
INSERT INTO fast_schema_change VALUES (3, 'charlie', '{"version": 2, "enabled": false}');
SELECT id, name, metadata FROM fast_schema_change ORDER BY id;

-- query 6
-- Section 3: ALTER TABLE ADD COLUMN - Traditional Schema Change
USE ${case_db};
CREATE TABLE traditional_schema_change (
    id INT NOT NULL,
    value INT
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "false");
INSERT INTO traditional_schema_change VALUES (1, 100), (2, 200);
ALTER TABLE traditional_schema_change ADD COLUMN metadata JSON DEFAULT '{"source": "migration", "timestamp": 0}';
SET @a = sleep(3);
SELECT count(*) FROM traditional_schema_change;

-- query 7
-- Section 4: Extended Column with JSON Functions (FlatJSON Optimization) - old rows use default
USE ${case_db};
CREATE TABLE extended_column_basic (
    id INT NOT NULL,
    user_data JSON
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO extended_column_basic VALUES
    (1, '{"user": {"name": "alice", "age": 25}}'),
    (2, '{"user": {"name": "bob", "age": 30}}');
ALTER TABLE extended_column_basic ADD COLUMN profile JSON DEFAULT '{"level": 1, "vip": false, "tags": ["default"]}';
SELECT
    id,
    cast(get_json_int(profile, '$.level') AS INT) AS level,
    cast(get_json_bool(profile, '$.vip') AS BOOLEAN) AS vip,
    get_json_string(profile, '$.tags[0]') AS first_tag
FROM extended_column_basic
ORDER BY id;

-- query 8
-- Section 4 continued: Also test with json_query function
USE ${case_db};
SELECT
    id,
    json_query(profile, '$.tags') AS tags,
    cast(get_json_int(profile, '$.level') AS INT) AS level
FROM extended_column_basic
ORDER BY id;

-- query 9
-- Section 4 continued: Mix of default values (rows 1-2) and actual values (row 3)
USE ${case_db};
INSERT INTO extended_column_basic VALUES
    (3, '{"user": {"name": "charlie", "age": 35}}', '{"level": 10, "vip": true, "tags": ["premium"]}');
SELECT
    id,
    get_json_string(user_data, '$.user.name') AS user_name,
    cast(get_json_int(profile, '$.level') AS INT) AS level,
    cast(get_json_bool(profile, '$.vip') AS BOOLEAN) AS vip,
    get_json_string(profile, '$.tags[0]') AS first_tag
FROM extended_column_basic
ORDER BY id;

-- query 10
-- Section 4.1: Extended subcolumn inherits DEFAULT from JSON parent (type coverage) - old rows
USE ${case_db};
CREATE TABLE extended_default_inherit_types (
    k1 INT
) DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
INSERT INTO extended_default_inherit_types SELECT 1;
ALTER TABLE extended_default_inherit_types
ADD COLUMN json_col JSON DEFAULT '{
  "i_str": "222",
  "i_num": 223,
  "b_str": "true",
  "b_bool": true,
  "d_str": "1.25",
  "d_num": 2.5,
  "s": "hello",
  "nullv": null,
  "obj": {"x": "7"},
  "arr": ["9"]
}';
SELECT
  k1,
  get_json_int(json_col, '$.i_str') AS i_str,
  get_json_int(json_col, '$.i_num') AS i_num,
  get_json_bool(json_col, '$.b_str') AS b_str,
  get_json_bool(json_col, '$.b_bool') AS b_bool,
  get_json_double(json_col, '$.d_str') AS d_str,
  get_json_double(json_col, '$.d_num') AS d_num,
  get_json_string(json_col, '$.s') AS s,
  get_json_int(json_col, '$.nullv') AS nullv_int,
  get_json_int(json_col, '$.obj') AS obj_int,
  get_json_int(json_col, '$.arr[0]') AS arr0_int
FROM extended_default_inherit_types
ORDER BY k1;

-- query 11
-- Section 4.1 continued: New rows (default and explicit) mixed with old row
USE ${case_db};
INSERT INTO extended_default_inherit_types(k1) SELECT 2;
INSERT INTO extended_default_inherit_types
SELECT 3, '{"i_str":"333","i_num":334,"b_str":"false","b_bool":false,"d_str":"3.75","d_num":4.5,"s":"world","nullv":null,"obj":{"x":"8"},"arr":["10"]}';
SELECT
  k1,
  get_json_int(json_col, '$.i_str') AS i_str,
  get_json_int(json_col, '$.i_num') AS i_num,
  get_json_bool(json_col, '$.b_str') AS b_str,
  get_json_bool(json_col, '$.b_bool') AS b_bool,
  get_json_double(json_col, '$.d_str') AS d_str,
  get_json_double(json_col, '$.d_num') AS d_num,
  get_json_string(json_col, '$.s') AS s,
  get_json_int(json_col, '$.nullv') AS nullv_int,
  get_json_int(json_col, '$.obj') AS obj_int,
  get_json_int(json_col, '$.arr[0]') AS arr0_int
FROM extended_default_inherit_types
ORDER BY k1;

-- query 12
-- Section 5.1: Complex nested JSON with all function types
USE ${case_db};
CREATE TABLE extended_complex_types (
    id INT NOT NULL,
    name VARCHAR(50)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO extended_complex_types VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
ALTER TABLE extended_complex_types
ADD COLUMN profile JSON DEFAULT '{"level": 10, "vip": true, "score": 95.5, "tags": ["gold", "premium"], "meta": {"city": "Beijing", "age": 25}}';
SELECT
    id,
    name,
    cast(get_json_int(profile, '$.level') AS INT) AS level,
    cast(get_json_bool(profile, '$.vip') AS BOOLEAN) AS vip,
    cast(get_json_double(profile, '$.score') AS DOUBLE) AS score,
    get_json_string(profile, '$.tags[0]') AS first_tag,
    get_json_string(profile, '$.tags[1]') AS second_tag,
    get_json_string(profile, '$.meta.city') AS city,
    cast(get_json_int(profile, '$.meta.age') AS INT) AS age
FROM extended_complex_types
ORDER BY id;

-- query 13
-- Section 5.1 continued: Mix default and actual values
USE ${case_db};
INSERT INTO extended_complex_types VALUES
    (4, 'david', '{"level": 99, "vip": false, "score": 88.8, "tags": ["silver"], "meta": {"city": "Shanghai", "age": 30}}');
SELECT
    id,
    name,
    cast(get_json_int(profile, '$.level') AS INT) AS level,
    cast(get_json_bool(profile, '$.vip') AS BOOLEAN) AS vip
FROM extended_complex_types
ORDER BY id;

-- query 14
-- Section 5.2: Multiple JSON columns with different defaults
USE ${case_db};
CREATE TABLE extended_multi_json (
    id INT NOT NULL,
    name VARCHAR(50)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO extended_multi_json VALUES (1, 'user1'), (2, 'user2');
ALTER TABLE extended_multi_json ADD COLUMN config JSON DEFAULT '{"theme": "dark", "lang": "en"}';
ALTER TABLE extended_multi_json ADD COLUMN stats JSON DEFAULT '{"views": 100, "likes": 50}';
SELECT
    id,
    name,
    get_json_string(config, '$.theme') AS theme,
    get_json_string(config, '$.lang') AS lang,
    cast(get_json_int(stats, '$.views') AS INT) AS views,
    cast(get_json_int(stats, '$.likes') AS INT) AS likes
FROM extended_multi_json
ORDER BY id;

-- query 15
-- Section 5.3: JSON with null values
USE ${case_db};
CREATE TABLE extended_null_values (
    id INT NOT NULL
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO extended_null_values VALUES (1), (2);
ALTER TABLE extended_null_values ADD COLUMN data JSON DEFAULT '{"value": null, "count": 0, "name": "test"}';
SELECT
    id,
    get_json_string(data, '$.value') AS value_field,
    cast(get_json_int(data, '$.count') AS INT) AS count_field,
    get_json_string(data, '$.name') AS name_field
FROM extended_null_values
ORDER BY id;

-- query 16
-- Section 5.4: Empty arrays and objects
USE ${case_db};
CREATE TABLE extended_empty_structures (
    id INT NOT NULL
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO extended_empty_structures VALUES (1), (2);
ALTER TABLE extended_empty_structures ADD COLUMN info JSON DEFAULT '{"tags": [], "meta": {}}';
SELECT
    id,
    json_query(info, '$.tags') AS tags,
    json_query(info, '$.meta') AS meta,
    get_json_string(info, '$.tags[0]') AS first_tag
FROM extended_empty_structures
ORDER BY id;

-- query 17
-- Section 5.5: Deep nested structures
USE ${case_db};
CREATE TABLE extended_deep_nested (
    id INT NOT NULL
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO extended_deep_nested VALUES (1), (2);
ALTER TABLE extended_deep_nested ADD COLUMN deep JSON DEFAULT '{"level1": {"level2": {"level3": {"value": 42, "flag": true}}}}';
SELECT
    id,
    cast(get_json_int(deep, '$.level1.level2.level3.value') AS INT) AS nested_value,
    cast(get_json_bool(deep, '$.level1.level2.level3.flag') AS BOOLEAN) AS nested_flag,
    json_query(deep, '$.level1.level2') AS level2_obj
FROM extended_deep_nested
ORDER BY id;

-- query 18
-- Section 5.6: Arrays with multiple elements
USE ${case_db};
CREATE TABLE extended_arrays (
    id INT NOT NULL
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO extended_arrays VALUES (1), (2), (3);
ALTER TABLE extended_arrays ADD COLUMN items JSON DEFAULT '{"products": ["apple", "banana", "orange"], "prices": [1.5, 2.0, 1.8]}';
SELECT
    id,
    get_json_string(items, '$.products[0]') AS product_0,
    get_json_string(items, '$.products[1]') AS product_1,
    get_json_string(items, '$.products[2]') AS product_2,
    cast(get_json_double(items, '$.prices[0]') AS DOUBLE) AS price_0,
    cast(get_json_double(items, '$.prices[1]') AS DOUBLE) AS price_1,
    json_query(items, '$.products') AS all_products
FROM extended_arrays
ORDER BY id;

-- query 19
-- Section 5.7: Function compatibility (json_query vs get_json_*)
USE ${case_db};
CREATE TABLE extended_function_compat (
    id INT NOT NULL
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO extended_function_compat VALUES (1), (2);
ALTER TABLE extended_function_compat ADD COLUMN profile JSON DEFAULT '{"user": {"name": "Alice", "age": 25}, "score": 95}';
SELECT
    id,
    json_query(profile, '$.user.name') AS name_via_query,
    get_json_string(profile, '$.user.name') AS name_via_get,
    json_query(profile, '$.score') AS score_via_query,
    cast(get_json_int(profile, '$.score') AS INT) AS score_via_get
FROM extended_function_compat
ORDER BY id;

-- query 20
-- Section 6.1: Basic PRIMARY KEY table with JSON defaults
USE ${case_db};
CREATE TABLE pk_basic_defaults (
    user_id INT NOT NULL,
    username VARCHAR(50),
    profile JSON DEFAULT '{"level": 1, "vip": false, "status": "active"}',
    settings JSON DEFAULT '{"notifications": true, "theme": "light"}'
) PRIMARY KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO pk_basic_defaults (user_id, username) VALUES (1, 'user1');
INSERT INTO pk_basic_defaults (user_id, username) VALUES (2, 'user2');
SELECT
    user_id,
    username,
    profile,
    settings
FROM pk_basic_defaults
ORDER BY user_id;

-- query 21
-- Section 6.1 continued: Query JSON subfields from default values
USE ${case_db};
SELECT
    user_id,
    username,
    cast(get_json_int(profile, '$.level') AS INT) AS level,
    cast(get_json_bool(profile, '$.vip') AS BOOLEAN) AS vip,
    get_json_string(profile, '$.status') AS status,
    cast(get_json_bool(settings, '$.notifications') AS BOOLEAN) AS notifications
FROM pk_basic_defaults
ORDER BY user_id;

-- query 22
-- Section 6.1 continued: Mix of default and explicit values
USE ${case_db};
INSERT INTO pk_basic_defaults VALUES
    (3, 'user3', '{"level": 10, "vip": true, "status": "premium"}', '{"notifications": false, "theme": "dark"}');
SELECT
    user_id,
    username,
    cast(get_json_int(profile, '$.level') AS INT) AS level,
    cast(get_json_bool(profile, '$.vip') AS BOOLEAN) AS vip
FROM pk_basic_defaults
ORDER BY user_id;

-- query 23
-- Section 6.2: Column mode partial update with JSON defaults
USE ${case_db};
CREATE TABLE pk_partial_update (
    order_id INT NOT NULL,
    product_name VARCHAR(50),
    quantity INT DEFAULT '1',
    config JSON DEFAULT '{"priority": "normal", "tracking": true}',
    metadata JSON DEFAULT '{"source": "web", "version": 1}'
) PRIMARY KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 2
PROPERTIES("replication_num" = "1");
SET partial_update_mode = 'column';
INSERT INTO pk_partial_update (order_id, product_name) VALUES (1, 'laptop');
INSERT INTO pk_partial_update (order_id, product_name) VALUES (2, 'phone');
INSERT INTO pk_partial_update (order_id, product_name) VALUES (3, 'tablet');
SELECT
    order_id,
    product_name,
    quantity,
    get_json_string(config, '$.priority') AS priority,
    cast(get_json_bool(config, '$.tracking') AS BOOLEAN) AS tracking,
    get_json_string(metadata, '$.source') AS source,
    cast(get_json_int(metadata, '$.version') AS INT) AS version
FROM pk_partial_update
ORDER BY order_id;

-- query 24
-- Section 6.2 continued: Insert with partial JSON columns
USE ${case_db};
INSERT INTO pk_partial_update (order_id, product_name, config)
VALUES (4, 'monitor', '{"priority": "high", "tracking": false}');
INSERT INTO pk_partial_update (order_id, product_name, metadata)
VALUES (5, 'keyboard', '{"source": "mobile", "version": 2}');
SELECT
    order_id,
    product_name,
    get_json_string(config, '$.priority') AS priority,
    get_json_string(metadata, '$.source') AS source
FROM pk_partial_update
ORDER BY order_id;

-- query 25
-- Section 6.2 continued: Add another JSON column after data exists
USE ${case_db};
ALTER TABLE pk_partial_update ADD COLUMN extras JSON DEFAULT '{"discount": 0.0, "tax": 0.08}';
INSERT INTO pk_partial_update (order_id, product_name) VALUES (6, 'mouse');
INSERT INTO pk_partial_update (order_id, product_name, quantity) VALUES (7, 'headset', 3);
SELECT
    order_id,
    product_name,
    quantity,
    get_json_string(config, '$.priority') AS priority,
    get_json_string(metadata, '$.source') AS source,
    cast(get_json_double(extras, '$.discount') AS DOUBLE) AS discount,
    cast(get_json_double(extras, '$.tax') AS DOUBLE) AS tax
FROM pk_partial_update
ORDER BY order_id;

-- query 26
-- Reset partial_update_mode to auto
USE ${case_db};
SET partial_update_mode = 'auto';
SELECT 1;

-- query 27
-- Section 7: FlatJSON Field Absence Optimization
USE ${case_db};
CREATE TABLE flatjson_field_absence (
    id INT NOT NULL,
    data JSON
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO flatjson_field_absence VALUES
    (1, '{"name": "alice", "age": 25, "city": "Beijing"}'),
    (2, '{"name": "bob", "age": 30, "city": "Shanghai"}'),
    (3, '{"name": "charlie", "age": 35, "city": "Guangzhou"}');
INSERT INTO flatjson_field_absence VALUES
    (4, '{"name": "david", "age": 40, "city": "Shenzhen", "optional_field": "value1"}'),
    (5, '{"name": "eve", "age": 45, "city": "Hangzhou", "optional_field": "value2"}');
SELECT
    id,
    get_json_string(data, '$.name') AS name,
    get_json_string(data, '$.optional_field') AS optional_field_get,
    json_query(data, '$.optional_field') AS optional_field_query
FROM flatjson_field_absence
ORDER BY id;

-- query 28
-- Section 7 continued: Query completely missing field - NULL for all rows
USE ${case_db};
SELECT
    id,
    get_json_string(data, '$.completely_missing') AS missing_field
FROM flatjson_field_absence
ORDER BY id;
