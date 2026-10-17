-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- Value-domain facts follow selected windows, identity CASTs and collection transforms.
-- query 1
CREATE TABLE ${case_db}.window_result_domains (id INT);

-- query 2
INSERT INTO ${case_db}.window_result_domains VALUES (1),(2);

-- query 3
-- @order_sensitive=true
-- Construct nominal Variant with the selected accessor before exercising its
-- identity CAST and window handoff. Observe values through the typed accessor.
SELECT id,
       variant_get(first_variant, '$', 'bigint') AS first_variant,
       variant_get(next_variant, '$', 'bigint') AS next_variant
FROM (
  SELECT id,
         first_value(CAST(variant_get(parse_json(CAST(id AS VARCHAR)), '$') AS VARIANT)) OVER (ORDER BY id) AS first_variant,
         lead(CAST(variant_get(parse_json(CAST(id AS VARCHAR)), '$') AS VARIANT), 1, CAST(NULL AS VARIANT)) OVER (ORDER BY id) AS next_variant
  FROM ${case_db}.window_result_domains
) window_values ORDER BY id;

-- query 4
-- @order_sensitive=true
SELECT id,
       first_value(to_bitmap(id)) OVER (ORDER BY id) AS bitmap_state,
       lag(hll_hash(CAST(id AS BIGINT))) OVER (ORDER BY id) AS hll_state,
       lead(parse_json('{"a": 2}'), 1, parse_json('{}')) OVER (ORDER BY id) AS next_json
FROM ${case_db}.window_result_domains ORDER BY id;

-- query 5
-- @order_sensitive=true
SELECT id, first_value([to_bitmap(id), NULL]) OVER (ORDER BY id) AS bitmap_values
FROM ${case_db}.window_result_domains ORDER BY id;

-- query 6
-- @order_sensitive=true
SELECT id,
       element_at(array_flatten([[to_bitmap(id)]]), 1) AS flattened_state,
       element_at(array_repeat(hll_hash(CAST(id AS BIGINT)), 2), 1) AS repeated_state,
       element_at(__array_struct_subfield([named_struct('payload', to_bitmap(id))], 'payload'), 1) AS selected_state,
       arrays_zip([to_bitmap(id)], [parse_json('{}')])[1].col1 AS zipped_state,
       bitmap_to_string(element_at(array_flatten([[to_bitmap(id)]]), 1)) AS flattened_members
FROM ${case_db}.window_result_domains ORDER BY id;
