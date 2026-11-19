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

-- Test Objective:
-- 1. Regression test for crash when partition runtime filter is applied
--    to a complex CTE with UNNEST + LEFT JOIN pattern.
-- 2. No persistent tables needed; all data is inline via CTEs.
-- query 1
WITH `ID_TABLE` AS (
    SELECT
        "A" AS `ID`
),
`MAP_TABLE` AS (
    SELECT
        ["A", "B", "C"] AS `MAP_KEYS`
),
`AGGED_MAP_TABLE_EX` AS (
    SELECT
        ARRAY_AGG(
            `AGGED_MAP_TABLE`.`MAP_KEYS`
        ) AS `AGGED_MAP_KEYS`
    FROM
        `MAP_TABLE` AS `AGGED_MAP_TABLE`
),
`AGGED_MAP_FIND_TABLE` AS (
    SELECT
        ["A", "B", "C"] AS `FIND_AGGED_KEY`
    FROM
        `AGGED_MAP_TABLE_EX`
),
`GROUPED_FIND_TABLE` AS (
    SELECT
        `GROUPED_AGGED_MAP_FIND_TABLE`.`FIND_AGGED_KEY` AS `FIND_AGGED_KEY`
    FROM
        `AGGED_MAP_FIND_TABLE` AS `GROUPED_AGGED_MAP_FIND_TABLE`
    GROUP BY
        `GROUPED_AGGED_MAP_FIND_TABLE`.`FIND_AGGED_KEY`
),
`AGGED_GROUPED_FIND_SEQUENCE` AS (
    SELECT
        `AGGED_GROUPED_FIND_TABLE`.`FIND_AGGED_KEY` AS `FIND_AGGED_KEY`,
        123 AS `GROUPED_FIND_LITERAL`
    FROM
        `GROUPED_FIND_TABLE` AS `AGGED_GROUPED_FIND_TABLE`
),
`HANDLE_SEQUENCE_TABLE` AS (
    SELECT
        `MockTOSQL_UNNEST`.`FIND_AGGED_KEY` AS `FIND_AGGED_KEY`,
        `SEQUENCE_TABLE`.`GROUPED_FIND_LITERAL` AS `GROUPED_FIND_LITERAL`
    FROM
        `AGGED_GROUPED_FIND_SEQUENCE` AS `SEQUENCE_TABLE`,
        UNNEST(
            `SEQUENCE_TABLE`.`FIND_AGGED_KEY`
        ) AS MockTOSQL_UNNEST(
            `FIND_AGGED_KEY`
        )
),
`JOINED_SEQUENCE` AS (
    SELECT
        `R_TABLE`.`ID` AS `JOINED_ID`,
        `HANDLE_SEQUENCE_TABLE_JOINED`.`GROUPED_FIND_LITERAL` AS `JOINED_LITERAL`
    FROM
        `HANDLE_SEQUENCE_TABLE` AS `HANDLE_SEQUENCE_TABLE_JOINED`
        LEFT JOIN `ID_TABLE` AS `R_TABLE` ON
            `HANDLE_SEQUENCE_TABLE_JOINED`.`FIND_AGGED_KEY` = `R_TABLE`.`ID`

),
`JOINED_AGGED_SEQUENCE` AS (
    SELECT
        `JOINED_TABLE_SEQUENCE`.`JOINED_LITERAL` AS `JOINED_LITERAL`,
        ARRAY_AGG(
            `JOINED_TABLE_SEQUENCE`.`JOINED_ID` ORDER BY `JOINED_TABLE_SEQUENCE`.`JOINED_ID` ASC NULLS LAST
        ) AS `JOINED_TABLE_AGGED_SEQUENCE`
    FROM
        `JOINED_SEQUENCE` AS `JOINED_TABLE_SEQUENCE`
    GROUP BY
        `JOINED_TABLE_SEQUENCE`.`JOINED_LITERAL`
)
SELECT
    `JOINED_AGGED_SEQUENCE_R_TABLE`.`JOINED_TABLE_AGGED_SEQUENCE` AS `RENAME_9`
FROM
    `AGGED_GROUPED_FIND_SEQUENCE` AS `AGGED_GROUP_FIND_L_TABLE`
    LEFT JOIN `JOINED_AGGED_SEQUENCE` AS `JOINED_AGGED_SEQUENCE_R_TABLE` ON
        `AGGED_GROUP_FIND_L_TABLE`.`GROUPED_FIND_LITERAL` = `JOINED_AGGED_SEQUENCE_R_TABLE`.`JOINED_LITERAL`
    ;
