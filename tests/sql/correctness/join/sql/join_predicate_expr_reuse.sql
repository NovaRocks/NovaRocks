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

-- @tags=join,predicate,expr_reuse
-- Test Objective:
-- Validate correctness of join queries where the same expression subtree appears
-- multiple times in WHERE predicates (expression reuse / CSE scenarios).
-- Covers: LEFT/RIGHT/INNER/CROSS/LEFT SEMI/LEFT ANTI joins with reused abs(),
-- bitwise_left_shift(), and arithmetic expressions in filter predicates.
-- Test Flow:
-- 1. Create t0 (100 rows) and t1 (100 rows) with non-overlapping v2/v5 values.
-- 2. Run joins with reused expression predicates and verify counts.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.pre_t0;

-- query 2
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.pre_t1;

-- query 3
-- @skip_result_check=true
CREATE TABLE ${case_db}.pre_t0 (
    v1 INT,
    v2 INT,
    v3 VARCHAR(20)
) WITH (format_version = 3);

-- query 4
-- @skip_result_check=true
CREATE TABLE ${case_db}.pre_t1 (
    v4 INT,
    v5 INT,
    v6 VARCHAR(20)
) WITH (format_version = 3);

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.pre_t0 VALUES
ROW(1, 10, 'a'), ROW(2, 20, 'b'), ROW(3, 30, 'c'), ROW(4, 40, 'd'), ROW(5, 50, 'e'),
ROW(6, 60, 'f'), ROW(7, 70, 'g'), ROW(8, 80, 'h'), ROW(9, 90, 'i'), ROW(10, 100, 'j'),
ROW(11, 110, 'a'), ROW(12, 120, 'b'), ROW(13, 130, 'c'), ROW(14, 140, 'd'), ROW(15, 150, 'e'),
ROW(16, 160, 'f'), ROW(17, 170, 'g'), ROW(18, 180, 'h'), ROW(19, 190, 'i'), ROW(20, 200, 'j'),
ROW(21, 210, 'a'), ROW(22, 220, 'b'), ROW(23, 230, 'c'), ROW(24, 240, 'd'), ROW(25, 250, 'e'),
ROW(26, 260, 'f'), ROW(27, 270, 'g'), ROW(28, 280, 'h'), ROW(29, 290, 'i'), ROW(30, 300, 'j'),
ROW(31, 310, 'a'), ROW(32, 320, 'b'), ROW(33, 330, 'c'), ROW(34, 340, 'd'), ROW(35, 350, 'e'),
ROW(36, 360, 'f'), ROW(37, 370, 'g'), ROW(38, 380, 'h'), ROW(39, 390, 'i'), ROW(40, 400, 'j'),
ROW(41, 410, 'a'), ROW(42, 420, 'b'), ROW(43, 430, 'c'), ROW(44, 440, 'd'), ROW(45, 450, 'e'),
ROW(46, 460, 'f'), ROW(47, 470, 'g'), ROW(48, 480, 'h'), ROW(49, 490, 'i'), ROW(50, 500, 'j'),
ROW(51, 510, 'a'), ROW(52, 520, 'b'), ROW(53, 530, 'c'), ROW(54, 540, 'd'), ROW(55, 550, 'e'),
ROW(56, 560, 'f'), ROW(57, 570, 'g'), ROW(58, 580, 'h'), ROW(59, 590, 'i'), ROW(60, 600, 'j'),
ROW(61, 610, 'a'), ROW(62, 620, 'b'), ROW(63, 630, 'c'), ROW(64, 640, 'd'), ROW(65, 650, 'e'),
ROW(66, 660, 'f'), ROW(67, 670, 'g'), ROW(68, 680, 'h'), ROW(69, 690, 'i'), ROW(70, 700, 'j'),
ROW(71, 710, 'a'), ROW(72, 720, 'b'), ROW(73, 730, 'c'), ROW(74, 740, 'd'), ROW(75, 750, 'e'),
ROW(76, 760, 'f'), ROW(77, 770, 'g'), ROW(78, 780, 'h'), ROW(79, 790, 'i'), ROW(80, 800, 'j'),
ROW(81, 810, 'a'), ROW(82, 820, 'b'), ROW(83, 830, 'c'), ROW(84, 840, 'd'), ROW(85, 850, 'e'),
ROW(86, 860, 'f'), ROW(87, 870, 'g'), ROW(88, 880, 'h'), ROW(89, 890, 'i'), ROW(90, 900, 'j'),
ROW(91, 910, 'a'), ROW(92, 920, 'b'), ROW(93, 930, 'c'), ROW(94, 940, 'd'), ROW(95, 950, 'e'),
ROW(96, 960, 'f'), ROW(97, 970, 'g'), ROW(98, 980, 'h'), ROW(99, 990, 'i'), ROW(100, 1000, 'j');

-- query 6
-- @skip_result_check=true
INSERT INTO ${case_db}.pre_t1 VALUES
ROW(1, 15, 'x'), ROW(2, 25, 'y'), ROW(3, 35, 'z'), ROW(4, 45, 'w'), ROW(5, 55, 'v'),
ROW(6, 65, 'u'), ROW(7, 75, 't'), ROW(8, 85, 's'), ROW(9, 95, 'r'), ROW(10, 105, 'q'),
ROW(11, 115, 'x'), ROW(12, 125, 'y'), ROW(13, 135, 'z'), ROW(14, 145, 'w'), ROW(15, 155, 'v'),
ROW(16, 165, 'u'), ROW(17, 175, 't'), ROW(18, 185, 's'), ROW(19, 195, 'r'), ROW(20, 205, 'q'),
ROW(21, 215, 'x'), ROW(22, 225, 'y'), ROW(23, 235, 'z'), ROW(24, 245, 'w'), ROW(25, 255, 'v'),
ROW(26, 265, 'u'), ROW(27, 275, 't'), ROW(28, 285, 's'), ROW(29, 295, 'r'), ROW(30, 305, 'q'),
ROW(31, 315, 'x'), ROW(32, 325, 'y'), ROW(33, 335, 'z'), ROW(34, 345, 'w'), ROW(35, 355, 'v'),
ROW(36, 365, 'u'), ROW(37, 375, 't'), ROW(38, 385, 's'), ROW(39, 395, 'r'), ROW(40, 405, 'q'),
ROW(41, 415, 'x'), ROW(42, 425, 'y'), ROW(43, 435, 'z'), ROW(44, 445, 'w'), ROW(45, 455, 'v'),
ROW(46, 465, 'u'), ROW(47, 475, 't'), ROW(48, 485, 's'), ROW(49, 495, 'r'), ROW(50, 505, 'q'),
ROW(51, 515, 'x'), ROW(52, 525, 'y'), ROW(53, 535, 'z'), ROW(54, 545, 'w'), ROW(55, 555, 'v'),
ROW(56, 565, 'u'), ROW(57, 575, 't'), ROW(58, 585, 's'), ROW(59, 595, 'r'), ROW(60, 605, 'q'),
ROW(61, 615, 'x'), ROW(62, 625, 'y'), ROW(63, 635, 'z'), ROW(64, 645, 'w'), ROW(65, 655, 'v'),
ROW(66, 665, 'u'), ROW(67, 675, 't'), ROW(68, 685, 's'), ROW(69, 695, 'r'), ROW(70, 705, 'q'),
ROW(71, 715, 'x'), ROW(72, 725, 'y'), ROW(73, 735, 'z'), ROW(74, 745, 'w'), ROW(75, 755, 'v'),
ROW(76, 765, 'u'), ROW(77, 775, 't'), ROW(78, 785, 's'), ROW(79, 795, 'r'), ROW(80, 805, 'q'),
ROW(81, 815, 'x'), ROW(82, 825, 'y'), ROW(83, 835, 'z'), ROW(84, 845, 'w'), ROW(85, 855, 'v'),
ROW(86, 865, 'u'), ROW(87, 875, 't'), ROW(88, 885, 's'), ROW(89, 895, 'r'), ROW(90, 905, 'q'),
ROW(91, 915, 'x'), ROW(92, 925, 'y'), ROW(93, 935, 'z'), ROW(94, 945, 'w'), ROW(95, 955, 'v'),
ROW(96, 965, 'u'), ROW(97, 975, 't'), ROW(98, 985, 's'), ROW(99, 995, 'r'), ROW(100, 1005, 'q');

-- query 7
-- LEFT JOIN with reused abs() in WHERE
SELECT COUNT(*) FROM ${case_db}.pre_t0 LEFT JOIN ${case_db}.pre_t1 ON ${case_db}.pre_t0.v1 = ${case_db}.pre_t1.v4
WHERE abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) = abs(${case_db}.pre_t0.v2 + ${case_db}.pre_t1.v5) AND abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) > 5;

-- query 8
-- LEFT JOIN with reused bitwise_left_shift() in WHERE
SELECT COUNT(*) FROM ${case_db}.pre_t0 LEFT JOIN ${case_db}.pre_t1 ON ${case_db}.pre_t0.v1 = ${case_db}.pre_t1.v4
WHERE bitwise_left_shift(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4, 1) = 10 OR bitwise_left_shift(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4, 1) = 20;

-- query 9
-- RIGHT JOIN with reused abs() in WHERE
SELECT COUNT(*) FROM ${case_db}.pre_t0 RIGHT JOIN ${case_db}.pre_t1 ON ${case_db}.pre_t0.v1 = ${case_db}.pre_t1.v4
WHERE abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) = abs(${case_db}.pre_t0.v2 + ${case_db}.pre_t1.v5) AND abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) > 5;

-- query 10
-- RIGHT JOIN with reused bitwise_left_shift() in WHERE
SELECT COUNT(*) FROM ${case_db}.pre_t0 RIGHT JOIN ${case_db}.pre_t1 ON ${case_db}.pre_t0.v1 = ${case_db}.pre_t1.v4
WHERE bitwise_left_shift(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4, 1) = 10 OR bitwise_left_shift(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4, 1) = 20;

-- query 11
-- LEFT JOIN with inequality ON and reused abs() in WHERE
SELECT COUNT(*) FROM ${case_db}.pre_t0 LEFT JOIN ${case_db}.pre_t1 ON ${case_db}.pre_t0.v1 > ${case_db}.pre_t1.v4
WHERE abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) = abs(${case_db}.pre_t0.v2 + ${case_db}.pre_t1.v5) AND abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) > 5;

-- query 12
-- LEFT JOIN with inequality ON and reused bitwise_left_shift() in WHERE
SELECT COUNT(*) FROM ${case_db}.pre_t0 LEFT JOIN ${case_db}.pre_t1 ON ${case_db}.pre_t0.v1 > ${case_db}.pre_t1.v4
WHERE bitwise_left_shift(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4, 1) = 10 OR bitwise_left_shift(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4, 1) = 20;

-- query 13
-- RIGHT JOIN with inequality + equality ON and reused abs() in WHERE
SELECT COUNT(*) FROM ${case_db}.pre_t0 RIGHT JOIN ${case_db}.pre_t1 ON ${case_db}.pre_t0.v1 > ${case_db}.pre_t1.v4 AND ${case_db}.pre_t0.v2 = ${case_db}.pre_t1.v5
WHERE abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) = abs(${case_db}.pre_t0.v2 + ${case_db}.pre_t1.v5) AND abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) > 5;

-- query 14
-- LEFT JOIN with reused abs() range predicate
SELECT COUNT(*) FROM ${case_db}.pre_t0 LEFT JOIN ${case_db}.pre_t1 ON ${case_db}.pre_t0.v1 = ${case_db}.pre_t1.v4
WHERE abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) > 5 AND abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) < 10;

-- query 15
-- LEFT JOIN with reused abs() + bitwise_left_shift() combination
SELECT COUNT(*) FROM ${case_db}.pre_t0 LEFT JOIN ${case_db}.pre_t1 ON ${case_db}.pre_t0.v1 = ${case_db}.pre_t1.v4
WHERE abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) = abs(${case_db}.pre_t0.v2 + ${case_db}.pre_t1.v5)
  AND bitwise_left_shift(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4, 1) > 10
  AND bitwise_left_shift(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4, 1) < 20;

-- query 16
-- RIGHT JOIN with inequality ON and reused abs() OR ranges
SELECT COUNT(*) FROM ${case_db}.pre_t0 RIGHT JOIN ${case_db}.pre_t1 ON ${case_db}.pre_t0.v1 > ${case_db}.pre_t1.v4
WHERE (abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) > 5 AND abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) < 10)
   OR (abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) > 15 AND abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) < 20);

-- query 17
-- LEFT JOIN with nested reused abs(bitwise_left_shift())
SELECT COUNT(*) FROM ${case_db}.pre_t0 LEFT JOIN ${case_db}.pre_t1 ON ${case_db}.pre_t0.v1 = ${case_db}.pre_t1.v4
WHERE abs(bitwise_left_shift(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4, 1)) = abs(bitwise_left_shift(${case_db}.pre_t0.v2 + ${case_db}.pre_t1.v5, 1))
  AND abs(bitwise_left_shift(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4, 1)) > 10;

-- query 18
-- RIGHT JOIN with reused arithmetic multiplication
SELECT COUNT(*) FROM ${case_db}.pre_t0 RIGHT JOIN ${case_db}.pre_t1 ON ${case_db}.pre_t0.v1 = ${case_db}.pre_t1.v4
WHERE (${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) * 2 = (${case_db}.pre_t0.v2 + ${case_db}.pre_t1.v5) * 2
  AND (${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) * 2 > 10
  AND (${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) * 2 < 100;

-- query 19
-- INNER JOIN with reused abs() in WHERE
SELECT COUNT(*) FROM ${case_db}.pre_t0 JOIN ${case_db}.pre_t1 ON ${case_db}.pre_t0.v1 = ${case_db}.pre_t1.v4
WHERE abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) = abs(${case_db}.pre_t0.v2 + ${case_db}.pre_t1.v5) AND abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) > 5;

-- query 20
-- INNER JOIN with reused abs() in ON clause
SELECT COUNT(*) FROM ${case_db}.pre_t0 JOIN ${case_db}.pre_t1 ON ${case_db}.pre_t0.v1 = ${case_db}.pre_t1.v4
  AND abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) = abs(${case_db}.pre_t0.v2 + ${case_db}.pre_t1.v5) AND abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) > 5;

-- query 21
-- CROSS JOIN with reused abs() in WHERE
SELECT COUNT() FROM ${case_db}.pre_t0 CROSS JOIN ${case_db}.pre_t1
WHERE abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) = abs(${case_db}.pre_t0.v2 + ${case_db}.pre_t1.v5) AND abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) > 5;

-- query 22
-- @order_sensitive=false
-- LEFT SEMI JOIN with reused abs() in WHERE + GROUP BY
SELECT COUNT(*), abs(${case_db}.pre_t0.v1) FROM ${case_db}.pre_t0
WHERE EXISTS (SELECT 1 FROM ${case_db}.pre_t1 WHERE ${case_db}.pre_t0.v1 = ${case_db}.pre_t1.v4) AND (abs(${case_db}.pre_t0.v1) + abs(${case_db}.pre_t0.v2) > 10 AND abs(${case_db}.pre_t0.v1) + abs(${case_db}.pre_t0.v2) < 100 AND abs(${case_db}.pre_t0.v1) > 5)
GROUP BY abs(${case_db}.pre_t0.v1);

-- query 23
-- LEFT ANTI JOIN with reused abs() in WHERE + GROUP BY (expect empty)
SELECT COUNT(*), abs(${case_db}.pre_t0.v1) FROM ${case_db}.pre_t0
WHERE NOT EXISTS (SELECT 1 FROM ${case_db}.pre_t1 WHERE ${case_db}.pre_t0.v1 = ${case_db}.pre_t1.v4) AND (abs(${case_db}.pre_t0.v1) + abs(${case_db}.pre_t0.v2) > 10 AND abs(${case_db}.pre_t0.v1) + abs(${case_db}.pre_t0.v2) < 100 AND abs(${case_db}.pre_t0.v1) > 5)
GROUP BY abs(${case_db}.pre_t0.v1);
