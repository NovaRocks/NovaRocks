-- @tags=join,predicate,expr_reuse
-- Test Objective:
-- Validate correctness of join queries where the same expression subtree appears
-- multiple times in WHERE predicates (expression reuse / CSE scenarios).
-- Covers: LEFT/RIGHT/INNER/CROSS/LEFT SEMI/LEFT ANTI joins with reused abs(),
-- bit_shift_left(), and arithmetic expressions in filter predicates.
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
) DUPLICATE KEY(v1)
DISTRIBUTED BY HASH(v1) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- query 4
-- @skip_result_check=true
CREATE TABLE ${case_db}.pre_t1 (
    v4 INT,
    v5 INT,
    v6 VARCHAR(20)
) DUPLICATE KEY(v4)
DISTRIBUTED BY HASH(v4) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.pre_t0 VALUES
(1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c'), (4, 40, 'd'), (5, 50, 'e'),
(6, 60, 'f'), (7, 70, 'g'), (8, 80, 'h'), (9, 90, 'i'), (10, 100, 'j'),
(11, 110, 'a'), (12, 120, 'b'), (13, 130, 'c'), (14, 140, 'd'), (15, 150, 'e'),
(16, 160, 'f'), (17, 170, 'g'), (18, 180, 'h'), (19, 190, 'i'), (20, 200, 'j'),
(21, 210, 'a'), (22, 220, 'b'), (23, 230, 'c'), (24, 240, 'd'), (25, 250, 'e'),
(26, 260, 'f'), (27, 270, 'g'), (28, 280, 'h'), (29, 290, 'i'), (30, 300, 'j'),
(31, 310, 'a'), (32, 320, 'b'), (33, 330, 'c'), (34, 340, 'd'), (35, 350, 'e'),
(36, 360, 'f'), (37, 370, 'g'), (38, 380, 'h'), (39, 390, 'i'), (40, 400, 'j'),
(41, 410, 'a'), (42, 420, 'b'), (43, 430, 'c'), (44, 440, 'd'), (45, 450, 'e'),
(46, 460, 'f'), (47, 470, 'g'), (48, 480, 'h'), (49, 490, 'i'), (50, 500, 'j'),
(51, 510, 'a'), (52, 520, 'b'), (53, 530, 'c'), (54, 540, 'd'), (55, 550, 'e'),
(56, 560, 'f'), (57, 570, 'g'), (58, 580, 'h'), (59, 590, 'i'), (60, 600, 'j'),
(61, 610, 'a'), (62, 620, 'b'), (63, 630, 'c'), (64, 640, 'd'), (65, 650, 'e'),
(66, 660, 'f'), (67, 670, 'g'), (68, 680, 'h'), (69, 690, 'i'), (70, 700, 'j'),
(71, 710, 'a'), (72, 720, 'b'), (73, 730, 'c'), (74, 740, 'd'), (75, 750, 'e'),
(76, 760, 'f'), (77, 770, 'g'), (78, 780, 'h'), (79, 790, 'i'), (80, 800, 'j'),
(81, 810, 'a'), (82, 820, 'b'), (83, 830, 'c'), (84, 840, 'd'), (85, 850, 'e'),
(86, 860, 'f'), (87, 870, 'g'), (88, 880, 'h'), (89, 890, 'i'), (90, 900, 'j'),
(91, 910, 'a'), (92, 920, 'b'), (93, 930, 'c'), (94, 940, 'd'), (95, 950, 'e'),
(96, 960, 'f'), (97, 970, 'g'), (98, 980, 'h'), (99, 990, 'i'), (100, 1000, 'j');

-- query 6
-- @skip_result_check=true
INSERT INTO ${case_db}.pre_t1 VALUES
(1, 15, 'x'), (2, 25, 'y'), (3, 35, 'z'), (4, 45, 'w'), (5, 55, 'v'),
(6, 65, 'u'), (7, 75, 't'), (8, 85, 's'), (9, 95, 'r'), (10, 105, 'q'),
(11, 115, 'x'), (12, 125, 'y'), (13, 135, 'z'), (14, 145, 'w'), (15, 155, 'v'),
(16, 165, 'u'), (17, 175, 't'), (18, 185, 's'), (19, 195, 'r'), (20, 205, 'q'),
(21, 215, 'x'), (22, 225, 'y'), (23, 235, 'z'), (24, 245, 'w'), (25, 255, 'v'),
(26, 265, 'u'), (27, 275, 't'), (28, 285, 's'), (29, 295, 'r'), (30, 305, 'q'),
(31, 315, 'x'), (32, 325, 'y'), (33, 335, 'z'), (34, 345, 'w'), (35, 355, 'v'),
(36, 365, 'u'), (37, 375, 't'), (38, 385, 's'), (39, 395, 'r'), (40, 405, 'q'),
(41, 415, 'x'), (42, 425, 'y'), (43, 435, 'z'), (44, 445, 'w'), (45, 455, 'v'),
(46, 465, 'u'), (47, 475, 't'), (48, 485, 's'), (49, 495, 'r'), (50, 505, 'q'),
(51, 515, 'x'), (52, 525, 'y'), (53, 535, 'z'), (54, 545, 'w'), (55, 555, 'v'),
(56, 565, 'u'), (57, 575, 't'), (58, 585, 's'), (59, 595, 'r'), (60, 605, 'q'),
(61, 615, 'x'), (62, 625, 'y'), (63, 635, 'z'), (64, 645, 'w'), (65, 655, 'v'),
(66, 665, 'u'), (67, 675, 't'), (68, 685, 's'), (69, 695, 'r'), (70, 705, 'q'),
(71, 715, 'x'), (72, 725, 'y'), (73, 735, 'z'), (74, 745, 'w'), (75, 755, 'v'),
(76, 765, 'u'), (77, 775, 't'), (78, 785, 's'), (79, 795, 'r'), (80, 805, 'q'),
(81, 815, 'x'), (82, 825, 'y'), (83, 835, 'z'), (84, 845, 'w'), (85, 855, 'v'),
(86, 865, 'u'), (87, 875, 't'), (88, 885, 's'), (89, 895, 'r'), (90, 905, 'q'),
(91, 915, 'x'), (92, 925, 'y'), (93, 935, 'z'), (94, 945, 'w'), (95, 955, 'v'),
(96, 965, 'u'), (97, 975, 't'), (98, 985, 's'), (99, 995, 'r'), (100, 1005, 'q');

-- query 7
-- LEFT JOIN with reused abs() in WHERE
SELECT COUNT(*) FROM ${case_db}.pre_t0 LEFT JOIN ${case_db}.pre_t1 ON ${case_db}.pre_t0.v1 = ${case_db}.pre_t1.v4
WHERE abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) = abs(${case_db}.pre_t0.v2 + ${case_db}.pre_t1.v5) AND abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) > 5;

-- query 8
-- LEFT JOIN with reused bit_shift_left() in WHERE
SELECT COUNT(*) FROM ${case_db}.pre_t0 LEFT JOIN ${case_db}.pre_t1 ON ${case_db}.pre_t0.v1 = ${case_db}.pre_t1.v4
WHERE bit_shift_left(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4, 1) = 10 OR bit_shift_left(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4, 1) = 20;

-- query 9
-- RIGHT JOIN with reused abs() in WHERE
SELECT COUNT(*) FROM ${case_db}.pre_t0 RIGHT JOIN ${case_db}.pre_t1 ON ${case_db}.pre_t0.v1 = ${case_db}.pre_t1.v4
WHERE abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) = abs(${case_db}.pre_t0.v2 + ${case_db}.pre_t1.v5) AND abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) > 5;

-- query 10
-- RIGHT JOIN with reused bit_shift_left() in WHERE
SELECT COUNT(*) FROM ${case_db}.pre_t0 RIGHT JOIN ${case_db}.pre_t1 ON ${case_db}.pre_t0.v1 = ${case_db}.pre_t1.v4
WHERE bit_shift_left(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4, 1) = 10 OR bit_shift_left(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4, 1) = 20;

-- query 11
-- LEFT JOIN with inequality ON and reused abs() in WHERE
SELECT COUNT(*) FROM ${case_db}.pre_t0 LEFT JOIN ${case_db}.pre_t1 ON ${case_db}.pre_t0.v1 > ${case_db}.pre_t1.v4
WHERE abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) = abs(${case_db}.pre_t0.v2 + ${case_db}.pre_t1.v5) AND abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) > 5;

-- query 12
-- LEFT JOIN with inequality ON and reused bit_shift_left() in WHERE
SELECT COUNT(*) FROM ${case_db}.pre_t0 LEFT JOIN ${case_db}.pre_t1 ON ${case_db}.pre_t0.v1 > ${case_db}.pre_t1.v4
WHERE bit_shift_left(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4, 1) = 10 OR bit_shift_left(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4, 1) = 20;

-- query 13
-- RIGHT JOIN with inequality + equality ON and reused abs() in WHERE
SELECT COUNT(*) FROM ${case_db}.pre_t0 RIGHT JOIN ${case_db}.pre_t1 ON ${case_db}.pre_t0.v1 > ${case_db}.pre_t1.v4 AND ${case_db}.pre_t0.v2 = ${case_db}.pre_t1.v5
WHERE abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) = abs(${case_db}.pre_t0.v2 + ${case_db}.pre_t1.v5) AND abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) > 5;

-- query 14
-- LEFT JOIN with reused abs() range predicate
SELECT COUNT(*) FROM ${case_db}.pre_t0 LEFT JOIN ${case_db}.pre_t1 ON ${case_db}.pre_t0.v1 = ${case_db}.pre_t1.v4
WHERE abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) > 5 AND abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) < 10;

-- query 15
-- LEFT JOIN with reused abs() + bit_shift_left() combination
SELECT COUNT(*) FROM ${case_db}.pre_t0 LEFT JOIN ${case_db}.pre_t1 ON ${case_db}.pre_t0.v1 = ${case_db}.pre_t1.v4
WHERE abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) = abs(${case_db}.pre_t0.v2 + ${case_db}.pre_t1.v5)
  AND bit_shift_left(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4, 1) > 10
  AND bit_shift_left(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4, 1) < 20;

-- query 16
-- RIGHT JOIN with inequality ON and reused abs() OR ranges
SELECT COUNT(*) FROM ${case_db}.pre_t0 RIGHT JOIN ${case_db}.pre_t1 ON ${case_db}.pre_t0.v1 > ${case_db}.pre_t1.v4
WHERE (abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) > 5 AND abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) < 10)
   OR (abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) > 15 AND abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) < 20);

-- query 17
-- LEFT JOIN with nested reused abs(bit_shift_left())
SELECT COUNT(*) FROM ${case_db}.pre_t0 LEFT JOIN ${case_db}.pre_t1 ON ${case_db}.pre_t0.v1 = ${case_db}.pre_t1.v4
WHERE abs(bit_shift_left(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4, 1)) = abs(bit_shift_left(${case_db}.pre_t0.v2 + ${case_db}.pre_t1.v5, 1))
  AND abs(bit_shift_left(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4, 1)) > 10;

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
SELECT COUNT(*) FROM ${case_db}.pre_t0 JOIN ${case_db}.pre_t1
WHERE abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) = abs(${case_db}.pre_t0.v2 + ${case_db}.pre_t1.v5) AND abs(${case_db}.pre_t0.v1 + ${case_db}.pre_t1.v4) > 5;

-- query 22
-- @order_sensitive=false
-- LEFT SEMI JOIN with reused abs() in WHERE + GROUP BY
SELECT COUNT(*), abs(${case_db}.pre_t0.v1) FROM ${case_db}.pre_t0 LEFT SEMI JOIN ${case_db}.pre_t1 ON ${case_db}.pre_t0.v1 = ${case_db}.pre_t1.v4
WHERE abs(${case_db}.pre_t0.v1) + abs(${case_db}.pre_t0.v2) > 10 AND abs(${case_db}.pre_t0.v1) + abs(${case_db}.pre_t0.v2) < 100 AND abs(${case_db}.pre_t0.v1) > 5
GROUP BY abs(${case_db}.pre_t0.v1);

-- query 23
-- LEFT ANTI JOIN with reused abs() in WHERE + GROUP BY (expect empty)
SELECT COUNT(*), abs(${case_db}.pre_t0.v1) FROM ${case_db}.pre_t0 LEFT ANTI JOIN ${case_db}.pre_t1 ON ${case_db}.pre_t0.v1 = ${case_db}.pre_t1.v4
WHERE abs(${case_db}.pre_t0.v1) + abs(${case_db}.pre_t0.v2) > 10 AND abs(${case_db}.pre_t0.v1) + abs(${case_db}.pre_t0.v2) < 100 AND abs(${case_db}.pre_t0.v1) > 5
GROUP BY abs(${case_db}.pre_t0.v1);
