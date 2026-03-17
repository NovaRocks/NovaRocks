-- Test Objective:
-- 1. Validate i256 boundary arithmetic: INT_256MAX and INT_256MIN exact values, +/-1 cases.
-- 2. Validate overflow behavior for multiply/add operations at INT_256MAX and INT_256MIN.
-- 3. Validate exact 256-bit result boundary (bit count = 256) and overflow (bit count = 257).
-- 4. Validate decimal multiplication overflow across various precision combinations.
-- 5. Validate progressive boundary testing with self-multiplication of different digit widths.
-- 6. Validate scale overflow vs value overflow distinction.
-- 7. Validate special value cases: multiply by 0, 1, 0.1, -1.
-- Migrated from dev/test/sql/test_decimal/T/test_decimal256_arithmetic_overflow

-- =============================================================================
-- TEST-0: int256 boundary values extreme cases
-- =============================================================================

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.powers_of_2 (
    power_250 DECIMAL(76, 0)
) PROPERTIES("replication_num"="1");

-- pow(2, 250)
insert into ${case_db}.powers_of_2 select 1809251394333065553493296640760748560207343510400633813116524750123642650624;

create table ${case_db}.test_256max_result(d1 decimal(76, 0)) PROPERTIES("replication_num"="1");
insert into ${case_db}.test_256max_result select 1496577676626844588240573307387100039795808514605057;

create table ${case_db}.test_256min_result(d1 decimal(76, 0), d2 decimal(76, 0)) PROPERTIES("replication_num"="1");
insert into ${case_db}.test_256min_result select 340282366920938463463374607431768211456, -170141183460469231731687303715884105728;

CREATE TABLE ${case_db}.test_decimal_multiply_overflow (
    id INT,
    case_desc VARCHAR(200),
    d60_30 DECIMAL(60, 30),
    d70_20 DECIMAL(70, 20),
    d76_10 DECIMAL(76, 10),
    d76_38 DECIMAL(76, 38),
    d50_0  DECIMAL(50, 0),
    d38_0  DECIMAL(38, 0)
) PROPERTIES("replication_num"="1");

INSERT INTO ${case_db}.test_decimal_multiply_overflow VALUES
-- 1. Small values
(1, 'Small values',
 1.123456789012345678901234567890,
 12.12345678901234567890,
 123.1234567890,
 0.12345678901234567890123456789012345678,
 1000,
 12345),

-- 2. Medium values
(2, 'Medium values',
 123456789012345678901234567890.123456789012345678901234567890,
 12345678901234567890123456789012345678901234567890.12345678901234567890,
 1234567890123456789012345678901234567890123456789012345678901234.1234567890,
 12345678901234567890123456789012345678.12345678901234567890123456789012345678,
 12345678901234567890123456789012345678901234567890,
 12345678901234567890123456789012345678),

-- 3. Near maximum values for each type
(3, 'Near max positive values',
 999999999999999999999999999999.999999999999999999999999999999,
 99999999999999999999999999999999999999999999999999.99999999999999999999,
 9999999999999999999999999999999999999999999999999999999999999999.9999999999,
 99999999999999999999999999999999999999.99999999999999999999999999999999999999,
 99999999999999999999999999999999999999999999999999,
 99999999999999999999999999999999999999),

-- 4. Corresponding negative values
(4, 'Near max negative values',
 -999999999999999999999999999999.999999999999999999999999999999,
 -99999999999999999999999999999999999999999999999999.99999999999999999999,
 -9999999999999999999999999999999999999999999999999999999999999999.9999999999,
 -99999999999999999999999999999999999999.99999999999999999999999999999999999999,
 -99999999999999999999999999999999999999999999999999,
 -99999999999999999999999999999999999999);

CREATE TABLE ${case_db}.test_boundary_values (
    id INT,
    case_desc VARCHAR(200),
    val_76_0 DECIMAL(76, 0),
    val_38_0 DECIMAL(38, 0),
    val_19_0 DECIMAL(19, 0),
    val_25_0 DECIMAL(25, 0),
    val_high_scale DECIMAL(76, 50)
) PROPERTIES("replication_num"="1");

INSERT INTO ${case_db}.test_boundary_values VALUES
-- 1. Near 76-digit decimal maximum
(1, 'Near 76-digit decimal max',
 9999999999999999999999999999999999999999999999999999999999999999999999999999,
 99999999999999999999999999999999999999,
 9999999999999999999,
 9999999999999999999999999,
 0.00000000000000000000000000000000000000000000000001),

-- 2. Corresponding negative values
(2, 'Near 76-digit decimal min',
 -9999999999999999999999999999999999999999999999999999999999999999999999999999,
 -99999999999999999999999999999999999999,
 -9999999999999999999,
 -9999999999999999999999999,
 -0.00000000000000000000000000000000000000000000000001),

-- 3. Values for testing multiplication overflow
(3, 'Multiplication overflow test values',
 1000000000000000000000000000000000000000000000000000000000000000000000000000,
 10000000000000000000000000000000000000,
 1000000000000000000,
 1000000000000000000000000,
 0.00000000000000000000000000000000000000000000000001),

-- 4. Medium size values
(4, 'Medium size values',
 5000000000000000000000000000000000000000000000000000000000000000000000000000,
 50000000000000000000000000000000000000,
 5000000000000000000,
 5000000000000000000000000,
 0.50000000000000000000000000000000000000000000000000);

CREATE TABLE ${case_db}.test_progressive_boundary (
    id INT,
    digits INT,
    test_val DECIMAL(76, 0)
) PROPERTIES("replication_num"="1");

INSERT INTO ${case_db}.test_progressive_boundary VALUES
(1, 10, 9999999999),
(2, 15, 999999999999999),
(3, 20, 99999999999999999999),
(4, 25, 9999999999999999999999999),
(5, 30, 999999999999999999999999999999),
(6, 35, 99999999999999999999999999999999999),
(7, 38, 99999999999999999999999999999999999999);

-- query 2
-- INT_256MAX
select cast(power_250 * 16 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602495 as string) from ${case_db}.powers_of_2;

-- query 3
-- INT_256MIN
select cast((-(power_250 * 16 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602495) - 1) as string) from ${case_db}.powers_of_2;

-- query 4
-- INT_256MAX + 1 (overflow)
select cast(power_250 * 16 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602495 + 1 as string) from ${case_db}.powers_of_2;

-- query 5
-- INT_256MAX - 1
select cast(power_250 * 16 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602495 - 1 as string) from ${case_db}.powers_of_2;

-- query 6
-- INT_256MAX * 0
select cast((power_250 * 16 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602495) * 0 as string) from ${case_db}.powers_of_2;

-- query 7
-- INT_256MAX * -1
select cast((power_250 * 16 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602495) * -1 as string) from ${case_db}.powers_of_2;

-- query 8
-- INT_256MAX * 2 (overflow)
select cast((power_250 * 16 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602495) * 2 as string) from ${case_db}.powers_of_2;

-- query 9
-- INT_256MAX * -2 (overflow)
select cast((power_250 * 16 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602495) * -2 as string) from ${case_db}.powers_of_2;

-- query 10
-- INT_256MIN - 1 (overflow)
select cast((-(power_250 * 16 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602495) - 1) - 1 as string) from ${case_db}.powers_of_2;

-- query 11
-- INT_256MIN + 1 (not overflow)
select cast((-(power_250 * 16 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602495) - 1) + 1 as string) from ${case_db}.powers_of_2;

-- query 12
-- INT_256MIN * 1
select cast((-(power_250 * 16 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602495) - 1) * 1 as string) from ${case_db}.powers_of_2;

-- query 13
-- INT_256MIN * -1 (overflow. special case)
select cast((-(power_250 * 16 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602495) - 1) * -1 as string) from ${case_db}.powers_of_2;

-- query 14
-- INT_256MIN * -2 (overflow)
select cast((-(power_250 * 16 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602495) - 1) * -2 as string) from ${case_db}.powers_of_2;

-- query 15
-- INT_256MIN * 2 (overflow)
select cast((-(power_250 * 16 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602495) - 1) * 2 as string) from ${case_db}.powers_of_2;

-- query 16
-- INT_256MIN * 0
select cast((-(power_250 * 16 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602495) - 1) * 0 as string) from ${case_db}.powers_of_2;

-- query 17
-- INT_256MIN + INT256_MAX(-1)
select cast(power_250 * 16 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602495 + (-(power_250 * 16 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602495) - 1) as string) from ${case_db}.powers_of_2;

-- query 18
-- INT_256MAX * INT_256MIN (overflow)
select cast((power_250 * 16 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602495) * (-(power_250 * 16 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602495) - 1) as string) from ${case_db}.powers_of_2;

-- query 19
-- INT_256MAX * INT_256MAX (overflow)
select cast((power_250 * 16 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602495) * (power_250 * 16 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602495) as string) from ${case_db}.powers_of_2;

-- query 20
-- INT_256MIN * INT_256MIN (overflow)
select cast((-(power_250 * 16 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602495) - 1) * (-(power_250 * 16 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602496 + 7237005577332262213973186563042994240829374041602535252466099000494570602495) - 1) as string) from ${case_db}.powers_of_2;

-- query 21
-- TEST INT_256MAX result (bit count is 256)
select cast(d1 * 38685626227668133590597631 as string) from ${case_db}.test_256max_result;

-- query 22
-- TEST INT_256MIN result (bit count is 257)
select cast(d1 * d2 as string) from ${case_db}.test_256min_result;

-- =============================================================================
-- Test 1: Positive number multiplication overflow
-- =============================================================================

-- query 23
-- Test 1.1: 76-digit max * 76-digit max (definite overflow)
SELECT
    'Test 1.1: 76-digit max * 76-digit max (definite overflow)' as test_case,
    cast(val_76_0 as string),
    cast(val_76_0 * val_76_0 as string) as result,
    'Should overflow: 10^75 * 10^75 = 10^150, way beyond int256' as expected
FROM ${case_db}.test_boundary_values WHERE id = 1;

-- query 24
-- Test 1.2: 38-digit max * 38-digit max (may overflow)
SELECT
    'Test 1.2: 38-digit max * 38-digit max (may overflow)' as test_case,
    cast(val_38_0 as string),
    cast(cast(val_38_0 as decimal(76, 0)) * val_38_0 as string) as result,
    'not overflow' as expected
FROM ${case_db}.test_boundary_values WHERE id = 1;

-- query 25
-- Test 1.3: 19-digit * 19-digit (should not overflow)
SELECT
    'Test 1.3: 19-digit * 19-digit (should not overflow)' as test_case,
    cast(val_19_0 as string),
    cast(val_19_0 * val_19_0 as string) as result,
    'Should not overflow: 10^18 * 10^18 = 10^36, within int256' as expected
FROM ${case_db}.test_boundary_values WHERE id = 1;

-- query 26
-- Test 1.4: Large value * 2
SELECT
    'Test 1.4: Large value * 2' as test_case,
    cast(val_76_0 as string),
    cast(val_76_0 * 2 as string) as result,
    'Should overflow: near-max * 2' as expected
FROM ${case_db}.test_boundary_values WHERE id = 1;

-- query 27
-- Test 1.5: Large value * 5
SELECT
    'Test 1.5: Large value * 5' as test_case,
    cast(val_76_0 as string),
    cast(val_76_0 * 5 as string) as result,
    'Should overflow: near-max * 5' as expected
FROM ${case_db}.test_boundary_values WHERE id = 1;

-- query 28
-- Test 1.6: Large value * 5.1 (overflow)
SELECT
    'Test 1.6: Large value * 5.1' as test_case,
    cast(val_76_0 as string),
    cast(val_76_0 * 5.1 as string) as result,
    'Should overflow: near-max * 5.1' as expected
FROM ${case_db}.test_boundary_values WHERE id = 1;

-- query 29
-- Test 1.7: Large value * 6 (overflow)
SELECT
    'Test 1.7: Large value * 6' as test_case,
    cast(val_76_0 as string),
    cast(val_76_0 * 6 as string) as result,
    'Should overflow: near-max * 6' as expected
FROM ${case_db}.test_boundary_values WHERE id = 1;

-- =============================================================================
-- Test 2: Negative number multiplication overflow
-- =============================================================================

-- query 30
-- Test 2.1: Large negative * Large negative (positive overflow)
SELECT
    'Test 2.1: Large negative * Large negative (positive overflow)' as test_case,
    cast(val_76_0 as string),
    cast(val_76_0 * val_76_0 as string) as result,
    'Should overflow: (-10^75) * (-10^75) = +10^150' as expected
FROM ${case_db}.test_boundary_values WHERE id = 2;

-- query 31
-- Test 2.2: Large negative * Large positive (negative overflow)
SELECT
    'Test 2.2: Large negative * Large positive (negative overflow)' as test_case,
    cast(a.val_76_0 as string) as neg_val,
    cast(b.val_76_0 as string) as pos_val,
    cast(a.val_76_0 * b.val_76_0 as string) as result,
    'Should overflow: (-10^75) * (+10^75) = -10^150' as expected
FROM ${case_db}.test_boundary_values a, ${case_db}.test_boundary_values b
WHERE a.id = 2 AND b.id = 1;

-- query 32
-- Test 2.3: Negative boundary * 5
SELECT
    'Test 2.3: Negative boundary * 5' as test_case,
    cast(val_76_0 as string),
    cast(val_76_0 * 5 as string) as result,
    'Should not overflow' as expected
FROM ${case_db}.test_boundary_values WHERE id = 2;

-- query 33
-- Test 2.4: Negative boundary * 6
SELECT
    'Test 2.4: Negative boundary * 6' as test_case,
    cast(val_76_0 as string),
    cast(val_76_0 * 6 as string) as result,
    'Should overflow' as expected
FROM ${case_db}.test_boundary_values WHERE id = 2;

-- =============================================================================
-- Test 3: Different precision combination overflow tests
-- =============================================================================

-- query 34
-- Test 3.1: 76-digit * 38-digit
SELECT
    'Test 3.1: 76-digit * 38-digit' as test_case,
    cast(a.val_76_0 as string),
    cast(b.val_38_0 as string),
    cast(a.val_76_0 * b.val_38_0 as string) as result,
    'Should overflow: 10^75 * 10^37 = 10^112' as expected
FROM ${case_db}.test_boundary_values a, ${case_db}.test_boundary_values b
WHERE a.id = 1 AND b.id = 1;

-- query 35
-- Test 3.2: 38-digit * 25-digit (can't auto scale up now)
SELECT
    'Test 3.2: 38-digit * 25-digit' as test_case,
    cast(a.val_38_0 as string),
    cast(b.val_25_0 as string),
    cast(a.val_38_0 * b.val_25_0 as string) as result,
    'overflow' as expected
FROM ${case_db}.test_boundary_values a, ${case_db}.test_boundary_values b
WHERE a.id = 1 AND b.id = 1;

-- query 36
-- Test 3.2: 38-digit * 25-digit (with explicit cast)
SELECT
    'Test 3.2: 38-digit * 25-digit' as test_case,
    cast(a.val_38_0 as string),
    cast(b.val_25_0 as string),
    cast(cast(a.val_38_0 as decimal(50, 10)) * b.val_25_0 as string) as result,
    'should not overflow' as expected
FROM ${case_db}.test_boundary_values a, ${case_db}.test_boundary_values b
WHERE a.id = 1 AND b.id = 1;

-- query 37
-- Test 3.3: 25-digit * 25-digit (can't auto scale up now)
SELECT
    'Test 3.3: 25-digit * 25-digit' as test_case,
    cast(val_25_0 as string),
    cast(val_25_0 * val_25_0 as string) as result,
    'overflow' as expected
FROM ${case_db}.test_boundary_values WHERE id = 1;

-- query 38
-- Test 3.3: 25-digit * 25-digit (with explicit cast)
SELECT
    'Test 3.3: 25-digit * 25-digit' as test_case,
    cast(val_25_0 as string),
    cast(val_25_0 * cast(val_25_0 as decimal(39, 0)) as string) as result,
    'should not overflow' as expected
FROM ${case_db}.test_boundary_values WHERE id = 1;

-- =============================================================================
-- Test 4: Progressive boundary testing
-- =============================================================================

-- query 39
SELECT
    id,
    digits,
    cast(test_val as string),
    cast(test_val * test_val as string) as result,
    CASE
        WHEN digits <= 19 THEN 'Should succeed'
        WHEN digits <= 25 THEN 'May succeed'
        WHEN digits > 30 THEN 'Should succeed'
        ELSE 'Boundary case'
    END as expected
FROM ${case_db}.test_progressive_boundary ORDER BY id;

-- =============================================================================
-- Test 5: Scale overflow vs value overflow distinction
-- =============================================================================

-- query 40
-- @expect_error=Return scale(100) exceeds maximum value(76)
-- Test 5.1: Scale overflow, tiny value (throw exception)
SELECT
    'Test 5.1: Scale overflow, tiny value' as test_case,
    cast(val_high_scale * val_high_scale as string) as result,
    'Should fail due to scale=100 > 76' as expected
FROM ${case_db}.test_boundary_values WHERE id = 1;

-- query 41
-- Test 5.2: Value overflow but reasonable scale
SELECT
    'Test 5.2: Value overflow, scale OK' as test_case,
        cast(val_38_0 * val_38_0 as string) as result,
    'Should fail due to value overflow, scale=0 is fine' as expected
FROM ${case_db}.test_boundary_values WHERE id = 1;

-- =============================================================================
-- Test 6: Special values and boundary cases
-- =============================================================================

-- query 42
-- Test 6.1: Large * 0
SELECT
    'Test 6.1: Large * 0' as test_case,
    cast(val_76_0 * 0 as string) as result,
    'Should return 0, no overflow' as expected
FROM ${case_db}.test_boundary_values WHERE id = 1;

-- query 43
-- Test 6.2: Large * 1
SELECT
    'Test 6.2: Large * 1' as test_case,
    cast(val_76_0 * 1 as string) as result,
    'Should return original value' as expected
FROM ${case_db}.test_boundary_values WHERE id = 1;

-- query 44
-- Test 6.3: Large * 0.1
SELECT
    'Test 6.3: Large * 0.1' as test_case,
    cast(val_76_0 * 0.1 as string) as result,
    'Should succeed, reduces magnitude' as expected
FROM ${case_db}.test_boundary_values WHERE id = 1;

-- query 45
-- Test 6.4: Large * -1
SELECT
    'Test 6.4: Large * -1' as test_case,
    cast(val_76_0 * (-1) as string) as result,
    'Should return negative of original' as expected
FROM ${case_db}.test_boundary_values WHERE id = 1;

-- =============================================================================
-- Test 7: Real-world scenario boundary tests
-- =============================================================================

-- query 46
-- Test 7.2: Scientific calculation
SELECT
    'Test 7.2: Scientific calculation' as test_case,
    cast(d76_10 * d76_10 as string) as result,
    'Simulates scientific computation overflow' as expected
FROM ${case_db}.test_decimal_multiply_overflow WHERE id = 3;
