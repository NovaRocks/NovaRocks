-- Migrated from dev/test/sql/test_bitmap_functions/T/test_bitmap_to_base64
-- Test Objective:
-- 1. Validate bitmap_to_base64(NULL) returns NULL.
-- 2. Validate bitmap_to_base64 on invalid bitmap input returns NULL.

-- query 1
select bitmap_to_base64(null);

-- query 2
select bitmap_to_base64(bitmap_from_string("abc"));
