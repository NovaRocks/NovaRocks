-- @order_sensitive=true
-- @tags=project,string,append_trailing_char_if_absent
-- Test Objective:
-- 1. Validate append_trailing_char_if_absent requires a single-byte trailing character.
-- 2. Prevent regressions where empty or multi-character trailing inputs produce non-NULL results.
-- Test Flow:
-- 1. Evaluate normal append and already-suffixed rows.
-- 2. Evaluate empty/multi-character and UTF-8 multibyte trailing arguments.
-- 3. Assert deterministic output and NULL markers in a single-row projection.
SELECT
  append_trailing_char_if_absent('a', 'c') AS append_ascii,
  append_trailing_char_if_absent('ac', 'c') AS keep_existing,
  append_trailing_char_if_absent('', 'c') AS empty_input_kept,
  append_trailing_char_if_absent('a', '') IS NULL AS empty_trailing_is_null,
  append_trailing_char_if_absent('a', 'xy') IS NULL AS multi_trailing_is_null,
  append_trailing_char_if_absent('a', '中') IS NULL AS utf8_multi_byte_trailing_is_null,
  append_trailing_char_if_absent(NULL, 'c') IS NULL AS null_input_is_null,
  append_trailing_char_if_absent('a', NULL) IS NULL AS null_trailing_is_null;
