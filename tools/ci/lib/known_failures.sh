#!/usr/bin/env bash

ci_known_failure_match() {
  local baseline="$1"
  local tier="$2"
  local suite="$3"
  local case_name="$4"
  local error_code="$5"
  local match

  if [ ! -f "$baseline" ]; then
    printf "NEW_FAIL||\n"
    return 0
  fi

  match="$(
    awk \
      -v want_tier="$tier" \
      -v want_suite="$suite" \
      -v want_case="$case_name" \
      -v want_error_code="$error_code" '
      function trim(s) {
        sub(/^[[:space:]]+/, "", s)
        sub(/[[:space:]]+$/, "", s)
        return s
      }

      function unquote(s) {
        s = trim(s)
        if (s ~ /^".*"$/) {
          s = substr(s, 2, length(s) - 2)
        }
        return s
      }

      function reset_row() {
        row_tier = ""
        row_suite = ""
        row_case = ""
        row_error_code = ""
        row_reason = ""
        row_expires = ""
      }

      function flush_row() {
        if (matched) {
          return
        }
        if (row_tier == want_tier &&
            row_suite == want_suite &&
            row_case == want_case &&
            row_error_code == want_error_code) {
          print "KNOWN_FAIL|" row_reason "|" row_expires
          matched = 1
        }
      }

      BEGIN {
        matched = 0
        reset_row()
      }

      /^[[:space:]]*#/ || /^[[:space:]]*$/ {
        next
      }

      /^[[:space:]]*\[\[failure\]\][[:space:]]*$/ {
        flush_row()
        reset_row()
        next
      }

      index($0, "=") > 0 {
        key = $0
        sub(/[[:space:]]*=.*/, "", key)
        key = trim(key)

        value = $0
        sub(/^[^=]*=[[:space:]]*/, "", value)
        value = unquote(value)

        if (key == "tier") {
          row_tier = value
        } else if (key == "suite") {
          row_suite = value
        } else if (key == "case") {
          row_case = value
        } else if (key == "error_code") {
          row_error_code = value
        } else if (key == "reason") {
          row_reason = value
        } else if (key == "expires") {
          row_expires = value
        }
      }

      END {
        flush_row()
      }
    ' "$baseline"
  )"

  if [ -n "$match" ]; then
    printf "%s\n" "$match"
  else
    printf "NEW_FAIL||\n"
  fi
}

ci_known_failure_rows_for_suite() {
  local baseline="$1"
  local tier="$2"
  local suite="$3"

  [ -f "$baseline" ] || return 0

  awk \
    -v want_tier="$tier" \
    -v want_suite="$suite" '
    function trim(s) {
      sub(/^[[:space:]]+/, "", s)
      sub(/[[:space:]]+$/, "", s)
      return s
    }

    function unquote(s) {
      s = trim(s)
      if (s ~ /^".*"$/) {
        s = substr(s, 2, length(s) - 2)
      }
      return s
    }

    function reset_row() {
      row_tier = ""
      row_suite = ""
      row_case = ""
      row_error_code = ""
      row_reason = ""
      row_expires = ""
    }

    function flush_row() {
      if (row_tier == want_tier && row_suite == want_suite && row_case != "" && row_error_code != "") {
        print row_case "|" row_error_code "|" row_reason "|" row_expires
      }
    }

    BEGIN {
      reset_row()
    }

    /^[[:space:]]*#/ || /^[[:space:]]*$/ {
      next
    }

    /^[[:space:]]*\[\[failure\]\][[:space:]]*$/ {
      flush_row()
      reset_row()
      next
    }

    index($0, "=") > 0 {
      key = $0
      sub(/[[:space:]]*=.*/, "", key)
      key = trim(key)

      value = $0
      sub(/^[^=]*=[[:space:]]*/, "", value)
      value = unquote(value)

      if (key == "tier") {
        row_tier = value
      } else if (key == "suite") {
        row_suite = value
      } else if (key == "case") {
        row_case = value
      } else if (key == "error_code") {
        row_error_code = value
      } else if (key == "reason") {
        row_reason = value
      } else if (key == "expires") {
        row_expires = value
      }
    }

    END {
      flush_row()
    }
  ' "$baseline"
}

ci_known_failure_status() {
  local baseline="$1"
  local tier="$2"
  local suite="$3"
  local case_name="$4"
  local error_code="$5"
  local today="${6:-}"
  local match
  local status
  local reason
  local expires

  if [ -z "$today" ]; then
    today="$(date -u +"%Y-%m-%d")"
  fi

  match="$(ci_known_failure_match "$baseline" "$tier" "$suite" "$case_name" "$error_code")"
  IFS='|' read -r status reason expires <<EOF
$match
EOF

  if [ "$status" = "KNOWN_FAIL" ] && [ -n "$expires" ] && [ "$expires" \< "$today" ]; then
    printf "EXPIRED_KNOWN_FAIL|%s|%s\n" "$reason" "$expires"
  else
    printf "%s\n" "$match"
  fi
}
