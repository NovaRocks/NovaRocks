#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIXTURE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
bash -n "$FIXTURE_DIR/provision.sh" "$SCRIPT_DIR/run.sh"
python3 -m py_compile "$FIXTURE_DIR/fixture_inputs.py" "$FIXTURE_DIR/provision.py" "$SCRIPT_DIR/test_fixture_inputs.py"
python3 -m unittest discover -s "$SCRIPT_DIR" -p 'test_*.py' -v
