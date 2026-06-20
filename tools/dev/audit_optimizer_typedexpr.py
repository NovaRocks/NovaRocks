#!/usr/bin/env python3
from pathlib import Path
import argparse
import re
import sys

DEFAULT_ALLOW = {
    "src/sql/optimizer/scalar.rs",
    "src/sql/optimizer/scalar/mod.rs",
    "src/sql/optimizer/scalar_bridge.rs",
    "src/sql/optimizer/convert.rs",
    "src/sql/optimizer/rewrite/rules/subquery/bridge.rs",
}

PATTERN = re.compile(r"\b(TypedExpr|LogicalPlanNode|ProjectItem|SortItem|materialize)\b")


def starts_raw_string(line: str, i: int) -> tuple[int, int] | None:
    start = i
    if line.startswith("br", i):
        i += 2
    elif line.startswith("r", i):
        i += 1
    else:
        return None

    hashes_start = i
    while i < len(line) and line[i] == "#":
        i += 1
    if i < len(line) and line[i] == '"':
        return i - hashes_start, i + 1 - start
    return None


def char_literal_end(line: str, i: int) -> int | None:
    if i >= len(line) or line[i] != "'":
        return None
    j = i + 1
    escaped = False
    while j < len(line):
        ch = line[j]
        if escaped:
            escaped = False
        elif ch == "\\":
            escaped = True
        elif ch == "'":
            return j + 1
        j += 1
    return None


def is_lifetime_or_label_start(line: str, i: int) -> bool:
    if i + 1 >= len(line) or line[i] != "'":
        return False
    if not (line[i + 1].isalpha() or line[i + 1] == "_"):
        return False

    j = i + 2
    while j < len(line) and (line[j].isalnum() or line[j] == "_"):
        j += 1

    return j >= len(line) or line[j] != "'"


def mask_rust_non_code(line: str, state: dict[str, object]) -> tuple[str, dict[str, object]]:
    out = []
    i = 0
    while i < len(line):
        block_depth = int(state["block_depth"])
        raw_hashes = state["raw_hashes"]
        string_delim = state["string_delim"]
        escaped = bool(state["escaped"])

        if block_depth:
            if line.startswith("/*", i):
                state["block_depth"] = block_depth + 1
                out.append("  ")
                i += 2
                continue
            if line.startswith("*/", i):
                state["block_depth"] = block_depth - 1
                out.append("  ")
                i += 2
                continue
            out.append(" ")
            i += 1
            continue

        if raw_hashes is not None:
            end = '"' + ("#" * int(raw_hashes))
            if line.startswith(end, i):
                state["raw_hashes"] = None
                out.append(" " * len(end))
                i += len(end)
            else:
                out.append(" ")
                i += 1
            continue

        if string_delim is not None:
            ch = line[i]
            out.append(" ")
            if escaped:
                state["escaped"] = False
            elif ch == "\\":
                state["escaped"] = True
            elif ch == string_delim:
                state["string_delim"] = None
            i += 1
            continue

        if line.startswith("//", i):
            out.append(" " * (len(line) - i))
            break

        if line.startswith("/*", i):
            state["block_depth"] = 1
            out.append("  ")
            i += 2
            continue

        raw_start = starts_raw_string(line, i)
        if raw_start is not None:
            hashes, consumed = raw_start
            state["raw_hashes"] = hashes
            out.append(" " * consumed)
            i += consumed
            continue

        if line.startswith('b"', i):
            state["string_delim"] = '"'
            state["escaped"] = False
            out.append("  ")
            i += 2
            continue

        if line[i] == '"':
            state["string_delim"] = '"'
            state["escaped"] = False
            out.append(" ")
            i += 1
            continue

        if line.startswith("b'", i):
            end = char_literal_end(line, i + 1)
            if end is not None:
                out.append(" " * (end - i))
                i = end
                continue

        if line[i] == "'" and not is_lifetime_or_label_start(line, i):
            end = char_literal_end(line, i)
            if end is not None:
                out.append(" " * (end - i))
                i = end
                continue

        out.append(line[i])
        i += 1

    return "".join(out), state


def brace_delta(line: str) -> int:
    return line.count("{") - line.count("}")


def skip_test_item_state(line: str) -> tuple[bool, int | None]:
    if "{" in line:
        depth = brace_delta(line)
        return False, depth if depth > 0 else None
    if ";" in line:
        return False, None
    return True, None


def production_hits(path: Path):
    pending_test_item = False
    test_item_brace_depth = None
    scanner_state: dict[str, object] = {
        "block_depth": 0,
        "raw_hashes": None,
        "string_delim": None,
        "escaped": False,
    }

    for lineno, line in enumerate(path.read_text().splitlines(), 1):
        code_line, scanner_state = mask_rust_non_code(line, scanner_state)
        stripped_code = code_line.strip()

        if test_item_brace_depth is not None:
            test_item_brace_depth += brace_delta(code_line)
            if test_item_brace_depth <= 0:
                test_item_brace_depth = None
            continue

        if pending_test_item:
            if stripped_code:
                pending_test_item, test_item_brace_depth = skip_test_item_state(code_line)
            continue

        if not stripped_code:
            continue

        if "#![cfg(test)]" in code_line:
            break

        test_attr_start = code_line.find("#[cfg(test)]")
        if test_attr_start != -1:
            after_attr = code_line[test_attr_start + len("#[cfg(test)]") :]
            pending_test_item, test_item_brace_depth = skip_test_item_state(after_attr)
            continue

        if PATTERN.search(code_line):
            yield lineno, line.rstrip()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--allow", action="append", default=[])
    args = parser.parse_args()

    allowed = set(DEFAULT_ALLOW)
    allowed.update(args.allow)

    failed = False
    for path in sorted(Path("src/sql/optimizer").rglob("*.rs")):
        rel = path.as_posix()
        hits = list(production_hits(path))
        if not hits:
            continue
        if rel in allowed:
            continue
        failed = True
        print(rel)
        for lineno, line in hits:
            print(f"  {lineno}: {line}")

    return 1 if failed and args.strict else 0


if __name__ == "__main__":
    sys.exit(main())
