// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import io.trino.spi.type.TimeZoneKey;
import io.trino.util.DateTimeZoneIndex;
import org.joda.time.DateTimeZone;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.zone.ZoneRules;
import java.time.zone.ZoneRulesProvider;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongToIntFunction;
import java.util.function.LongUnaryOperator;

class ExportTemporalRules {
    // Region-bearing text casts can produce an unzoned timestamp across
    // the signed microsecond range. Pad the corresponding millisecond domain
    // by Joda's maximum representable offset for the adjacent lookup segments.
    // Zoned timestamp values independently enforce signed 52-bit milliseconds.
    static final long DOMAIN_START = Math.floorDiv(Long.MIN_VALUE, 1000) - (1L << 31);
    static final long DOMAIN_END = Math.floorDiv(Long.MAX_VALUE, 1000) + (1L << 31) + 1;
    static final long CYCLE_START = Instant.parse("2400-01-01T00:00:00Z").toEpochMilli();
    static final long CYCLE_MILLIS = 146097 * 86400_000L;
    static final int NO_CYCLE = -1;

    record Transition(long millis, int offsetSeconds) {}
    record Table(int firstOffsetSeconds, int cycleIndex, long nextAfterDomain, List<Transition> transitions) {}
    record Input(LongToIntFunction offsetMillis, LongUnaryOperator nextTransitionMillis) {}
    record RulePair(int jdk, int joda) {}

    private final Map<Table, Integer> tables = new LinkedHashMap<>();

    public static void main(String[] args) throws IOException {
        new ExportTemporalRules().export(Path.of(args[0]));
    }

    void export(Path output) throws IOException {
        Files.createDirectories(output);
        for (int minutes = -840; minutes <= 840; minutes++) {
            int expectedId = minutes == 0 ? 0 : minutes < 0 ? minutes + 841 : minutes + 840;
            if (TimeZoneKey.getTimeZoneKeyForOffset(minutes).getKey() != expectedId) {
                throw new IllegalStateException("Unexpected fixed offset key");
            }
        }
        List<TimeZoneKey> keys = TimeZoneKey.getTimeZoneKeys().stream()
                .sorted(Comparator.comparingInt(TimeZoneKey::getKey)).toList();
        Map<ZoneRules, Integer> jdkIds = new HashMap<>();
        Map<DateTimeZone, Integer> jodaIds = new HashMap<>();
        Map<Short, RulePair> keyRules = new LinkedHashMap<>();
        for (TimeZoneKey key : keys) {
            ZoneRules jdk = key.getZoneId().getRules();
            DateTimeZone joda = DateTimeZoneIndex.getDateTimeZone(key);
            int jdkTable = jdkIds.computeIfAbsent(jdk, rules -> intern(read(jdkInput(rules))));
            int jodaTable = jodaIds.computeIfAbsent(joda, rules -> intern(read(jodaInput(rules))));
            keyRules.put(key.getKey(), new RulePair(jdkTable, jodaTable));
        }
        writeIds(output, keys);
        writeRules(output, keyRules);
        System.out.printf("Runtime %s, JDK TZDB %s, Joda %s%n", Runtime.version(),
                ZoneRulesProvider.getVersions("America/New_York").keySet(),
                DateTimeZone.class.getPackage().getImplementationVersion());
        System.out.printf("%d time zone keys, %d distinct rule tables (%d with proved cycles)%n",
                keys.size(), tables.size(), tables.keySet().stream().filter(t -> t.cycleIndex() != NO_CYCLE).count());
        System.out.printf("%d JDK and %d Joda rule objects%n", jdkIds.size(), jodaIds.size());
        System.out.printf("%d retained transitions for [%d, %d)%n",
                tables.keySet().stream().mapToInt(t -> t.transitions().size()).sum(), DOMAIN_START, DOMAIN_END);
    }

    private static Input jdkInput(ZoneRules rules) {
        return new Input(ms -> rules.getOffset(Instant.ofEpochMilli(ms)).getTotalSeconds() * 1000,
                ms -> {
                    var transition = rules.nextTransition(Instant.ofEpochMilli(ms));
                    return transition == null ? ms : transition.getInstant().toEpochMilli();
                });
    }

    private static Input jodaInput(DateTimeZone rules) {
        return new Input(rules::getOffset, rules::nextTransition);
    }

    private int intern(Table table) {
        return tables.computeIfAbsent(table, ignored -> tables.size());
    }

    private static Table read(Input source) {
        int initial = offsetSeconds(source, DOMAIN_START);
        var transitions = new ArrayList<Transition>();
        long previous = DOMAIN_START;
        long nextAfterDomain;
        while (true) {
            long next = source.nextTransitionMillis().applyAsLong(previous);
            if (next == previous || next >= DOMAIN_END) {
                nextAfterDomain = next == previous ? Long.MAX_VALUE : next;
                break;
            }
            if (next < previous) {
                throw new IllegalStateException("Non-monotonic transition");
            }
            transitions.add(new Transition(next, offsetSeconds(source, next)));
            previous = next;
        }
        int finalOffset = transitions.isEmpty() ? initial : transitions.getLast().offsetSeconds();
        if (finalOffset != offsetSeconds(source, DOMAIN_END - 1)) {
            throw new IllegalStateException("Transition list does not cover the full domain");
        }
        return compress(initial, nextAfterDomain, transitions);
    }

    private static int offsetSeconds(Input input, long millis) {
        int offset = input.offsetMillis().applyAsInt(millis);
        if (offset % 1000 != 0) {
            throw new IllegalStateException("Offset is not a whole second");
        }
        return offset / 1000;
    }

    private static Table compress(int firstOffset, long nextAfterDomain, List<Transition> transitions) {
        int cycle = firstAtOrAfter(transitions, CYCLE_START);
        int end = firstAtOrAfter(transitions, CYCLE_START + CYCLE_MILLIS);
        if (cycle == transitions.size() || cycle == end) {
            return new Table(firstOffset, NO_CYCLE, nextAfterDomain, List.copyOf(transitions));
        }
        int beforeCycle = cycle == 0 ? firstOffset : transitions.get(cycle - 1).offsetSeconds();
        int span = end - cycle;
        if (beforeCycle != transitions.get(end - 1).offsetSeconds()) {
            return new Table(firstOffset, NO_CYCLE, nextAfterDomain, List.copyOf(transitions));
        }
        for (int index = end; index <= transitions.size(); index++) {
            int relative = index - cycle;
            Transition template = transitions.get(cycle + relative % span);
            long expectedAt = template.millis() + (relative / span) * CYCLE_MILLIS;
            if (index == transitions.size()) {
                if (expectedAt >= DOMAIN_END && expectedAt == nextAfterDomain) {
                    return new Table(firstOffset, cycle, nextAfterDomain, List.copyOf(transitions.subList(0, end)));
                }
            }
            else {
                Transition actual = transitions.get(index);
                if (actual.millis() == expectedAt && actual.offsetSeconds() == template.offsetSeconds()) {
                    continue;
                }
            }
            return new Table(firstOffset, NO_CYCLE, nextAfterDomain, List.copyOf(transitions));
        }
        throw new AssertionError("Expected an end-of-domain check");
    }

    private static int firstAtOrAfter(List<Transition> transitions, long millis) {
        int low = 0;
        int high = transitions.size();
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (transitions.get(mid).millis() < millis) {
                low = mid + 1;
            }
            else {
                high = mid;
            }
        }
        return low;
    }

    private static void writeIds(Path output, List<TimeZoneKey> keys) throws IOException {
        var byId = new StringBuilder();
        byId.append("// Generated by ExportTemporalRules.java.\n");
        byId.append("pub(super) static ZONE_IDS: &[&str] = &[\n");
        int index = 0;
        for (TimeZoneKey key : keys) {
            while (index < key.getKey()) {
                byId.append("    \"\",\n");
                index++;
            }
            if (!key.getId().matches("[A-Za-z0-9_./+:-]+")) {
                throw new IllegalStateException("Unexpected time zone id " + key.getId());
            }
            byId.append("    \"").append(key.getId()).append("\",\n");
            index++;
        }
        byId.append("];\n\n#[rustfmt::skip]\npub(super) static ZONES_BY_NAME: &[u16] = &[\n");
        int column = 0;
        for (TimeZoneKey key : keys.stream().sorted(Comparator.comparing(TimeZoneKey::getId)).toList()) {
            byId.append(column % 16 == 0 ? "    " : " ");
            byId.append(key.getKey()).append(',');
            if (++column % 16 == 0) {
                byId.append('\n');
            }
        }
        if (column % 16 != 0) {
            byId.append('\n');
        }
        byId.append("];\n");
        Files.writeString(output.resolve("temporal_zone_ids.rs"), byId);
    }

    private void writeRules(Path output, Map<Short, RulePair> keyRules) throws IOException {
        // Big-endian, compact index plus records. Every record starts with
        // first offset, cycle index (-1 if none), entry count and the next
        // transition after the lookup domain (Long.MAX_VALUE at exhaustion).
        // Each transition occupies (i64 UTC milliseconds, i32 offset seconds).
        try (var out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(output.resolve("temporal_zone_rules.bin"))))) {
            out.writeBytes("NRTZ");
            out.writeInt(1);
            out.writeLong(DOMAIN_START);
            out.writeLong(DOMAIN_END);
            out.writeLong(CYCLE_START);
            out.writeLong(CYCLE_MILLIS);
            out.writeInt(TimeZoneKey.MAX_TIME_ZONE_KEY + 1);
            out.writeInt(tables.size());
            for (short index = 0; index <= TimeZoneKey.MAX_TIME_ZONE_KEY; index++) {
                RulePair pair = keyRules.get(index);
                out.writeShort(pair == null ? -1 : pair.jdk());
                out.writeShort(pair == null ? -1 : pair.joda());
            }
            long relativeOffset = 0;
            for (Table table : tables.keySet()) {
                out.writeInt(Math.toIntExact(relativeOffset));
                relativeOffset += 20 + 12L * table.transitions().size();
            }
            for (Table table : tables.keySet()) {
                out.writeInt(table.firstOffsetSeconds());
                out.writeInt(table.cycleIndex());
                out.writeInt(table.transitions().size());
                out.writeLong(table.nextAfterDomain());
                for (Transition transition : table.transitions()) {
                    out.writeLong(transition.millis());
                    out.writeInt(transition.offsetSeconds());
                }
            }
        }
    }
}
