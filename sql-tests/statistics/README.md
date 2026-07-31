<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# statistics

Focused statistics regressions for the 128-bit `LARGEINT` path inherited from
the former low-cardinality companion suite. The suite is runner-managed native
`1FE+3BE`, backed by SQLite StateStore and the shared Iceberg REST fixture: an
`ANALYZE` assertion therefore exercises durable job ownership and distributed
collection instead of the standalone convenience path.

The suite deliberately uses small tables. Its purpose is statistics collection
and plan visibility, not aggregate or low-cardinality runtime behavior.

`iceberg_statistics_puffin_read_by_spark.sql` is the bidirectional Iceberg
interop acceptance: Spark creates a REST-catalog table, NovaRocks publishes
statistics through native `ANALYZE`, then Spark's Iceberg `Table` API reads the
published `StatisticsFile` and verifies standard Apache DataSketches Theta
Puffin metadata.
