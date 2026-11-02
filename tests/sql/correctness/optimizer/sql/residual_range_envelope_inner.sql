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

-- @tags=optimizer,oq9,residual,range_envelope
-- Test Objective:
-- Derive conservative range envelopes from OR branches while preserving the
-- original OR predicate.
-- Preserve that original disjunction before its derived [10,40] envelope;
-- the envelope does not authorize reordering the original evaluation.
DROP TABLE IF EXISTS ${case_db}.residual_rng_l;
DROP TABLE IF EXISTS ${case_db}.residual_rng_r;
CREATE TABLE ${case_db}.residual_rng_l (k INT, score INT, payload INT);
CREATE TABLE ${case_db}.residual_rng_r (k INT, score INT, payload INT);

EXPLAIN VERBOSE
SELECT l.payload, r.payload
FROM ${case_db}.residual_rng_l l
JOIN ${case_db}.residual_rng_r r ON l.k = r.k
WHERE (l.score = r.score AND l.score BETWEEN 10 AND 20)
   OR (l.score = r.score AND l.score BETWEEN 30 AND 40);
