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

//! Low-cardinality terminal observations for statement-local DML publications.

use once_cell::sync::Lazy;
use prometheus::{IntCounterVec, Opts, Registry};

use novarocks_spi::connector::{LakePublicationDisposition, LakePublicationFamily};

use crate::dml::attempt::{
    DmlPublicationFinalization, DmlPublicationPhase, publication_disposition_name,
};

static DML_PUBLICATION_TERMINALS: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "novarocks_dml_publication_terminal_total",
            "Terminal outcomes of statement-local DML publications.",
        ),
        &["family", "phase", "disposition", "finalization"],
    )
    .expect("register novarocks_dml_publication_terminal_total")
});

pub(crate) fn register_collectors(registry: &Registry) -> Result<(), String> {
    registry
        .register(Box::new(DML_PUBLICATION_TERMINALS.clone()))
        .map_err(|error| format!("register DML publication metrics failed: {error}"))
}

pub(crate) fn observe_terminal(
    family: LakePublicationFamily,
    phase: DmlPublicationPhase,
    disposition: LakePublicationDisposition,
    finalization: DmlPublicationFinalization,
) {
    DML_PUBLICATION_TERMINALS
        .with_label_values(&[
            family.as_str(),
            phase.as_str(),
            publication_disposition_name(disposition),
            finalization.as_str(),
        ])
        .inc();
}

pub(crate) fn ensure_label_families() {
    for family in [
        LakePublicationFamily::Write,
        LakePublicationFamily::DataMutation,
        LakePublicationFamily::Ctas,
    ] {
        for phase in [
            DmlPublicationPhase::PreDispatch,
            DmlPublicationPhase::DispatchPossible,
        ] {
            for disposition in [
                LakePublicationDisposition::KnownUncommitted,
                LakePublicationDisposition::CommitUnknown,
                LakePublicationDisposition::KnownCommitted,
            ] {
                let finalizations: &[DmlPublicationFinalization] = match disposition {
                    LakePublicationDisposition::KnownCommitted => &[
                        DmlPublicationFinalization::Succeeded,
                        DmlPublicationFinalization::Failed,
                    ],
                    LakePublicationDisposition::KnownUncommitted
                    | LakePublicationDisposition::CommitUnknown => {
                        &[DmlPublicationFinalization::NotApplicable]
                    }
                };
                for finalization in finalizations {
                    let _ = DML_PUBLICATION_TERMINALS.get_metric_with_label_values(&[
                        family.as_str(),
                        phase.as_str(),
                        publication_disposition_name(disposition),
                        finalization.as_str(),
                    ]);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use prometheus::{Encoder, Registry, TextEncoder};

    use super::*;

    #[test]
    fn terminal_metric_has_only_the_documented_low_cardinality_labels() {
        let registry = Registry::new();
        register_collectors(&registry).expect("register collectors");
        observe_terminal(
            LakePublicationFamily::Write,
            DmlPublicationPhase::DispatchPossible,
            LakePublicationDisposition::CommitUnknown,
            DmlPublicationFinalization::NotApplicable,
        );

        let mut rendered = Vec::new();
        TextEncoder::new()
            .encode(&registry.gather(), &mut rendered)
            .expect("encode metrics");
        let rendered = String::from_utf8(rendered).expect("metrics are utf-8");
        let line = rendered
            .lines()
            .find(|line| {
                line.starts_with("novarocks_dml_publication_terminal_total")
                    && line.contains("family=\"write\"")
                    && line.contains("phase=\"dispatch_possible\"")
                    && line.contains("disposition=\"commit_unknown\"")
                    && line.contains("finalization=\"not_applicable\"")
            })
            .expect("DML publication metric line");

        assert!(line.contains("family=\"write\""), "{line}");
        assert!(line.contains("phase=\"dispatch_possible\""), "{line}");
        assert!(line.contains("disposition=\"commit_unknown\""), "{line}");
        assert!(line.contains("finalization=\"not_applicable\""), "{line}");
        assert!(!line.contains("publication_id"), "{line}");
        assert!(!line.contains("catalog"), "{line}");
        assert!(!line.contains("namespace"), "{line}");
        assert!(!line.contains("table"), "{line}");
        assert!(!line.contains("statement_tag"), "{line}");
    }
}
