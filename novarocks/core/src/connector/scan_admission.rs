// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Provider scan-admission leaves over an exact planning lease.

pub(crate) fn admit_connector_change_window(
    table: &novarocks_spi::connector::ConnectorTableHandle,
    schema: &arrow::datatypes::SchemaRef,
    planning_lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
    context: novarocks_spi::connector::ConnectorRequestContext,
    window: novarocks_spi::connector::ConnectorChangeWindow,
) -> Result<novarocks_spi::connector::ConnectorScan, String> {
    use novarocks_spi::connector::{
        ConnectorBatchBudget, ConnectorBeginScanRequest, ConnectorReadPurpose,
        ConnectorScanSelection,
    };

    let binding = planning_lease.binding();
    if table.owner() != &binding.descriptor().instance_id {
        return Err(
            "connector change-window table handle owner does not match its exact planning lease"
                .to_string(),
        );
    }
    let scan = binding
        .planning()
        .begin_scan(
            table,
            ConnectorBeginScanRequest {
                projection: (0..schema.fields().len()).collect(),
                static_predicates: Vec::new(),
                selection: ConnectorScanSelection::ChangeWindow(window),
                purpose: ConnectorReadPurpose::Query,
                limit: None,
                batch: ConnectorBatchBudget {
                    max_rows: std::num::NonZeroUsize::new(4096).expect("batch rows are nonzero"),
                    max_bytes: std::num::NonZeroUsize::new(
                        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                    )
                    .expect("batch bytes are nonzero"),
                },
                context: context.clone(),
            },
        )
        .map_err(|error| error.to_string())?;
    scan.validate(
        &novarocks_spi::connector::ConnectorExecutionBindingKey {
            instance_id: binding.descriptor().instance_id.clone(),
            incarnation: binding.incarnation(),
        },
        ConnectorScanSelection::ChangeWindow(window),
    )
    .map_err(|error| error.to_string())?;
    if scan.output_schema().fields() != schema.fields() {
        return Err(
            "connector change-window scan schema does not match its exact table metadata"
                .to_string(),
        );
    }
    if !scan.predicate_dispositions().is_empty() {
        return Err(
            "connector change-window scan returned dispositions without static predicates"
                .to_string(),
        );
    }
    Ok(scan)
}
