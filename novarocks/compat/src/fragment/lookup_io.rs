use std::sync::Arc;

use novarocks::runtime::fragment::{
    FragmentIoError, FragmentIoErrorKind, FragmentIoOperation, FragmentLookupClient, LookupBatch,
    LookupColumn, LookupKind, LookupRequest,
};

pub(crate) fn brpc_fragment_lookup_client() -> Arc<dyn FragmentLookupClient> {
    Arc::new(BrpcFragmentLookupClient)
}

struct BrpcFragmentLookupClient;

impl FragmentLookupClient for BrpcFragmentLookupClient {
    fn lookup(&self, request: LookupRequest) -> Result<LookupBatch, FragmentIoError> {
        let local_backend_id = novarocks::runtime::backend_id::backend_id()
            .ok_or_else(|| {
                lookup_error(
                    FragmentIoErrorKind::Unavailable,
                    "backend_id is not initialized",
                )
            })
            .and_then(|id| {
                i32::try_from(id).map_err(|error| {
                    lookup_error(
                        FragmentIoErrorKind::Internal,
                        format!("backend_id does not fit in int32: {error}"),
                    )
                })
            })?;
        if request.target().backend_id() == local_backend_id {
            return local_lookup(request);
        }

        let endpoint = request.target().endpoint().ok_or_else(|| {
            lookup_error(
                FragmentIoErrorKind::InvalidResponse,
                format!(
                    "lookup target {} has no endpoint",
                    request.target().backend_id()
                ),
            )
        })?;
        let port = u16::try_from(endpoint.port()).map_err(|error| {
            lookup_error(
                FragmentIoErrorKind::InvalidResponse,
                format!("invalid BRPC lookup port: {error}"),
            )
        })?;
        let response =
            crate::internal_rpc_client::lookup(endpoint.host(), port, remote_request(&request)?)
                .map_err(|error| lookup_error(FragmentIoErrorKind::Unavailable, error))?;
        decode_response(response)
    }
}

fn local_lookup(request: LookupRequest) -> Result<LookupBatch, FragmentIoError> {
    let output = match request.kind() {
        LookupKind::PrimaryKey => novarocks::runtime::lookup::execute_lookup_request(
            request.query_id(),
            request.tuple_id(),
            request
                .columns()
                .iter()
                .map(|column| (column.slot_id(), column.values().clone()))
                .collect(),
        ),
        LookupKind::Lake => {
            return Err(lookup_error(
                FragmentIoErrorKind::Internal,
                "lake late-materialization lookup is retired",
            ));
        }
    }
    .map_err(|error| lookup_error(FragmentIoErrorKind::Internal, error))?;
    Ok(LookupBatch::new(
        output
            .into_iter()
            .map(|(slot_id, values)| LookupColumn::new(slot_id, values))
            .collect(),
    ))
}

fn remote_request(
    request: &LookupRequest,
) -> Result<novarocks::proto::filter::LookupRequest, FragmentIoError> {
    let mut output = novarocks::proto::filter::LookupRequest {
        query_id: Some(novarocks::proto::common::UniqueId {
            hi: request.query_id().high(),
            lo: request.query_id().low(),
        }),
        lookup_node_id: request.lookup_node_id(),
        request_tuple_id: request.tuple_id(),
        request_columns: Vec::with_capacity(request.columns().len()),
    };
    for column in request.columns() {
        let data = novarocks::runtime::lookup::encode_column_ipc(column.values())
            .map_err(|error| lookup_error(FragmentIoErrorKind::Internal, error))?;
        output
            .request_columns
            .push(novarocks::proto::filter::Column {
                slot_id: column.slot_id().as_u32() as i32,
                data_size: data.len() as i64,
                data,
            });
    }
    Ok(output)
}

fn decode_response(
    response: novarocks::proto::filter::LookupResponse,
) -> Result<LookupBatch, FragmentIoError> {
    if let Some(status) = response.status.as_ref()
        && status.code != 0
    {
        return Err(lookup_error(
            FragmentIoErrorKind::RemoteRejected,
            format!("lookup failed: {}", status.message),
        ));
    }
    let mut columns = Vec::with_capacity(response.columns.len());
    for column in response.columns {
        if column.data.is_empty() {
            return Err(lookup_error(
                FragmentIoErrorKind::InvalidResponse,
                "lookup response column missing data",
            ));
        }
        let slot_id =
            novarocks::common::ids::SlotId::try_from(column.slot_id).map_err(|error| {
                lookup_error(FragmentIoErrorKind::InvalidResponse, error.to_string())
            })?;
        let values = novarocks::runtime::lookup::decode_column_ipc(&column.data)
            .map_err(|error| lookup_error(FragmentIoErrorKind::InvalidResponse, error))?;
        columns.push(LookupColumn::new(slot_id, values));
    }
    Ok(LookupBatch::new(columns))
}

fn lookup_error(kind: FragmentIoErrorKind, message: impl Into<String>) -> FragmentIoError {
    FragmentIoError::new(FragmentIoOperation::Lookup, kind, message)
}
