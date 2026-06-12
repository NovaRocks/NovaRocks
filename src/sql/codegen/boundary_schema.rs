use arrow::datatypes::DataType;

use crate::sql::codegen::OutputColumn;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BoundaryKind {
    ExchangeSender,
    ExchangeReceiver,
    RemoteRoot,
    ResultRoot,
}

impl BoundaryKind {
    pub(crate) fn label(self) -> &'static str {
        match self {
            BoundaryKind::ExchangeSender => "EXCHANGE_SEND",
            BoundaryKind::ExchangeReceiver => "EXCHANGE_RECV",
            BoundaryKind::RemoteRoot => "REMOTE_ROOT",
            BoundaryKind::ResultRoot => "RESULT_ROOT",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BoundarySchemaColumn {
    pub slot_id: i32,
    pub name: String,
    pub arrow_type: DataType,
    pub logical_type: Option<String>,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BoundarySchemaReport {
    pub fragment_id: Option<i32>,
    pub node_id: i32,
    pub boundary_kind: BoundaryKind,
    pub columns: Vec<BoundarySchemaColumn>,
}

pub(crate) fn output_columns_to_boundary_columns(
    outputs: &[OutputColumn],
) -> Vec<BoundarySchemaColumn> {
    outputs
        .iter()
        .enumerate()
        .map(|(idx, output)| BoundarySchemaColumn {
            slot_id: (idx + 1) as i32,
            name: output.name.clone(),
            arrow_type: output.data_type.clone(),
            logical_type: None,
            nullable: output.nullable,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::{BoundaryKind, output_columns_to_boundary_columns};
    use crate::sql::codegen::OutputColumn;

    #[test]
    fn boundary_kind_labels_are_stable() {
        assert_eq!(BoundaryKind::ExchangeSender.label(), "EXCHANGE_SEND");
        assert_eq!(BoundaryKind::ExchangeReceiver.label(), "EXCHANGE_RECV");
        assert_eq!(BoundaryKind::RemoteRoot.label(), "REMOTE_ROOT");
        assert_eq!(BoundaryKind::ResultRoot.label(), "RESULT_ROOT");
    }

    #[test]
    fn output_columns_convert_to_one_based_boundary_columns() {
        let columns = vec![
            OutputColumn {
                name: "k1".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            OutputColumn {
                name: "payload".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
        ];

        let converted = output_columns_to_boundary_columns(&columns);

        assert_eq!(converted.len(), 2);
        assert_eq!(converted[0].slot_id, 1);
        assert_eq!(converted[0].name, "k1");
        assert_eq!(converted[0].arrow_type, DataType::Int64);
        assert_eq!(converted[0].logical_type, None);
        assert!(!converted[0].nullable);
        assert_eq!(converted[1].slot_id, 2);
        assert_eq!(converted[1].name, "payload");
        assert_eq!(converted[1].arrow_type, DataType::Utf8);
        assert_eq!(converted[1].logical_type, None);
        assert!(converted[1].nullable);
    }
}
