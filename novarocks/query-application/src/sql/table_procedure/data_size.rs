// SPDX-License-Identifier: Apache-2.0

use novarocks_spi::connector::ConnectorDataSize;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

use super::{ArgumentContext, QueryServiceError, invalid_procedure_argument};

pub(super) fn parse_file_size_threshold(
    binding: ArgumentContext<'_>,
    text: &str,
    control: &dyn PureCompileControl,
) -> Result<ConnectorDataSize, QueryServiceError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)
        .map_err(QueryServiceError::from_compile_control)?;
    let parsed = ConnectorDataSize::parse(
        text,
        || work.step().map_err(QueryServiceError::from_compile_control),
        |failure, field| {
            invalid_procedure_argument(
                binding.sql,
                binding.span,
                format!(
                    "Unable to set catalog '{}' table procedure '{}' property 'file_size_threshold' to ['{text}']: {}",
                    binding.catalog,
                    binding.procedure,
                    failure.message(field),
                ),
            )
        },
    );
    work.finish_result(parsed, QueryServiceError::from_compile_control)
}
