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

//! Application properties of the MV engine. Iceberg declares storage properties.

use std::{collections::BTreeSet, error::Error, fmt};

use novarocks_query_application::sql::property_evaluation::EvaluatedDdlProperty;
use novarocks_spi::connector::{
    DdlPropertyChange, DdlPropertyDeclaration, DdlPropertyDeclarationError, DdlPropertyType,
    DdlPropertyValue,
};

use crate::persistence::{
    definition::MvDesiredRefreshPolicy, semantic::MvRefreshDesiredConfiguration,
};
use crate::repository::InitialMvRefreshConfiguration;

const REFRESH_POLICY: &str = "refresh_policy";
const REFRESH_INTERVAL: &str = "refresh_interval";
const PRIMARY_KEY: &str = "primary_key";
const REFRESH_PAUSED: &str = "refresh_paused";

const SECOND_MS: u64 = 1_000;
const MINUTE_MS: u64 = 60 * SECOND_MS;
const HOUR_MS: u64 = 60 * MINUTE_MS;
const DAY_MS: u64 = 24 * HOUR_MS;

pub fn engine_property_declarations()
-> Result<Vec<DdlPropertyDeclaration>, DdlPropertyDeclarationError> {
    use DdlPropertyType as T;
    use DdlPropertyValue as V;
    let initial = InitialMvRefreshConfiguration::default();
    [
        (
            REFRESH_POLICY,
            T::Varchar,
            V::Varchar(refresh_policy_property_value(&initial.policy).into()),
        ),
        (REFRESH_INTERVAL, T::Varchar, V::Null),
        (PRIMARY_KEY, T::ArrayVarchar, V::Null),
        (REFRESH_PAUSED, T::Boolean, V::Boolean(initial.paused)),
    ]
    .into_iter()
    .map(|(name, data_type, default)| DdlPropertyDeclaration::new(name, data_type, default, false))
    .collect()
}

/// These are SQL property values. The durable policy names remain unchanged.
pub const fn refresh_policy_property_value(policy: &MvDesiredRefreshPolicy) -> &'static str {
    match policy {
        MvDesiredRefreshPolicy::Manual => "MANUAL",
        MvDesiredRefreshPolicy::AsyncOnChange => "ON_CHANGE",
        MvDesiredRefreshPolicy::AsyncInterval => "INTERVAL",
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvEnginePropertyErrorKind {
    InvalidValue,
    Unsupported,
    Invariant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvEnginePropertyError {
    kind: MvEnginePropertyErrorKind,
    property: String,
    message: String,
}

impl MvEnginePropertyError {
    pub const fn kind(&self) -> MvEnginePropertyErrorKind {
        self.kind
    }

    pub fn property(&self) -> &str {
        &self.property
    }
}

impl fmt::Display for MvEnginePropertyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}
impl Error for MvEnginePropertyError {}

fn invalid(property: &str, message: impl Into<String>) -> MvEnginePropertyError {
    MvEnginePropertyError {
        kind: MvEnginePropertyErrorKind::InvalidValue,
        property: property.into(),
        message: message.into(),
    }
}

fn invalid_evaluated(property: &str) -> MvEnginePropertyError {
    MvEnginePropertyError {
        kind: MvEnginePropertyErrorKind::Invariant,
        property: property.into(),
        message: format!("Invalid evaluated MV engine property '{property}'"),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvCreateEngineProperties {
    pub refresh: InitialMvRefreshConfiguration,
    pub primary_key: Option<Vec<String>>,
}

/// Consume only the evaluated engine subset. Storage properties are projected
/// by the selected provider. Table/schema-dependent key checks remain later.
pub fn prepare_create_engine_properties(
    properties: &[EvaluatedDdlProperty],
) -> Result<MvCreateEngineProperties, MvEnginePropertyError> {
    let mut refresh = MvRefreshPropertyChanges::default();
    let mut primary_key = None;
    for property in properties {
        if property.name == PRIMARY_KEY {
            let DdlPropertyChange::Set(DdlPropertyValue::ArrayVarchar(columns)) = &property.change
            else {
                // CREATE preparation omitted the NULL default already.
                return Err(invalid_evaluated(PRIMARY_KEY));
            };
            validate_primary_key_names(columns.iter().map(Option::as_deref))?;
            primary_key = Some(columns.iter().flatten().cloned().collect());
        } else {
            refresh.add(property)?;
        }
    }
    Ok(MvCreateEngineProperties {
        refresh: refresh.initial_configuration()?,
        primary_key,
    })
}

/// The legacy caller has already checked the interval syntax and its range.
/// Construct the same initial configuration used by evaluated CREATE properties.
pub fn prepare_legacy_create_refresh_configuration(
    policy: MvDesiredRefreshPolicy,
    interval_ms: Option<i64>,
) -> Result<InitialMvRefreshConfiguration, MvEnginePropertyError> {
    MvRefreshPropertyChanges::replace_policy(policy, interval_ms).initial_configuration()
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MvRefreshPropertyChanges {
    policy: Option<MvDesiredRefreshPolicy>,
    interval_ms: Option<Option<i64>>,
    paused: Option<bool>,
}

impl MvRefreshPropertyChanges {
    pub fn replace_policy(policy: MvDesiredRefreshPolicy, interval_ms: Option<i64>) -> Self {
        Self {
            policy: Some(policy),
            interval_ms: Some(interval_ms),
            paused: None,
        }
    }

    pub fn set_paused(paused: bool) -> Self {
        Self {
            paused: Some(paused),
            ..Self::default()
        }
    }

    pub fn from_alter_properties(
        properties: &[EvaluatedDdlProperty],
    ) -> Result<Self, MvEnginePropertyError> {
        let mut refresh = Self::default();
        for property in properties {
            if property.name == PRIMARY_KEY {
                return Err(MvEnginePropertyError {
                    kind: MvEnginePropertyErrorKind::Unsupported,
                    property: PRIMARY_KEY.into(),
                    message: "ALTER MATERIALIZED VIEW cannot change primary_key".into(),
                });
            }
            refresh.add(property)?;
        }
        Ok(refresh)
    }

    fn initial_configuration(self) -> Result<InitialMvRefreshConfiguration, MvEnginePropertyError> {
        let initial = InitialMvRefreshConfiguration::default();
        let refresh = self.apply(MvRefreshDesiredConfiguration {
            policy: initial.policy,
            paused: initial.paused,
            interval_ms: initial.interval_ms,
            max_staleness_ms: initial.max_staleness_ms,
        })?;
        Ok(InitialMvRefreshConfiguration {
            policy: refresh.policy,
            paused: refresh.paused,
            interval_ms: refresh.interval_ms,
            max_staleness_ms: refresh.max_staleness_ms,
        })
    }

    fn add(&mut self, property: &EvaluatedDdlProperty) -> Result<(), MvEnginePropertyError> {
        let value = match &property.change {
            DdlPropertyChange::Set(value) | DdlPropertyChange::Reset(value) => value,
        };
        match (property.name.as_str(), value) {
            (REFRESH_POLICY, DdlPropertyValue::Varchar(policy)) => {
                self.policy = Some(parse_refresh_policy(policy)?);
            }
            (REFRESH_INTERVAL, DdlPropertyValue::Varchar(interval)) => {
                self.interval_ms = Some(Some(parse_refresh_interval(interval)?));
            }
            (REFRESH_INTERVAL, DdlPropertyValue::Null)
                if matches!(&property.change, DdlPropertyChange::Reset(_)) =>
            {
                self.interval_ms = Some(None);
            }
            (REFRESH_PAUSED, DdlPropertyValue::Boolean(paused)) => self.paused = Some(*paused),
            (name, _) => return Err(invalid_evaluated(name)),
        }
        Ok(())
    }

    /// Apply the entire change to one current configuration. An unspecified
    /// interval retains the current interval only for the INTERVAL policy.
    pub fn apply(
        &self,
        current: MvRefreshDesiredConfiguration,
    ) -> Result<MvRefreshDesiredConfiguration, MvEnginePropertyError> {
        let policy = self.policy.clone().unwrap_or(current.policy);
        let interval_ms = self.interval_ms.unwrap_or_else(|| {
            if policy == MvDesiredRefreshPolicy::AsyncInterval {
                current.interval_ms
            } else {
                None
            }
        });
        MvRefreshDesiredConfiguration::new(
            policy,
            self.paused.unwrap_or(current.paused),
            interval_ms,
            current.max_staleness_ms,
        )
        .map_err(|error| {
            let property = if self.interval_ms.is_some() {
                REFRESH_INTERVAL
            } else {
                REFRESH_POLICY
            };
            invalid(property, error)
        })
    }
}

fn parse_refresh_policy(text: &str) -> Result<MvDesiredRefreshPolicy, MvEnginePropertyError> {
    [
        MvDesiredRefreshPolicy::Manual,
        MvDesiredRefreshPolicy::AsyncOnChange,
        MvDesiredRefreshPolicy::AsyncInterval,
    ]
    .into_iter()
    .find(|policy| text.eq_ignore_ascii_case(refresh_policy_property_value(policy)))
    .ok_or_else(|| {
        invalid(
            REFRESH_POLICY,
            format!("Invalid MV refresh_policy '{text}'; expected MANUAL, ON_CHANGE, or INTERVAL"),
        )
    })
}

fn parse_refresh_interval(text: &str) -> Result<i64, MvEnginePropertyError> {
    let syntax = || {
        invalid(
            REFRESH_INTERVAL,
            "MV refresh_interval requires a positive decimal followed by s, m, h, or d",
        )
    };
    let (suffix, magnitude) = text.as_bytes().split_last().ok_or_else(syntax)?;
    let multiplier = match suffix {
        b's' => SECOND_MS,
        b'm' => MINUTE_MS,
        b'h' => HOUR_MS,
        b'd' => DAY_MS,
        _ => return Err(syntax()),
    };
    let number = &text[..magnitude.len()];
    let (whole, fraction) = match number.split_once('.') {
        Some((whole, fraction)) if decimal_digits(fraction) => (whole, fraction),
        Some(_) => return Err(syntax()),
        None => (number, ""),
    };
    if !decimal_digits(whole) {
        return Err(syntax());
    }
    let whole = whole.parse::<u64>().map_err(|_| interval_too_large())?;
    let mut fractional_millis = 0;
    for digit in fraction.bytes().rev() {
        // The accumulator stays below one unit. A discarded nonzero digit
        // would remain a positive fraction after every subsequent division.
        let scaled = u64::from(digit - b'0') * multiplier + fractional_millis;
        if scaled % 10 != 0 {
            return Err(invalid(
                REFRESH_INTERVAL,
                "MV refresh_interval must be an exact number of milliseconds",
            ));
        }
        fractional_millis = scaled / 10;
    }
    checked_interval_millis(whole, multiplier, fractional_millis)
}

fn decimal_digits(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit())
}

/// The original legacy literal author supplies the parsed unsigned magnitude.
/// This stage preserves its zero, invalid-unit and overflow refusal order.
pub fn legacy_refresh_interval_millis(
    amount: u64,
    unit: &str,
) -> Result<i64, MvEnginePropertyError> {
    if amount == 0 {
        return Err(nonpositive_interval());
    }
    let multiplier = match unit.to_ascii_uppercase().as_str() {
        "SECOND" | "SECONDS" => SECOND_MS,
        "MINUTE" | "MINUTES" => MINUTE_MS,
        "HOUR" | "HOURS" => HOUR_MS,
        "DAY" | "DAYS" => DAY_MS,
        _ => {
            return Err(invalid(
                REFRESH_INTERVAL,
                format!(
                    "unsupported REFRESH ASYNC interval unit `{unit}`; expected SECOND, MINUTE, HOUR, or DAY",
                ),
            ));
        }
    };
    checked_interval_millis(amount, multiplier, 0)
}

fn checked_interval_millis(
    whole: u64,
    multiplier: u64,
    fractional_millis: u64,
) -> Result<i64, MvEnginePropertyError> {
    if whole == 0 && fractional_millis == 0 {
        return Err(nonpositive_interval());
    }
    whole
        .checked_mul(multiplier)
        .and_then(|millis| millis.checked_add(fractional_millis))
        .and_then(|millis| i64::try_from(millis).ok())
        .ok_or_else(interval_too_large)
}

fn nonpositive_interval() -> MvEnginePropertyError {
    invalid(REFRESH_INTERVAL, "REFRESH ASYNC interval must be positive")
}

fn interval_too_large() -> MvEnginePropertyError {
    invalid(REFRESH_INTERVAL, "REFRESH ASYNC interval is too large")
}

/// Apply the same name/order check while the legacy caller owns parsed names.
pub fn validate_legacy_primary_key<'a>(
    columns: impl IntoIterator<Item = &'a str>,
) -> Result<(), MvEnginePropertyError> {
    validate_primary_key_names(columns.into_iter().map(Some))
}

fn validate_primary_key_names<'a>(
    columns: impl IntoIterator<Item = Option<&'a str>>,
) -> Result<(), MvEnginePropertyError> {
    let mut seen = BTreeSet::new();
    for column in columns {
        let name =
            column.ok_or_else(|| invalid(PRIMARY_KEY, "MV primary_key must not contain NULL"))?;
        if !seen.insert(name.to_ascii_lowercase()) {
            return Err(invalid(
                PRIMARY_KEY,
                format!("duplicate column `{name}` in PRIMARY KEY clause"),
            ));
        }
    }
    if seen.is_empty() {
        return Err(invalid(
            PRIMARY_KEY,
            "PRIMARY KEY clause requires at least one column",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(name: &str, value: DdlPropertyValue) -> EvaluatedDdlProperty {
        EvaluatedDdlProperty {
            name: name.into(),
            change: DdlPropertyChange::Set(value),
        }
    }

    #[test]
    fn create_applies_real_defaults_and_preserves_declared_primary_key_order() {
        use DdlPropertyValue as V;
        let defaults = prepare_create_engine_properties(&[]).unwrap();
        assert_eq!(defaults.refresh, InitialMvRefreshConfiguration::default());
        assert_eq!(defaults.primary_key, None);

        let evaluated = [
            set("refresh_policy", V::Varchar("interval".into())),
            set("refresh_interval", V::Varchar("2h".into())),
            set(
                "primary_key",
                V::ArrayVarchar(vec![Some("Second".into()), Some("first".into())]),
            ),
            set("refresh_paused", V::Boolean(true)),
        ];
        let create = prepare_create_engine_properties(&evaluated).unwrap();
        assert_eq!(
            create.primary_key,
            Some(vec!["Second".into(), "first".into()])
        );
        assert_eq!(
            create.refresh,
            InitialMvRefreshConfiguration {
                policy: MvDesiredRefreshPolicy::AsyncInterval,
                paused: true,
                interval_ms: Some(7_200_000),
                max_staleness_ms: None,
            }
        );
        let duplicate = prepare_create_engine_properties(&[set(
            "primary_key",
            V::ArrayVarchar(vec![Some("Id".into()), Some("ID".into())]),
        )])
        .unwrap_err();
        assert_eq!(duplicate.property(), "primary_key");
        assert_eq!(
            duplicate.to_string(),
            "duplicate column `ID` in PRIMARY KEY clause"
        );
        let null_child =
            prepare_create_engine_properties(&[set("primary_key", V::ArrayVarchar(vec![None]))])
                .unwrap_err();
        assert_eq!(null_child.kind(), MvEnginePropertyErrorKind::InvalidValue);
    }

    #[test]
    fn alter_applies_the_batch_while_retaining_untouched_durable_facts() {
        use DdlPropertyValue as V;
        let current = MvRefreshDesiredConfiguration::new(
            MvDesiredRefreshPolicy::AsyncInterval,
            true,
            Some(123),
            Some(60_000),
        )
        .unwrap();
        let resumed = MvRefreshPropertyChanges::from_alter_properties(&[set(
            "refresh_paused",
            V::Boolean(false),
        )])
        .unwrap()
        .apply(current.clone())
        .unwrap();
        assert_eq!(resumed.interval_ms, Some(123));
        assert_eq!(resumed.max_staleness_ms, Some(60_000));
        assert!(!resumed.paused);

        let interval_default = EvaluatedDdlProperty {
            name: "refresh_interval".into(),
            change: DdlPropertyChange::Reset(V::Null),
        };
        let error = MvRefreshPropertyChanges::from_alter_properties(std::slice::from_ref(
            &interval_default,
        ))
        .unwrap()
        .apply(current.clone())
        .unwrap_err();
        assert_eq!(error.property(), "refresh_interval");
        assert_eq!(
            error.to_string(),
            "MV refresh policy ASYNC_INTERVAL requires a positive interval_ms"
        );
        let policy = set("refresh_policy", V::Varchar("ON_CHANGE".into()));
        for assignments in [vec![policy.clone()], vec![interval_default, policy]] {
            let desired = MvRefreshPropertyChanges::from_alter_properties(&assignments)
                .unwrap()
                .apply(current.clone())
                .unwrap();
            assert_eq!(
                desired,
                MvRefreshDesiredConfiguration {
                    policy: MvDesiredRefreshPolicy::AsyncOnChange,
                    interval_ms: None,
                    ..current.clone()
                }
            );
        }
        let unsupported = MvRefreshPropertyChanges::from_alter_properties(&[set(
            "primary_key",
            V::ArrayVarchar(vec![Some("id".into())]),
        )])
        .unwrap_err();
        assert_eq!(unsupported.kind(), MvEnginePropertyErrorKind::Unsupported);
    }

    #[test]
    fn decimal_intervals_represent_existing_milliseconds_exactly() {
        for (text, expected) in [
            ("0.123s", 123),
            ("0.123000000000000000000000000000000s", 123),
            ("1.5h", 5_400_000),
            ("0.0000003125d", 27),
            ("9223372036854775.807s", i64::MAX),
        ] {
            assert_eq!(parse_refresh_interval(text).unwrap(), expected, "{text}");
        }
        for text in ["0.0001s", "0.00000031249d"] {
            assert_eq!(
                parse_refresh_interval(text).unwrap_err().to_string(),
                "MV refresh_interval must be an exact number of milliseconds",
                "{text}",
            );
        }
        assert_eq!(
            parse_refresh_interval("9223372036854775.808s")
                .unwrap_err()
                .to_string(),
            "REFRESH ASYNC interval is too large",
        );
        for text in [".123s", "0.s", "0.1.2s", "1e1s", "0.123ms"] {
            assert!(parse_refresh_interval(text).is_err(), "{text}");
        }
    }

    #[test]
    fn intervals_use_the_original_signed_millisecond_range_and_legacy_error_order() {
        let last = i64::MAX as u64 / 1_000;
        let milliseconds = (last * 1_000) as i64;
        assert_eq!(
            legacy_refresh_interval_millis(last, "Seconds").unwrap(),
            milliseconds
        );
        assert_eq!(
            parse_refresh_interval(&format!("{last}s")).unwrap(),
            milliseconds
        );
        assert_eq!(
            legacy_refresh_interval_millis(0, "WEEKS")
                .unwrap_err()
                .to_string(),
            "REFRESH ASYNC interval must be positive",
        );
        assert_eq!(
            legacy_refresh_interval_millis(1, "WEEKS")
                .unwrap_err()
                .to_string(),
            "unsupported REFRESH ASYNC interval unit `WEEKS`; expected SECOND, MINUTE, HOUR, or DAY",
        );
        for error in [
            legacy_refresh_interval_millis(last + 1, "SECOND").unwrap_err(),
            legacy_refresh_interval_millis(i64::MAX as u64, "SECOND").unwrap_err(),
            parse_refresh_interval(&format!("{}s", last + 1)).unwrap_err(),
        ] {
            assert_eq!(error.to_string(), "REFRESH ASYNC interval is too large");
        }
        for text in ["0s", "2ms", "-1m", "1w"] {
            assert_eq!(
                parse_refresh_interval(text).unwrap_err().kind(),
                MvEnginePropertyErrorKind::InvalidValue
            );
        }
    }
}
