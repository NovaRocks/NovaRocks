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

//! One borrowed recursive Debug author for ordinary and observed scalar views.

use crate::aggregate_scalar::AggScalarValue;
use crate::decimal256_digits::{FixedDigitsEvent, fixed_digits};
use std::cell::RefCell;
use std::fmt::{self, Debug, Write};

pub trait ScalarDebugObserver {
    type Error;
    fn step(&mut self) -> Result<(), Self::Error>;
    fn boundary(&mut self) -> Result<(), Self::Error>;
    fn fixed_digits_event(&mut self, _: FixedDigitsEvent) -> Result<(), Self::Error> {
        self.step()
    }
}

pub enum ScalarDebugFailure<E> {
    Control(E),
    Formatting,
}

/// A borrowed root, not a copied scalar graph. JSONObject uses the inner roots.
pub enum ScalarDebugRoot<'a> {
    Scalar(&'a AggScalarValue),
    Sequence(&'a [Option<AggScalarValue>]),
    Pairs(&'a [(Option<AggScalarValue>, Option<AggScalarValue>)]),
}

trait Context {
    fn step(&self) -> fmt::Result;
    fn boundary(&self) -> fmt::Result;
    fn digits_event(&self, event: FixedDigitsEvent) -> fmt::Result;
    fn stopped(&self) -> bool;
    fn check(&self) -> fmt::Result {
        if self.stopped() {
            Err(fmt::Error)
        } else {
            Ok(())
        }
    }
}
struct Ordinary;
impl Context for Ordinary {
    fn step(&self) -> fmt::Result {
        Ok(())
    }
    fn boundary(&self) -> fmt::Result {
        Ok(())
    }
    fn digits_event(&self, _: FixedDigitsEvent) -> fmt::Result {
        Ok(())
    }
    // Public Formatter errors retain the original std builder propagation and
    // iteration. Only our owned writer has access to a first-refusal latch.
    fn stopped(&self) -> bool {
        false
    }
}

struct State<'a, O: ScalarDebugObserver> {
    observer: &'a mut O,
    cause: Option<O::Error>,
    formatting: bool,
}
struct Observed<'a, O: ScalarDebugObserver>(RefCell<State<'a, O>>);
impl<O: ScalarDebugObserver> Observed<'_, O> {
    fn observe(&self, f: impl FnOnce(&mut O) -> Result<(), O::Error>) -> fmt::Result {
        let mut state = self.0.borrow_mut();
        if state.cause.is_some() || state.formatting {
            return Err(fmt::Error);
        }
        match f(state.observer) {
            Ok(()) => Ok(()),
            Err(cause) => {
                state.cause = Some(cause);
                Err(fmt::Error)
            }
        }
    }
    fn formatting_failed(&self) {
        self.0.borrow_mut().formatting = true;
    }
}
impl<O: ScalarDebugObserver> Context for Observed<'_, O> {
    fn step(&self) -> fmt::Result {
        self.observe(|o| o.step())
    }
    fn boundary(&self) -> fmt::Result {
        self.observe(|o| o.boundary())
    }
    fn digits_event(&self, event: FixedDigitsEvent) -> fmt::Result {
        self.observe(|o| o.fixed_digits_event(event))
    }
    fn stopped(&self) -> bool {
        let state = self.0.borrow();
        state.cause.is_some() || state.formatting
    }
}

struct ObservedWriter<'a, 'o, W, O: ScalarDebugObserver> {
    destination: &'a mut W,
    context: &'a Observed<'o, O>,
}
impl<W: Write, O: ScalarDebugObserver> Write for ObservedWriter<'_, '_, W, O> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.context.boundary()?;
        for character in text.chars() {
            self.context.step()?;
            // No RefCell borrow crosses this real destination operation.
            if self.destination.write_char(character).is_err() {
                self.context.formatting_failed();
                return Err(fmt::Error);
            }
            self.context.boundary()?;
        }
        self.context.boundary()
    }
}

struct ScalarView<'a, C>(&'a AggScalarValue, &'a C);
struct OptionView<'a, C>(&'a Option<AggScalarValue>, &'a C);
struct PairView<'a, C>(&'a (Option<AggScalarValue>, Option<AggScalarValue>), &'a C);
enum Items<'a> {
    Scalars(&'a [Option<AggScalarValue>]),
    Pairs(&'a [(Option<AggScalarValue>, Option<AggScalarValue>)]),
    Bytes(&'a [u8]),
}
struct SequenceView<'a, C>(Items<'a>, &'a C);
struct DecimalView<'a, C>(&'a arrow_buffer::i256, &'a C);

fn tuple<C: Context>(
    name: &str,
    value: &impl Debug,
    context: &C,
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    let mut builder = f.debug_tuple(name);
    context.check()?;
    builder.field(value);
    context.check()?;
    builder.finish()
}

impl<C: Context> Debug for ScalarView<'_, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.1.step()?;
        use AggScalarValue::*;
        match self.0 {
            Bool(v) => tuple("Bool", v, self.1, f),
            Int64(v) => tuple("Int64", v, self.1, f),
            Float64(v) => tuple("Float64", v, self.1, f),
            Utf8(v) => tuple("Utf8", v, self.1, f),
            Date32(v) => tuple("Date32", v, self.1, f),
            Timestamp(v) => tuple("Timestamp", v, self.1, f),
            Decimal128(v) => tuple("Decimal128", v, self.1, f),
            Decimal256(v) => tuple("Decimal256", &DecimalView(v, self.1), self.1, f),
            Binary(v) => tuple("Binary", &SequenceView(Items::Bytes(v), self.1), self.1, f),
            Struct(v) => tuple(
                "Struct",
                &SequenceView(Items::Scalars(v), self.1),
                self.1,
                f,
            ),
            Map(v) => tuple("Map", &SequenceView(Items::Pairs(v), self.1), self.1, f),
            List(v) => tuple("List", &SequenceView(Items::Scalars(v), self.1), self.1, f),
        }
    }
}
impl<C: Context> Debug for OptionView<'_, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.1.step()?;
        match self.0 {
            None => f.write_str("None"),
            Some(value) => tuple("Some", &ScalarView(value, self.1), self.1, f),
        }
    }
}
impl<C: Context> Debug for PairView<'_, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.1.step()?;
        let mut builder = f.debug_tuple("");
        self.1.check()?;
        builder.field(&OptionView(&self.0.0, self.1));
        self.1.check()?;
        builder.field(&OptionView(&self.0.1, self.1));
        self.1.check()?;
        builder.finish()
    }
}
impl<C: Context> Debug for SequenceView<'_, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.1.step()?;
        let mut builder = f.debug_list();
        self.1.check()?;
        let len = match &self.0 {
            Items::Scalars(values) => values.len(),
            Items::Pairs(values) => values.len(),
            Items::Bytes(values) => values.len(),
        };
        for index in 0..len {
            self.1.check()?;
            self.1.step()?;
            match &self.0 {
                Items::Scalars(values) => builder.entry(&OptionView(&values[index], self.1)),
                Items::Pairs(values) => builder.entry(&PairView(&values[index], self.1)),
                Items::Bytes(values) => builder.entry(&values[index]),
            };
            self.1.check()?;
        }
        builder.finish()
    }
}
impl<C: Context> Debug for DecimalView<'_, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let digits = fixed_digits(self.0.to_le_bytes(), |event| self.1.digits_event(event))?;
        self.1.boundary()?;
        // Original i256 Debug rebuilds a default Display argument. Its leaf
        // does not inherit the outer width/precision/sign/hex flags.
        if digits.negative() {
            f.write_str("-")?;
            self.1.check()?;
        }
        f.write_str(digits.as_str())?;
        self.1.check()
    }
}

struct RootView<'a, C>(ScalarDebugRoot<'a>, &'a C);
impl<C: Context> Debug for RootView<'_, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            ScalarDebugRoot::Scalar(value) => ScalarView(value, self.1).fmt(f),
            ScalarDebugRoot::Sequence(values) => {
                SequenceView(Items::Scalars(values), self.1).fmt(f)
            }
            ScalarDebugRoot::Pairs(values) => SequenceView(Items::Pairs(values), self.1).fmt(f),
        }
    }
}

pub(crate) fn fmt_scalar(value: &AggScalarValue, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    ScalarView(value, &Ordinary).fmt(f)
}

/// The caller owns the destination and its allocation authority. Views and
/// first-cause storage are inline; no borrow crosses child formatting or write.
pub fn write_observed<O: ScalarDebugObserver, W: Write>(
    value: ScalarDebugRoot<'_>,
    destination: &mut W,
    observer: &mut O,
) -> Result<(), ScalarDebugFailure<O::Error>> {
    let context = Observed(RefCell::new(State {
        observer,
        cause: None,
        formatting: false,
    }));
    let result = fmt::write(
        &mut ObservedWriter {
            destination,
            context: &context,
        },
        format_args!("{:?}", RootView(value, &context)),
    );
    let state = context.0.into_inner();
    if let Some(cause) = state.cause {
        Err(ScalarDebugFailure::Control(cause))
    } else if state.formatting || result.is_err() {
        Err(ScalarDebugFailure::Formatting)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
