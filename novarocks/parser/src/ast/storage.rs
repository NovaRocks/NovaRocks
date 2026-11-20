// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Stack-checked owning edges for recursive syntax facts.
//!
//! Every owning recursion cycle must cross one of these carriers. These
//! boundaries protect the carrier's recursive traits and destruction, not
//! arbitrary visitor, printer, parser, or analyzer implementations.

use std::fmt;
use std::ops::{Deref, DerefMut};

/// A boxed owning edge with protected recursive trait/destruction boundaries.
/// The internal empty state exists only during consumption/destruction.
pub struct AstBox<T> {
    inner: Option<Box<T>>,
}

impl<T> AstBox<T> {
    pub fn new(value: T) -> Self {
        Self::from_box(Box::new(value))
    }

    pub fn from_box(value: Box<T>) -> Self {
        Self { inner: Some(value) }
    }

    /// Consumes the carrier and returns T, not Box<T>.
    /// Replace an owning `*old_box` move with `old_edge.into_inner()`.
    #[recursive::recursive]
    pub fn into_inner(mut self) -> T {
        *self.inner.take().expect("live AstBox payload")
    }

    /// Preserves the original allocation when a Box<T> is required.
    #[recursive::recursive]
    pub fn into_box(mut self) -> Box<T> {
        self.inner.take().expect("live AstBox payload")
    }
}

impl<T> From<T> for AstBox<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: Default> Default for AstBox<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> Deref for AstBox<T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.inner.as_deref().expect("live AstBox payload")
    }
}

impl<T> DerefMut for AstBox<T> {
    fn deref_mut(&mut self) -> &mut T {
        self.inner.as_deref_mut().expect("live AstBox payload")
    }
}

impl<T> AsRef<T> for AstBox<T> {
    fn as_ref(&self) -> &T {
        self
    }
}

impl<T> AsMut<T> for AstBox<T> {
    fn as_mut(&mut self) -> &mut T {
        self
    }
}

impl<T: Clone> Clone for AstBox<T> {
    #[recursive::recursive]
    fn clone(&self) -> Self {
        Self::new(T::clone(self))
    }

    #[recursive::recursive]
    fn clone_from(&mut self, source: &Self) {
        T::clone_from(self, source);
    }
}

impl<T: fmt::Debug> fmt::Debug for AstBox<T> {
    #[recursive::recursive]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Preserve Box's transparent Debug representation.
        T::fmt(self, f)
    }
}

impl<T: PartialEq> PartialEq for AstBox<T> {
    #[recursive::recursive]
    fn eq(&self, other: &Self) -> bool {
        T::eq(self, other)
    }
}

impl<T: Eq> Eq for AstBox<T> {}

impl<T> Drop for AstBox<T> {
    #[recursive::recursive]
    fn drop(&mut self) {
        // Both transfer and actual recursive destruction run on the checked
        // stack. Automatic field destruction afterwards sees only None.
        drop(self.inner.take());
    }
}

/// A vector owning edge. Borrowed Vec operations remain available via Deref;
/// owned moves must use into_inner/IntoIterator rather than dereference.
pub struct AstVec<T> {
    inner: Vec<T>,
}

impl<T> AstVec<T> {
    pub fn new() -> Self {
        Self { inner: Vec::new() }
    }

    /// Consumes the carrier and returns the original Vec<T> allocation.
    #[recursive::recursive]
    pub fn into_inner(mut self) -> Vec<T> {
        std::mem::take(&mut self.inner)
    }
}

impl<T> Default for AstVec<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> From<Vec<T>> for AstVec<T> {
    fn from(inner: Vec<T>) -> Self {
        Self { inner }
    }
}

impl<T> FromIterator<T> for AstVec<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self::from(Vec::from_iter(iter))
    }
}

impl<T> Deref for AstVec<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Vec<T> {
        &self.inner
    }
}

impl<T> DerefMut for AstVec<T> {
    fn deref_mut(&mut self) -> &mut Vec<T> {
        &mut self.inner
    }
}

impl<T> IntoIterator for AstVec<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    #[recursive::recursive]
    fn into_iter(mut self) -> Self::IntoIter {
        std::mem::take(&mut self.inner).into_iter()
    }
}

impl<'a, T> IntoIterator for &'a AstVec<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.iter()
    }
}

impl<'a, T> IntoIterator for &'a mut AstVec<T> {
    type Item = &'a mut T;
    type IntoIter = std::slice::IterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.iter_mut()
    }
}

impl<T: Clone> Clone for AstVec<T> {
    #[recursive::recursive]
    fn clone(&self) -> Self {
        Self::from(self.inner.clone())
    }

    #[recursive::recursive]
    fn clone_from(&mut self, source: &Self) {
        self.inner.clone_from(&source.inner);
    }
}

impl<T: fmt::Debug> fmt::Debug for AstVec<T> {
    #[recursive::recursive]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T: PartialEq> PartialEq for AstVec<T> {
    #[recursive::recursive]
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl<T: Eq> Eq for AstVec<T> {}

impl<T> Drop for AstVec<T> {
    #[recursive::recursive]
    fn drop(&mut self) {
        // The original allocation and its elements are destroyed inside the
        // checked body; automatic field destruction sees a valid empty Vec.
        drop(std::mem::take(&mut self.inner));
    }
}

#[cfg(test)]
#[path = "storage_tests.rs"]
mod tests;
