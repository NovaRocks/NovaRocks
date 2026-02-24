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
use arrow::array::{Array, ArrayRef, BinaryArray, BinaryBuilder, Int64Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use base64::Engine;
use openssl::symm::{Cipher, Crypter, Mode};
use std::sync::Arc;

const DEFAULT_IV: &[u8] = b"STARROCKS_16BYTE";
const GCM_TAG_SIZE: usize = 16;

#[derive(Clone)]
pub(super) enum OwnedBytesArray {
    Utf8(StringArray),
    Binary(BinaryArray),
}

impl OwnedBytesArray {
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Utf8(arr) => arr.len(),
            Self::Binary(arr) => arr.len(),
        }
    }

    pub(super) fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Utf8(arr) => arr.is_null(row),
            Self::Binary(arr) => arr.is_null(row),
        }
    }

    pub(super) fn bytes(&self, row: usize) -> &[u8] {
        match self {
            Self::Utf8(arr) => arr.value(row).as_bytes(),
            Self::Binary(arr) => arr.value(row),
        }
    }

    pub(super) fn utf8(&self, row: usize) -> Option<&str> {
        match self {
            Self::Utf8(arr) if !arr.is_null(row) => Some(arr.value(row)),
            _ => None,
        }
    }
}

pub(super) fn to_owned_bytes_array(
    array: ArrayRef,
    fn_name: &str,
    arg_idx: usize,
) -> Result<OwnedBytesArray, String> {
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(OwnedBytesArray::Utf8(arr.clone()));
    }
    if let Some(arr) = array.as_any().downcast_ref::<BinaryArray>() {
        return Ok(OwnedBytesArray::Binary(arr.clone()));
    }
    Err(format!(
        "{}: arg{} must be VARCHAR or VARBINARY",
        fn_name, arg_idx
    ))
}

pub(super) fn to_i64_array(
    array: &ArrayRef,
    fn_name: &str,
    arg_idx: usize,
) -> Result<Int64Array, String> {
    let casted = cast(array, &DataType::Int64).map_err(|e| {
        format!(
            "{}: failed to cast arg{} to BIGINT: {}",
            fn_name, arg_idx, e
        )
    })?;
    casted
        .as_any()
        .downcast_ref::<Int64Array>()
        .cloned()
        .ok_or_else(|| format!("{}: arg{} is not BIGINT", fn_name, arg_idx))
}

pub(super) fn cast_output(
    out: ArrayRef,
    output_type: Option<&DataType>,
    fn_name: &str,
) -> Result<ArrayRef, String> {
    let Some(target) = output_type else {
        return Ok(out);
    };
    if out.data_type() == target {
        return Ok(out);
    }
    cast(&out, target).map_err(|e| format!("{}: failed to cast output: {}", fn_name, e))
}

fn build_binary_array(values: Vec<Option<Vec<u8>>>) -> BinaryArray {
    let mut builder = BinaryBuilder::new();
    for value in values {
        match value {
            Some(bytes) => builder.append_value(bytes),
            None => builder.append_null(),
        }
    }
    builder.finish()
}

fn bytes_to_latin1_string(bytes: &[u8]) -> String {
    bytes.iter().map(|b| char::from(*b)).collect()
}

pub(super) fn latin1_string_to_bytes(s: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(s.len());
    for ch in s.chars() {
        if (ch as u32) > 0xff {
            return None;
        }
        out.push(ch as u8);
    }
    Some(out)
}

pub(super) fn build_bytes_output_lossy(
    values: Vec<Option<Vec<u8>>>,
    output_type: Option<&DataType>,
) -> Result<ArrayRef, String> {
    match output_type {
        Some(DataType::Binary) => Ok(Arc::new(build_binary_array(values)) as ArrayRef),
        _ => {
            let out: Vec<Option<String>> = values
                .into_iter()
                .map(|v| v.map(|b| String::from_utf8_lossy(&b).to_string()))
                .collect();
            Ok(Arc::new(StringArray::from(out)) as ArrayRef)
        }
    }
}

pub(super) fn build_bytes_output_latin1(
    values: Vec<Option<Vec<u8>>>,
    output_type: Option<&DataType>,
) -> Result<ArrayRef, String> {
    match output_type {
        Some(DataType::Binary) => Ok(Arc::new(build_binary_array(values)) as ArrayRef),
        _ => {
            let out: Vec<Option<String>> = values
                .into_iter()
                .map(|v| v.map(|b| bytes_to_latin1_string(&b)))
                .collect();
            Ok(Arc::new(StringArray::from(out)) as ArrayRef)
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum BinaryFormatType {
    Hex,
    Encode64,
    Utf8,
}

pub(super) fn parse_binary_format(format: Option<&str>) -> BinaryFormatType {
    let Some(format) = format else {
        return BinaryFormatType::Hex;
    };

    match format.to_ascii_lowercase().as_str() {
        "encode64" => BinaryFormatType::Encode64,
        "utf8" => BinaryFormatType::Utf8,
        _ => BinaryFormatType::Hex,
    }
}

#[derive(Clone, Copy)]
pub(super) enum AesMode {
    Aes128Ecb,
    Aes192Ecb,
    Aes256Ecb,
    Aes128Cbc,
    Aes192Cbc,
    Aes256Cbc,
    Aes128Cfb,
    Aes192Cfb,
    Aes256Cfb,
    Aes128Cfb1,
    Aes192Cfb1,
    Aes256Cfb1,
    Aes128Cfb8,
    Aes192Cfb8,
    Aes256Cfb8,
    Aes128Cfb128,
    Aes192Cfb128,
    Aes256Cfb128,
    Aes128Ofb,
    Aes192Ofb,
    Aes256Ofb,
    Aes128Ctr,
    Aes192Ctr,
    Aes256Ctr,
    Aes128Gcm,
    Aes192Gcm,
    Aes256Gcm,
}

impl AesMode {
    pub(super) fn parse(bytes: &[u8]) -> Self {
        let mode = String::from_utf8_lossy(bytes).to_ascii_uppercase();
        match mode.as_str() {
            "AES_192_ECB" => Self::Aes192Ecb,
            "AES_256_ECB" => Self::Aes256Ecb,
            "AES_128_CBC" => Self::Aes128Cbc,
            "AES_192_CBC" => Self::Aes192Cbc,
            "AES_256_CBC" => Self::Aes256Cbc,
            "AES_128_CFB" => Self::Aes128Cfb,
            "AES_192_CFB" => Self::Aes192Cfb,
            "AES_256_CFB" => Self::Aes256Cfb,
            "AES_128_CFB1" => Self::Aes128Cfb1,
            "AES_192_CFB1" => Self::Aes192Cfb1,
            "AES_256_CFB1" => Self::Aes256Cfb1,
            "AES_128_CFB8" => Self::Aes128Cfb8,
            "AES_192_CFB8" => Self::Aes192Cfb8,
            "AES_256_CFB8" => Self::Aes256Cfb8,
            "AES_128_CFB128" => Self::Aes128Cfb128,
            "AES_192_CFB128" => Self::Aes192Cfb128,
            "AES_256_CFB128" => Self::Aes256Cfb128,
            "AES_128_OFB" => Self::Aes128Ofb,
            "AES_192_OFB" => Self::Aes192Ofb,
            "AES_256_OFB" => Self::Aes256Ofb,
            "AES_128_CTR" => Self::Aes128Ctr,
            "AES_192_CTR" => Self::Aes192Ctr,
            "AES_256_CTR" => Self::Aes256Ctr,
            "AES_128_GCM" => Self::Aes128Gcm,
            "AES_192_GCM" => Self::Aes192Gcm,
            "AES_256_GCM" => Self::Aes256Gcm,
            _ => Self::Aes128Ecb,
        }
    }

    pub(super) fn is_gcm(self) -> bool {
        matches!(self, Self::Aes128Gcm | Self::Aes192Gcm | Self::Aes256Gcm)
    }

    pub(super) fn is_stream(self) -> bool {
        matches!(
            self,
            Self::Aes128Cfb
                | Self::Aes192Cfb
                | Self::Aes256Cfb
                | Self::Aes128Cfb1
                | Self::Aes192Cfb1
                | Self::Aes256Cfb1
                | Self::Aes128Cfb8
                | Self::Aes192Cfb8
                | Self::Aes256Cfb8
                | Self::Aes128Cfb128
                | Self::Aes192Cfb128
                | Self::Aes256Cfb128
                | Self::Aes128Ofb
                | Self::Aes192Ofb
                | Self::Aes256Ofb
                | Self::Aes128Ctr
                | Self::Aes192Ctr
                | Self::Aes256Ctr
        )
    }

    pub(super) fn is_ecb(self) -> bool {
        matches!(self, Self::Aes128Ecb | Self::Aes192Ecb | Self::Aes256Ecb)
    }

    fn key_len(self) -> usize {
        match self {
            Self::Aes128Ecb
            | Self::Aes128Cbc
            | Self::Aes128Cfb
            | Self::Aes128Cfb1
            | Self::Aes128Cfb8
            | Self::Aes128Cfb128
            | Self::Aes128Ofb
            | Self::Aes128Ctr
            | Self::Aes128Gcm => 16,
            Self::Aes192Ecb
            | Self::Aes192Cbc
            | Self::Aes192Cfb
            | Self::Aes192Cfb1
            | Self::Aes192Cfb8
            | Self::Aes192Cfb128
            | Self::Aes192Ofb
            | Self::Aes192Ctr
            | Self::Aes192Gcm => 24,
            Self::Aes256Ecb
            | Self::Aes256Cbc
            | Self::Aes256Cfb
            | Self::Aes256Cfb1
            | Self::Aes256Cfb8
            | Self::Aes256Cfb128
            | Self::Aes256Ofb
            | Self::Aes256Ctr
            | Self::Aes256Gcm => 32,
        }
    }

    fn cipher(self) -> Cipher {
        match self {
            Self::Aes128Ecb => Cipher::aes_128_ecb(),
            Self::Aes192Ecb => Cipher::aes_192_ecb(),
            Self::Aes256Ecb => Cipher::aes_256_ecb(),
            Self::Aes128Cbc => Cipher::aes_128_cbc(),
            Self::Aes192Cbc => Cipher::aes_192_cbc(),
            Self::Aes256Cbc => Cipher::aes_256_cbc(),
            Self::Aes128Cfb | Self::Aes128Cfb128 => Cipher::aes_128_cfb128(),
            Self::Aes192Cfb | Self::Aes192Cfb128 => Cipher::aes_192_cfb128(),
            Self::Aes256Cfb | Self::Aes256Cfb128 => Cipher::aes_256_cfb128(),
            Self::Aes128Cfb1 => Cipher::aes_128_cfb1(),
            Self::Aes192Cfb1 => Cipher::aes_192_cfb1(),
            Self::Aes256Cfb1 => Cipher::aes_256_cfb1(),
            Self::Aes128Cfb8 => Cipher::aes_128_cfb8(),
            Self::Aes192Cfb8 => Cipher::aes_192_cfb8(),
            Self::Aes256Cfb8 => Cipher::aes_256_cfb8(),
            Self::Aes128Ofb => Cipher::aes_128_ofb(),
            Self::Aes192Ofb => Cipher::aes_192_ofb(),
            Self::Aes256Ofb => Cipher::aes_256_ofb(),
            Self::Aes128Ctr => Cipher::aes_128_ctr(),
            Self::Aes192Ctr => Cipher::aes_192_ctr(),
            Self::Aes256Ctr => Cipher::aes_256_ctr(),
            Self::Aes128Gcm => Cipher::aes_128_gcm(),
            Self::Aes192Gcm => Cipher::aes_192_gcm(),
            Self::Aes256Gcm => Cipher::aes_256_gcm(),
        }
    }
}

fn build_aes_key(input_key: &[u8], key_size: usize) -> Vec<u8> {
    let mut key = vec![0u8; key_size];
    for (idx, b) in input_key.iter().enumerate() {
        key[idx % key_size] ^= b;
    }
    key
}

fn build_iv(iv_input: Option<&[u8]>, iv_length: usize) -> Vec<u8> {
    let mut iv = vec![0u8; iv_length];
    if iv_length == 0 {
        return iv;
    }

    if let Some(input) = iv_input.filter(|v| !v.is_empty()) {
        let copy_len = input.len().min(iv_length);
        iv[..copy_len].copy_from_slice(&input[..copy_len]);
    } else {
        let copy_len = DEFAULT_IV.len().min(iv_length);
        iv[..copy_len].copy_from_slice(&DEFAULT_IV[..copy_len]);
    }

    iv
}

pub(super) fn aes_encrypt_raw(
    mode: AesMode,
    source: &[u8],
    key: &[u8],
    iv_input: Option<&[u8]>,
    aad: Option<&[u8]>,
) -> Option<Vec<u8>> {
    let cipher = mode.cipher();
    let key = build_aes_key(key, mode.key_len());
    let iv_len = cipher.iv_len().unwrap_or(0);
    let iv = build_iv(iv_input, iv_len);

    if mode.is_gcm() {
        let mut crypter = Crypter::new(cipher, Mode::Encrypt, &key, Some(&iv)).ok()?;
        crypter.pad(false);

        if let Some(aad) = aad.filter(|v| !v.is_empty()) {
            crypter.aad_update(aad).ok()?;
        }

        let mut ciphertext = vec![0u8; source.len() + cipher.block_size()];
        let count = crypter.update(source, &mut ciphertext).ok()?;
        let rest = crypter.finalize(&mut ciphertext[count..]).ok()?;
        ciphertext.truncate(count + rest);

        let mut tag = [0u8; GCM_TAG_SIZE];
        crypter.get_tag(&mut tag).ok()?;

        let mut out = Vec::with_capacity(iv.len() + ciphertext.len() + tag.len());
        out.extend_from_slice(&iv);
        out.extend_from_slice(&ciphertext);
        out.extend_from_slice(&tag);
        return Some(out);
    }

    let iv_opt = if iv_len > 0 {
        Some(iv.as_slice())
    } else {
        None
    };
    let mut crypter = Crypter::new(cipher, Mode::Encrypt, &key, iv_opt).ok()?;
    crypter.pad(!mode.is_stream());

    let mut out = vec![0u8; source.len() + cipher.block_size()];
    let count = crypter.update(source, &mut out).ok()?;
    let rest = crypter.finalize(&mut out[count..]).ok()?;
    out.truncate(count + rest);
    Some(out)
}

pub(super) fn aes_decrypt_raw(
    mode: AesMode,
    encrypted: &[u8],
    key: &[u8],
    iv_input: Option<&[u8]>,
    aad: Option<&[u8]>,
) -> Option<Vec<u8>> {
    let cipher = mode.cipher();
    let key = build_aes_key(key, mode.key_len());
    let iv_len = cipher.iv_len().unwrap_or(0);

    if mode.is_gcm() {
        if encrypted.len() < iv_len + GCM_TAG_SIZE {
            return None;
        }

        let iv = &encrypted[..iv_len];
        let ciphertext_end = encrypted.len() - GCM_TAG_SIZE;
        let ciphertext = &encrypted[iv_len..ciphertext_end];
        let tag = &encrypted[ciphertext_end..];

        let mut crypter = Crypter::new(cipher, Mode::Decrypt, &key, Some(iv)).ok()?;
        crypter.pad(false);

        if let Some(aad) = aad.filter(|v| !v.is_empty()) {
            crypter.aad_update(aad).ok()?;
        }

        crypter.set_tag(tag).ok()?;

        let mut out = vec![0u8; ciphertext.len() + cipher.block_size()];
        let count = crypter.update(ciphertext, &mut out).ok()?;
        let rest = crypter.finalize(&mut out[count..]).ok()?;
        out.truncate(count + rest);
        return Some(out);
    }

    let iv = build_iv(iv_input, iv_len);
    let iv_opt = if iv_len > 0 {
        Some(iv.as_slice())
    } else {
        None
    };
    let mut crypter = Crypter::new(cipher, Mode::Decrypt, &key, iv_opt).ok()?;
    crypter.pad(!mode.is_stream());

    let mut out = vec![0u8; encrypted.len() + cipher.block_size()];
    let count = crypter.update(encrypted, &mut out).ok()?;
    let rest = crypter.finalize(&mut out[count..]).ok()?;
    out.truncate(count + rest);
    Some(out)
}

pub(super) fn decode_base64(input: &[u8]) -> Option<Vec<u8>> {
    base64::engine::general_purpose::STANDARD.decode(input).ok()
}

pub(super) fn encode_base64(input: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(input)
}
