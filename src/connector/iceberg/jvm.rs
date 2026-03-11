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

#[cfg(feature = "embedded-jvm")]
mod imp {
    use std::sync::OnceLock;

    use jni::objects::{JByteArray, JObject, JString, JThrowable, JValue};
    use jni::{InitArgsBuilder, JNIEnv, JNIVersion, JavaVM};

    use super::super::ensure_embedded_jvm_enabled;

    const BRIDGE_CLASS: &str = "com/novarocks/connector/iceberg/IcebergMetadataBridge";
    const BRIDGE_JAR: &str = env!("NOVAROCKS_ICEBERG_BRIDGE_JAR");
    const SCAN_SIGNATURE: &str =
        "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Z)[B";

    struct JavaVmHolder {
        vm: JavaVM,
    }

    static JAVA_VM: OnceLock<Result<JavaVmHolder, String>> = OnceLock::new();

    fn java_vm() -> Result<&'static JavaVM, String> {
        match JAVA_VM.get_or_init(JavaVmHolder::new) {
            Ok(holder) => Ok(&holder.vm),
            Err(err) => Err(err.clone()),
        }
    }

    impl JavaVmHolder {
        fn new() -> Result<Self, String> {
            if !std::path::Path::new(BRIDGE_JAR).exists() {
                return Err(format!(
                    "embedded iceberg bridge jar not found: {}",
                    BRIDGE_JAR
                ));
            }
            let class_path_arg = format!("-Djava.class.path={BRIDGE_JAR}");
            let args = InitArgsBuilder::new()
                .version(JNIVersion::V8)
                .option("-Djava.awt.headless=true")
                .option("-Xrs")
                .option("--add-opens=java.base/java.util=ALL-UNNAMED")
                .option("--add-opens=java.base/java.nio=ALL-UNNAMED")
                .option("--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
                .option(&class_path_arg)
                .build()
                .map_err(|e| format!("build JVM init args failed: {e}"))?;
            let vm = JavaVM::new(args).map_err(|e| format!("create JVM failed: {e}"))?;
            Ok(Self { vm })
        }
    }

    pub(crate) fn scan_metadata(
        scanner_type: &str,
        serialized_table: &str,
        serialized_split: &str,
        serialized_predicate: &str,
        load_column_stats: bool,
    ) -> Result<Vec<u8>, String> {
        ensure_embedded_jvm_enabled("Iceberg metadata JVM bridge")?;
        let vm = java_vm()?;
        let mut env = vm
            .attach_current_thread()
            .map_err(|e| format!("attach current thread to JVM failed: {e}"))?;
        let class = env.find_class(BRIDGE_CLASS).map_err(|e| {
            format!(
                "find JVM bridge class failed: {}",
                describe_exception(&mut env, e)
            )
        })?;

        let scanner_type = env
            .new_string(scanner_type)
            .map_err(|e| format!("create scanner_type jstring failed: {e}"))?;
        let serialized_table = env
            .new_string(serialized_table)
            .map_err(|e| format!("create serialized_table jstring failed: {e}"))?;
        let serialized_split = env
            .new_string(serialized_split)
            .map_err(|e| format!("create serialized_split jstring failed: {e}"))?;
        let serialized_predicate = env
            .new_string(serialized_predicate)
            .map_err(|e| format!("create serialized_predicate jstring failed: {e}"))?;

        let scanner_type = JObject::from(scanner_type);
        let serialized_table = JObject::from(serialized_table);
        let serialized_split = JObject::from(serialized_split);
        let serialized_predicate = JObject::from(serialized_predicate);
        let args = [
            JValue::Object(&scanner_type),
            JValue::Object(&serialized_table),
            JValue::Object(&serialized_split),
            JValue::Object(&serialized_predicate),
            JValue::Bool(load_column_stats as u8),
        ];
        let result = env.call_static_method(class, "scan", SCAN_SIGNATURE, &args);
        let result = match result {
            Ok(value) => value,
            Err(err) => {
                return Err(format!(
                    "call JVM iceberg bridge failed: {}",
                    describe_exception(&mut env, err)
                ));
            }
        };
        let bytes = result
            .l()
            .map_err(|e| format!("read JVM bridge result failed: {e}"))?;
        if bytes.is_null() {
            return Ok(Vec::new());
        }
        env.convert_byte_array(JByteArray::from(bytes))
            .map_err(|e| format!("copy JVM bridge byte array failed: {e}"))
    }

    fn describe_exception(env: &mut JNIEnv<'_>, err: jni::errors::Error) -> String {
        if env.exception_check().unwrap_or(false) {
            if let Ok(throwable) = env.exception_occurred() {
                let _ = env.exception_clear();
                if let Ok(text) = format_throwable(env, &throwable) {
                    if !text.trim().is_empty() {
                        return text;
                    }
                }
                if let Ok(value) =
                    env.call_method(&throwable, "toString", "()Ljava/lang/String;", &[])
                {
                    if let Ok(obj) = value.l() {
                        if let Ok(text) = env.get_string(&JString::from(obj)) {
                            return text.to_string_lossy().into_owned();
                        }
                    }
                }
            }
        }
        err.to_string()
    }

    fn format_throwable(
        env: &mut JNIEnv<'_>,
        throwable: &JThrowable<'_>,
    ) -> Result<String, jni::errors::Error> {
        let string_writer = env.new_object("java/io/StringWriter", "()V", &[])?;
        let print_writer = env.new_object(
            "java/io/PrintWriter",
            "(Ljava/io/Writer;)V",
            &[JValue::Object(&string_writer)],
        )?;
        env.call_method(
            throwable,
            "printStackTrace",
            "(Ljava/io/PrintWriter;)V",
            &[JValue::Object(&print_writer)],
        )?;
        env.call_method(&print_writer, "flush", "()V", &[])?;
        let value = env.call_method(&string_writer, "toString", "()Ljava/lang/String;", &[])?;
        let obj = value.l()?;
        let jstring = JString::from(obj);
        let text = env.get_string(&jstring)?;
        Ok(text.to_string_lossy().into_owned())
    }
}

#[cfg(not(feature = "embedded-jvm"))]
mod imp {
    pub(crate) fn scan_metadata(
        _scanner_type: &str,
        _serialized_table: &str,
        _serialized_split: &str,
        _serialized_predicate: &str,
        _load_column_stats: bool,
    ) -> Result<Vec<u8>, String> {
        Err(
            "Iceberg metadata JVM bridge is unavailable because this NovaRocks binary was built without the `embedded-jvm` feature; rebuild with `--features embedded-jvm`"
                .to_string(),
        )
    }
}

pub(crate) use imp::scan_metadata;
