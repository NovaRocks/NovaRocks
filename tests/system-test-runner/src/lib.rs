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

pub mod actors;
pub mod build_information;
mod cli;
mod config;
mod runner;
pub mod scenario;
mod scenario_settlement;
mod scenarios;

pub fn run(build: build_information::BuildInformation) {
    if std::env::args().skip(1).eq(["--build-info"]) {
        println!("{}", build_information::value(&build));
        return;
    }
    if let Err(error) = cli::Cli::parse_env().and_then(|cli| runner::run(cli, build)) {
        if let Some(verdict) = runner::observed_failure_presentation(&error) {
            eprintln!("system scenario runner failed: {verdict}");
        } else {
            eprintln!("system scenario runner failed: {error:#}");
        }
        let status = if runner::fixture_admission_rejected(&error) {
            75
        } else {
            1
        };
        std::process::exit(status);
    }
}
