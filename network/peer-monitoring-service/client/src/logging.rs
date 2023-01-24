// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use crate::Error;
use aptos_logger::Schema;
use serde::Serialize;

#[derive(Schema)]
pub struct LogSchema<'a> {
    name: LogEntry,
    #[schema(debug)]
    error: Option<&'a Error>,
    event: Option<LogEvent>,
    message: Option<&'a str>,
}

impl<'a> LogSchema<'a> {
    pub fn new(name: LogEntry) -> Self {
        Self {
            name,
            error: None,
            event: None,
            message: None,
        }
    }
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LogEntry {
    PeerMonitorLoop,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LogEvent {
    StartedPeerMonitorLoop,
    UnexpectedErrorEncountered,
}
