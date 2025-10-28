/*
Copyright (C) 2022 Aurora McGinnis

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

use std::borrow::Cow;
use std::fmt;

pub trait FormatLog {
    fn level(&self) -> Cow<'_, str>;
    fn message(&self) -> Cow<'_, str>;
    fn target(&self) -> Cow<'_, str>;
    fn module(&self) -> Option<Cow<'_, str>>;
    fn file(&self) -> Option<Cow<'_, str>>;
    fn line(&self) -> Option<Cow<'_, str>>;
    #[cfg(feature = "kv_unstable")]
    fn key_values(&self) -> &dyn log::kv::Source;
}

impl FormatLog for log::Record<'_> {
    fn level(&self) -> Cow<'_, str> {
        Cow::Owned(self.level().to_string().to_lowercase())
    }

    fn message(&self) -> Cow<'_, str> {
        Cow::Owned(self.args().to_string())
    }

    fn target(&self) -> Cow<'_, str> {
        Cow::Borrowed(self.target())
    }

    fn module(&self) -> Option<Cow<'_, str>> {
        self.module_path()
            .map(Cow::Borrowed)
            .or_else(|| self.module_path_static().map(Cow::Borrowed))
    }

    fn file(&self) -> Option<Cow<'_, str>> {
        self.file()
            .map(Cow::Borrowed)
            .or_else(|| self.file_static().map(Cow::Borrowed))
    }

    fn line(&self) -> Option<Cow<'_, str>> {
        self.line().map(|line| Cow::Owned(line.to_string()))
    }

    #[cfg(feature = "kv_unstable")]
    fn key_values(&self) -> &dyn log::kv::Source {
        self.key_values()
    }
}

/// `LokiFormatter` implementations marshals a log record to a string. This trait can be implemented
/// to customize the format of the strings that get sent to Loki. By default, this crate provides a
/// logfmt `LokiFormatter` implementation, which is used by default.
pub trait LokiFormatter: Send + Sync {
    fn write_record(&self, dst: &mut String, rec: &dyn FormatLog) -> fmt::Result;
}
