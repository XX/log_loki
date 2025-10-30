/*
Copyright (C) 2022 Aurora McGinnis

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

use std::collections::HashMap;

use bitflags::bitflags;
#[cfg(feature = "kv_unstable")]
use log::kv::{Key, Value, Visitor, value::Error as LogError};

use crate::{FormatLog, LokiFormatter};

// Contains all characters that may not appear in logfmt keys
const INVALID_KEY_CHARS: &[char] = &[' ', '=', '"'];

/// `LogfmtFormatter` provides a `LokiFormatter` that marshals logs using the logfmt format, which is a
/// plain text log format that is easy for both humans and machines to read and write. Loki provides
/// support for logfmt out of the box. This is used as the default formatter for the Loki logger if
/// the `logfmt` feature is enabled.
/// To learn more about logfmt, see: <https://www.brandur.org/logfmt>
#[derive(Default, Debug)]
pub struct LogfmtFormatter {
    include_fields: LogfmtAutoFields,
    escape_newlines: bool,
}

impl LogfmtFormatter {
    /// Create a new `LogfmtFormatter`. The created formatter will automatically insert fields
    /// depending on the value of include_fields. See `LogfmtAutoFields` for more details.
    /// \r, \n, and \t can be optionally escaped depending on the value of escape_newlines, but
    /// Loki does not require this.
    pub fn new(include_fields: LogfmtAutoFields, escape_newlines: bool) -> Self {
        LogfmtFormatter {
            include_fields,
            escape_newlines,
        }
    }

    /// Write a key value pair to the underlying string. Duplicate keys are dropped.
    fn write_pair(&self, attributes: &mut HashMap<String, String>, mut key: String, val: &str) {
        // Normalize the key
        key.retain(|c| {
            for invalid_char in INVALID_KEY_CHARS {
                if c == *invalid_char {
                    return false;
                }
            }
            true
        });
        if key.is_empty() {
            key.push('_');
        }

        // ensure uniqueness of the key
        if attributes.contains_key(&key) {
            return;
        }

        // reformat the value if needed
        let mut formatted_value = String::new();
        formatted_value.reserve(val.len() + 10);
        let mut need_quotes = false;
        for chr in val.chars() {
            match chr {
                '\\' | '"' => {
                    need_quotes = true;
                    formatted_value.push('\\');
                    formatted_value.push(chr);
                },
                ' ' | '=' => {
                    need_quotes = true;
                    formatted_value.push(chr);
                },

                '\n' | '\r' | '\t' => {
                    need_quotes = true;

                    if self.escape_newlines {
                        formatted_value.push('\\');
                    }

                    formatted_value.push(chr);
                },
                _ => {
                    if !chr.is_control() {
                        formatted_value.push(chr);
                    } else {
                        need_quotes = true;
                        formatted_value.push_str(&chr.escape_unicode().to_string());
                    }
                },
            }
        }
        if need_quotes {
            formatted_value.push('"');
        }

        attributes.insert(key, formatted_value);
    }
}

impl LokiFormatter for LogfmtFormatter {
    fn attributes(&self, rec: &dyn FormatLog) -> HashMap<String, String> {
        let mut attributes = HashMap::new();
        attributes.reserve(10);

        if self.include_fields.contains(LogfmtAutoFields::LEVEL) {
            self.write_pair(&mut attributes, "level".to_owned(), &rec.level());
        }

        let message = rec.message();
        if self.include_fields.contains(LogfmtAutoFields::MESSAGE) && !message.is_empty() {
            self.write_pair(&mut attributes, "message".to_owned(), &message);
        }

        let target = rec.target();
        if self.include_fields.contains(LogfmtAutoFields::TARGET) && !target.is_empty() {
            self.write_pair(&mut attributes, "target".to_owned(), &target);
        }

        if self.include_fields.contains(LogfmtAutoFields::MODULE_PATH)
            && let Some(module) = rec.module()
        {
            self.write_pair(&mut attributes, "module".to_owned(), &module);
        }

        if self.include_fields.contains(LogfmtAutoFields::FILE)
            && let Some(file) = rec.file()
        {
            self.write_pair(&mut attributes, "file".to_owned(), &file);
        }

        let line = rec.line();
        if self.include_fields.contains(LogfmtAutoFields::LINE)
            && let Some(line) = line
        {
            self.write_pair(&mut attributes, "line".to_owned(), &line);
        }

        #[cfg(feature = "kv_unstable")]
        if self.include_fields.contains(LogfmtAutoFields::EXTRA) {
            rec.key_values()
                .visit(&mut LogfmtVisitor {
                    fmt: self,
                    attributes: &mut attributes,
                })
                .expect("This visitor should not return an error");
        }

        attributes
    }
}

#[cfg(feature = "kv_unstable")]
struct LogfmtVisitor<'a> {
    fmt: &'a LogfmtFormatter,
    attributes: &'a mut HashMap<String, String>,
}

#[cfg(feature = "kv_unstable")]
impl<'a, 'kvs> Visitor<'kvs> for LogfmtVisitor<'a> {
    fn visit_pair(&mut self, key: Key<'kvs>, value: Value<'kvs>) -> Result<(), LogError> {
        self.fmt
            .write_pair(self.attributes, key.to_string(), &value.to_string());
        Ok(())
    }
}

bitflags! {
    /// `LogfmtAutoFields` is used to determine what fields of a log::Record should be rendered into
    /// the final logfmt string by the `LogfmtFormatter`. The default set is LEVEL | MESSAGE | MODULE_PATH
    /// | EXTRA
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct LogfmtAutoFields: u32 {
        /// Include a `level` field indicating the level the message was logged at.
        const LEVEL = 1;
        /// Include the `message` field containing the message passed to the log directive
        const MESSAGE = 1 << 1;
        /// Include a `target` field, corresponding to the target of the log directive
        const TARGET = 1 << 2;
        /// Include the `module` field set on the log record
        const MODULE_PATH = 1 << 3;
        /// Include the `file` field set on the log record
        const FILE = 1 << 4;
        /// Include the `line` field associated with the log directive.
        const LINE = 1 << 5;
        /// Include any extra fields specified via the structured logging API, if enabled.
        #[cfg(feature = "kv_unstable")]
        const EXTRA = 1 << 6;
    }
}

impl Default for LogfmtAutoFields {
    fn default() -> Self {
        #[cfg(feature = "kv_unstable")]
        {
            LogfmtAutoFields::LEVEL | LogfmtAutoFields::MODULE_PATH | LogfmtAutoFields::EXTRA
        }

        #[cfg(not(feature = "kv_unstable"))]
        {
            LogfmtAutoFields::LEVEL | LogfmtAutoFields::MODULE_PATH
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logfmt_write_record() {
        let record = log::Record::builder()
            .args(format_args!("log message"))
            .level(log::Level::Info)
            .target("target")
            .module_path(Some("module"))
            .file(Some("file"))
            .line(Some(1))
            .build();

        let formatter = LogfmtFormatter::default();
        let log_line = formatter.log_line(&record).unwrap();
        let attributes = formatter.attributes(&record);

        assert_eq!(log_line, "log message");
        assert_eq!(
            attributes,
            [("level", "info"), ("module", "module")]
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect()
        );

        let formatter = LogfmtFormatter::new(
            LogfmtAutoFields::default() | LogfmtAutoFields::TARGET | LogfmtAutoFields::FILE | LogfmtAutoFields::LINE,
            false,
        );
        let log_line = formatter.log_line(&record).unwrap();
        let attributes = formatter.attributes(&record);

        assert_eq!(log_line, "log message");
        assert_eq!(
            attributes,
            [
                ("level", "info"),
                ("target", "target"),
                ("module", "module"),
                ("file", "file"),
                ("line", "1")
            ]
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect()
        );
    }
}
