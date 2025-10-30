use std::time::Duration;
use std::{env, thread};

use log_loki::{LogfmtAutoFields, LogfmtFormatter, LokiBuilder};

fn main() {
    let endpoint = env::args()
        .nth(1)
        .unwrap_or_else(|| "http://localhost/loki/api/v1/push".into());

    let labels = [("application".into(), "example_log".into())].into_iter().collect();
    let loki = LokiBuilder::new(endpoint.parse().unwrap(), labels)
        .level(log::LevelFilter::Info)
        .formatter(Box::new(LogfmtFormatter::new(
            LogfmtAutoFields::default() | LogfmtAutoFields::FILE | LogfmtAutoFields::LINE,
            false,
        )))
        .build();

    let record = log::Record::builder()
        .level(log::Level::Info)
        .file(Some("log.rs"))
        .line(Some(18))
        .module_path(Some("example::log"))
        .args(format_args!("Example log message"))
        .build();

    loki.send_log(&record);
    loki.send_and_white_flush();

    thread::sleep(Duration::from_secs(1));
}
