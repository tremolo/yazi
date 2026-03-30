use std::{
	fs::OpenOptions,
	io::Write,
	time::{SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use yazi_shared::Id;

const DEFAULT_LOG_PATH: &str = "/tmp/yazi-transfer-events.log";

#[derive(Debug, Serialize)]
pub struct TransferEvent {
	pub ts_ms:   u128,
	pub event:   &'static str,
	pub id:      u64,
	pub kind:    &'static str,
	pub from:    Option<String>,
	pub to:      Option<String>,
	pub detail:  Option<String>,
	pub active:  bool,
	pub pending: usize,
	pub blocked: usize,
}

impl TransferEvent {
	#[inline]
	pub fn new(event: &'static str, id: Id, kind: &'static str) -> Self {
		Self {
			ts_ms: now_ms(),
			event,
			id: id.get(),
			kind,
			from: None,
			to: None,
			detail: None,
			active: false,
			pending: 0,
			blocked: 0,
		}
	}
}

pub fn append(event: &TransferEvent) {
	let path = std::env::var("YAZI_TRANSFER_EVENTS_LOG")
		.ok()
		.filter(|s| !s.is_empty())
		.unwrap_or_else(|| DEFAULT_LOG_PATH.to_owned());

	let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) else {
		return;
	};

	let Ok(line) = serde_json::to_string(event) else {
		return;
	};

	let _ = writeln!(file, "{line}");
}

fn now_ms() -> u128 {
	SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0)
}
