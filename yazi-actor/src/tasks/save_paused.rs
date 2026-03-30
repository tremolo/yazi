use std::path::PathBuf;

use anyhow::{Context, Result};
use yazi_boot::BOOT;
use yazi_macro::succ;
use yazi_proxy::NotifyProxy;
use yazi_shared::data::Data;

use crate::{Actor, Ctx};

pub struct SavePaused;

impl Actor for SavePaused {
	type Options = yazi_parser::VoidOpt;

	const NAME: &str = "save_paused";

	fn act(cx: &mut Ctx, _: Self::Options) -> Result<Data> {
		let jobs = cx.tasks.scheduler.paused_transfer_jobs();
		let path = paused_jobs_path();

		if jobs.is_empty() {
			NotifyProxy::push_warn("Save paused transfers", "No paused copy/move jobs to save");
			succ!();
		}

		if let Some(parent) = path.parent() {
			std::fs::create_dir_all(parent)
				.with_context(|| format!("Failed to create directory {}", parent.display()))?;
		}

		let body = serde_json::to_string_pretty(&jobs)
			.with_context(|| "Failed to serialize paused transfer jobs")?;
		std::fs::write(&path, body)
			.with_context(|| format!("Failed to write {}", path.display()))?;

		NotifyProxy::push_warn(
			"Paused transfers saved",
			format!("Saved {} job(s) to {}", jobs.len(), path.display()),
		);
		succ!();
	}
}

fn paused_jobs_path() -> PathBuf { BOOT.state_dir.join("paused-transfer-jobs.json") }
