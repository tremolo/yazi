use std::path::PathBuf;

use anyhow::{Context, Result};
use yazi_boot::BOOT;
use yazi_macro::succ;
use yazi_proxy::NotifyProxy;
use yazi_scheduler::PausedTransferJob;
use yazi_shared::data::Data;

use crate::{Actor, Ctx};

pub struct LoadPaused;

impl Actor for LoadPaused {
	type Options = yazi_parser::VoidOpt;

	const NAME: &str = "load_paused";

	fn act(cx: &mut Ctx, _: Self::Options) -> Result<Data> {
		let path = paused_jobs_path();
		if !path.exists() {
			NotifyProxy::push_warn(
				"Load paused transfers",
				format!("No saved job list found at {}", path.display()),
			);
			succ!();
		}

		let body = std::fs::read_to_string(&path)
			.with_context(|| format!("Failed to read {}", path.display()))?;
		let jobs: Vec<PausedTransferJob> = serde_json::from_str(&body)
			.with_context(|| format!("Failed to parse {}", path.display()))?;

		if jobs.is_empty() {
			NotifyProxy::push_warn("Load paused transfers", "Saved job list is empty");
			succ!();
		}

		let count = cx.tasks.scheduler.restore_paused_transfer_jobs(jobs);
		if count == 0 {
			NotifyProxy::push_warn(
				"Paused transfers loaded",
				"No new jobs were added (all already queued)",
			);
			succ!();
		}

		NotifyProxy::push_warn(
			"Paused transfers loaded",
			format!("Queued {} job(s) from {}", count, path.display()),
		);
		succ!();
	}
}

fn paused_jobs_path() -> PathBuf { BOOT.state_dir.join("paused-transfer-jobs.json") }
