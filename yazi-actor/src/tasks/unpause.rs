use anyhow::Result;
use yazi_macro::{act, render, succ};
use yazi_proxy::NotifyProxy;
use yazi_shared::data::Data;

use crate::{Actor, Ctx};

pub struct Unpause;

impl Actor for Unpause {
	type Options = yazi_parser::VoidOpt;

	const NAME: &str = "unpause";

	fn act(cx: &mut Ctx, _: Self::Options) -> Result<Data> {
		let tasks = &mut cx.tasks;
		let Some(id) = tasks.ongoing().lock().get_id(tasks.cursor) else {
			succ!();
		};

		if !tasks.scheduler.unpause_transfer(id) {
			NotifyProxy::push_warn("Unpause transfer", "Selected task is not paused");
			succ!();
		}

		tasks.snaps = tasks.paginate();
		act!(tasks:arrow, cx)?;
		succ!(render!());
	}
}
