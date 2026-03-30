use std::{collections::VecDeque, sync::Arc, time::Duration};

use parking_lot::Mutex;
use tokio::{select, sync::mpsc, task::JoinHandle};
use yazi_parser::notify::{PushLevel, PushOpt};
use yazi_config::YAZI;
use yazi_proxy::NotifyProxy;
use yazi_shared::url::UrlLike;

use crate::{LOW, Ongoing, TaskOp, TaskOps, TaskOut, fetch::{Fetch, FetchIn}, file::{File, FileIn}, hook::{Hook, HookIn}, plugin::{Plugin, PluginIn}, preload::{Preload, PreloadIn}, process::{Process, ProcessIn}, size::{Size, SizeIn}, transfer_log::{self, TransferEvent}};

#[derive(Clone)]
pub struct Runner {
	pub(super) file:    Arc<File>,
	pub(super) plugin:  Arc<Plugin>,
	pub fetch:          Arc<Fetch>,
	pub preload:        Arc<Preload>,
	pub size:           Arc<Size>,
	pub(super) process: Arc<Process>,
	pub(super) hook:    Arc<Hook>,

	pub ops:     TaskOps,
	pub ongoing: Arc<Mutex<Ongoing>>,
	transfer:    Arc<Mutex<TransferQueue>>,
}

#[derive(Default)]
struct TransferQueue {
	active:     Option<yazi_shared::Id>,
	active_job: Option<PausedTransferJob>,
	pending:    VecDeque<(FileIn, u8)>,
	blocked:    VecDeque<(FileIn, u8)>,
}

impl TransferQueue {
	fn take_next(&mut self) -> Option<(FileIn, u8, bool)> {
		if self.active.is_some() {
			return None;
		}

		if let Some((r#in, priority)) = self.pending.pop_front() {
			self.active = Some(r#in.id());
			self.active_job = blocked_job_of(&r#in);
			Some((r#in, priority, false))
		} else if let Some((r#in, priority)) = self.blocked.pop_front() {
			self.active = Some(r#in.id());
			self.active_job = blocked_job_of(&r#in);
			Some((r#in, priority, true))
		} else {
			None
		}
	}

	fn mark_blocked(&mut self, r#in: FileIn, priority: u8) {
		if self.active == Some(r#in.id()) {
			self.active = None;
			self.active_job = None;
		}
		self.blocked.push_back((r#in, priority));
	}

	fn mark_complete(&mut self, id: yazi_shared::Id) {
		if self.active == Some(id) {
			self.active = None;
			self.active_job = None;
		}
	}

	fn push_front(&mut self, r#in: FileIn, priority: u8, blocked: bool) {
		if self.active == Some(r#in.id()) {
			self.active = None;
			self.active_job = None;
		}
		if blocked {
			self.blocked.push_front((r#in, priority));
		} else {
			self.pending.push_front((r#in, priority));
		}
	}
}

#[derive(Clone, Copy, Debug, Default)]
pub struct TransferCounts {
	pub active:  bool,
	pub pending: usize,
	pub blocked: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum TransferJobKind {
	Copy,
	Cut,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct PausedTransferJob {
	pub kind:   TransferJobKind,
	pub from:   yazi_shared::url::UrlBuf,
	pub to:     yazi_shared::url::UrlBuf,
	pub force:  bool,
	pub follow: bool,
}

impl Runner {
	pub(super) fn make() -> (Self, Vec<JoinHandle<()>>) {
		let (file_tx, file_rx) = async_priority_channel::unbounded();
		let (plugin_tx, plugin_rx) = async_priority_channel::unbounded();
		let (fetch_tx, fetch_rx) = async_priority_channel::unbounded();
		let (preload_tx, preload_rx) = async_priority_channel::unbounded();
		let (size_tx, size_rx) = async_priority_channel::unbounded();
		let (process_tx, process_rx) = async_priority_channel::unbounded();
		let (hook_tx, hook_rx) = async_priority_channel::unbounded();
		let (op_tx, op_rx) = mpsc::unbounded_channel();
		let ongoing = Arc::new(Mutex::new(Ongoing::default()));

		let me = Self {
			file: Arc::new(File::new(&op_tx, file_tx)),
			plugin: Arc::new(Plugin::new(&op_tx, plugin_tx)),
			fetch: Arc::new(Fetch::new(&op_tx, fetch_tx)),
			preload: Arc::new(Preload::new(&op_tx, preload_tx)),
			size: Arc::new(Size::new(&op_tx, size_tx)),
			process: Arc::new(Process::new(&op_tx, process_tx)),
			hook: Arc::new(Hook::new(&op_tx, &ongoing, hook_tx)),

			ops: TaskOps(op_tx),
			ongoing,
			transfer: Default::default(),
		};

		let handles = []
			.into_iter()
			.chain((0..YAZI.tasks.file_workers).map(|_| me.file(file_rx.clone())))
			.chain((0..YAZI.tasks.plugin_workers).map(|_| me.plugin(plugin_rx.clone())))
			.chain((0..YAZI.tasks.fetch_workers).map(|_| me.fetch(fetch_rx.clone())))
			.chain((0..YAZI.tasks.preload_workers).map(|_| me.preload(preload_rx.clone())))
			.chain((0..3).map(|_| me.size(size_rx.clone())))
			.chain((0..YAZI.tasks.process_workers).map(|_| me.process(process_rx.clone())))
			.chain((0..3).map(|_| me.hook(hook_rx.clone())))
			.chain([me.op(op_rx), me.blocked_tick()])
			.collect();

		(me, handles)
	}

	pub(crate) fn enqueue_transfer(&self, r#in: impl Into<FileIn>, priority: u8) {
		let r#in = r#in.into();
		self.log_event("enqueue", &r#in, None);
		if let Err(blocked) = self.file.check_transfer_slot(&r#in) {
			let id = r#in.id();
			self.log_event("blocked", &r#in, Some(format!(
				"needed={} available={} reserved={} dest={}",
				blocked.needed,
				blocked.available,
				blocked.reserved,
				blocked.dest.display()
			)));
			self.transfer.lock().blocked.push_back((r#in, priority));
			self.notify_capacity_blocked(id, blocked);
			return;
		}

		self.transfer.lock().pending.push_back((r#in, priority));
		self.dispatch_transfer();
	}

	pub fn transfer_counts(&self) -> TransferCounts {
		let q = self.transfer.lock();
		TransferCounts { active: q.active.is_some(), pending: q.pending.len(), blocked: q.blocked.len() }
	}

	pub fn blocked_jobs(&self) -> Vec<PausedTransferJob> {
		let q = self.transfer.lock();
		q.blocked.iter().filter_map(|(r#in, _)| blocked_job_of(r#in)).collect()
	}

	pub fn has_transfer_job(&self, job: &PausedTransferJob) -> bool {
		let q = self.transfer.lock();
		q.active_job
			.as_ref()
			.is_some_and(|existing| same_job(existing, job))
			|| q.pending
			.iter()
			.filter_map(|(r#in, _)| blocked_job_of(r#in))
			.any(|existing| same_job(&existing, job))
			|| q
				.blocked
				.iter()
				.filter_map(|(r#in, _)| blocked_job_of(r#in))
				.any(|existing| same_job(&existing, job))
	}

	pub fn unpause_transfer(&self, id: yazi_shared::Id) -> bool {
		let resumed = {
			let mut q = self.transfer.lock();
			let Some(i) = q.blocked.iter().position(|(r#in, _)| r#in.id() == id) else {
				return false;
			};

			let Some((r#in, priority)) = q.blocked.remove(i) else {
				return false;
			};
			q.pending.push_front((r#in, priority));
			true
		};

		if resumed {
			self.dispatch_transfer();
		}
		resumed
	}

	pub(crate) fn cancel_transfer(&self, id: yazi_shared::Id) {
		let mut q = self.transfer.lock();
		q.pending.retain(|(r#in, _)| r#in.id() != id);
		q.blocked.retain(|(r#in, _)| r#in.id() != id);
		if q.active == Some(id) {
			q.active = None;
		}
		drop(q);

		self.file.release_reservation(id);
		self.dispatch_transfer();
	}

	fn dispatch_transfer(&self) {
		loop {
			let Some((r#in, priority, from_blocked)) = ({
				let mut q = self.transfer.lock();
				q.take_next()
			}) else {
				return;
			};

			let token = if let Some(ongoing) = self.ongoing.try_lock() {
				if let Some(token) = ongoing.get_token(r#in.id()) {
					token
				} else {
					drop(ongoing);
					self.complete_transfer(r#in.id());
					continue;
				}
			} else {
				{
					let mut q = self.transfer.lock();
					q.push_front(r#in, priority, from_blocked);
				}
				return;
			};

			if token.completed() == Some(false) {
				self.complete_transfer(r#in.id());
				continue;
			}

			if let Err(blocked) = self.file.check_transfer_slot(&r#in) {
				let id = r#in.id();
				if !from_blocked {
					self.log_event("blocked", &r#in, Some(format!(
						"needed={} available={} reserved={} dest={}",
						blocked.needed,
						blocked.available,
						blocked.reserved,
						blocked.dest.display()
					)));
					self.notify_capacity_blocked(id, blocked);
				}
				{
					let mut q = self.transfer.lock();
					q.mark_blocked(r#in, priority);
				}
				if from_blocked {
					return;
				}
				continue;
			}

			if let Err(blocked) = self.file.reserve_transfer_slot(&r#in) {
				self.log_event("blocked", &r#in, Some(format!(
					"needed={} available={} reserved={} dest={}",
					blocked.needed,
					blocked.available,
					blocked.reserved,
					blocked.dest.display()
				)));
				{
					let mut q = self.transfer.lock();
					q.mark_blocked(r#in, priority);
				}
				continue;
			}

			self.log_event("start", &r#in, None);
			self.file.submit(r#in, priority);
			return;
		}
	}

	fn blocked_tick(&self) -> JoinHandle<()> {
		let me = self.clone();
		tokio::spawn(async move {
			if !YAZI.tasks.auto_restart_paused {
				return;
			}

			loop {
				tokio::time::sleep(Duration::from_secs(2)).await;
				let should_dispatch = {
					let q = me.transfer.lock();
					q.active.is_none() && !q.blocked.is_empty()
				};
				if should_dispatch {
					me.dispatch_transfer();
				}
			}
		})
	}

	fn complete_transfer(&self, id: yazi_shared::Id) {
		let should_log = {
			let q = self.transfer.lock();
			q.active == Some(id)
				|| q.pending.iter().any(|(r#in, _)| r#in.id() == id)
				|| q.blocked.iter().any(|(r#in, _)| r#in.id() == id)
		};
		if should_log {
			self.log_state_only("complete", id, "unknown", None);
		}

		let mut q = self.transfer.lock();
		q.mark_complete(id);
		drop(q);

		self.file.release_reservation(id);
		self.dispatch_transfer();
	}

	fn notify_capacity_blocked(&self, id: yazi_shared::Id, blocked: crate::file::CapacityBlocked) {
		let content = format!(
			"Auto-paused transfer for {}\nneeded: {} B, available: {} B, reserved: {} B",
			blocked.dest.display(),
			blocked.needed,
			blocked.available,
			blocked.reserved
		);
		self.ops.out(
			id,
			crate::file::FileOutCopyDo::Log(format!(
				"Queued waiting for destination space at {} (needed={}B, available={}B, reserved={}B)",
				blocked.dest.display(),
				blocked.needed,
				blocked.available,
				blocked.reserved
			)),
		);
		self.ops.out(
			id,
			crate::file::FileOutCutDo::Log(format!(
				"Queued waiting for destination space at {} (needed={}B, available={}B, reserved={}B)",
				blocked.dest.display(),
				blocked.needed,
				blocked.available,
				blocked.reserved
			)),
		);
		NotifyProxy::push(PushOpt {
			title: "Transfer auto-paused (insufficient destination space)".to_owned(),
			content,
			level: PushLevel::Warn,
			timeout: std::time::Duration::from_secs(10),
		});
	}

	fn log_event(&self, event: &'static str, r#in: &FileIn, detail: Option<String>) {
		let mut e = TransferEvent::new(event, r#in.id(), kind_of(r#in));
		if let Some((from, to)) = io_paths(r#in) {
			e.from = Some(from);
			e.to = Some(to);
		}
		e.detail = detail;
		let q = self.transfer.lock();
		e.active = q.active.is_some();
		e.pending = q.pending.len();
		e.blocked = q.blocked.len();
		drop(q);
		transfer_log::append(&e);
	}

	fn log_state_only(&self, event: &'static str, id: yazi_shared::Id, kind: &'static str, detail: Option<String>) {
		let mut e = TransferEvent::new(event, id, kind);
		e.detail = detail;
		let q = self.transfer.lock();
		e.active = q.active.is_some();
		e.pending = q.pending.len();
		e.blocked = q.blocked.len();
		drop(q);
		transfer_log::append(&e);
	}

	fn file(&self, rx: async_priority_channel::Receiver<FileIn, u8>) -> JoinHandle<()> {
		let me = self.clone();
		tokio::spawn(async move {
			loop {
				if let Ok((r#in, _)) = rx.recv().await {
					let id = r#in.id();
					let Some(token) = me.ongoing.lock().get_token(id) else {
						continue;
					};

					let result = select! {
						r = me.file_do(r#in) => r,
						false = token.future() => Ok(())
					};

					if let Err(out) = result {
						me.ops.out(id, out);
					}
				}
			}
		})
	}

	async fn file_do(&self, r#in: FileIn) -> Result<(), TaskOut> {
		match r#in {
			FileIn::Copy(r#in) => self.file.copy(r#in).await.map_err(Into::into),
			FileIn::CopyDo(r#in) => self.file.copy_do(r#in).await.map_err(Into::into),
			FileIn::Cut(r#in) => self.file.cut(r#in).await.map_err(Into::into),
			FileIn::CutDo(r#in) => self.file.cut_do(r#in).await.map_err(Into::into),
			FileIn::Link(r#in) => self.file.link(r#in).await.map_err(Into::into),
			FileIn::LinkDo(r#in) => self.file.link_do(r#in).await.map_err(Into::into),
			FileIn::Hardlink(r#in) => self.file.hardlink(r#in).await.map_err(Into::into),
			FileIn::HardlinkDo(r#in) => self.file.hardlink_do(r#in).await.map_err(Into::into),
			FileIn::Delete(r#in) => self.file.delete(r#in).await.map_err(Into::into),
			FileIn::DeleteDo(r#in) => self.file.delete_do(r#in).await.map_err(Into::into),
			FileIn::Trash(r#in) => self.file.trash(r#in).await.map_err(Into::into),
			FileIn::TrashDo(r#in) => self.file.trash_do(r#in).await.map_err(Into::into),
			FileIn::Download(r#in) => self.file.download(r#in).await.map_err(Into::into),
			FileIn::DownloadDo(r#in) => self.file.download_do(r#in).await.map_err(Into::into),
			FileIn::Upload(r#in) => self.file.upload(r#in).await.map_err(Into::into),
			FileIn::UploadDo(r#in) => self.file.upload_do(r#in).await.map_err(Into::into),
		}
	}

	fn plugin(&self, rx: async_priority_channel::Receiver<PluginIn, u8>) -> JoinHandle<()> {
		let me = self.clone();
		tokio::spawn(async move {
			loop {
				if let Ok((r#in, _)) = rx.recv().await {
					let id = r#in.id();
					let Some(token) = me.ongoing.lock().get_token(id) else {
						continue;
					};

					let result = select! {
						r = me.plugin_do(r#in) => r,
						false = token.future() => Ok(())
					};

					if let Err(out) = result {
						me.ops.out(id, out);
					}
				}
			}
		})
	}

	async fn plugin_do(&self, r#in: PluginIn) -> Result<(), TaskOut> {
		match r#in {
			PluginIn::Entry(r#in) => self.plugin.entry(r#in).await.map_err(Into::into),
		}
	}

	fn fetch(&self, rx: async_priority_channel::Receiver<FetchIn, u8>) -> JoinHandle<()> {
		let me = self.clone();
		tokio::spawn(async move {
			loop {
				if let Ok((r#in, _)) = rx.recv().await {
					let id = r#in.id();
					let Some(token) = me.ongoing.lock().get_token(id) else {
						continue;
					};

					let result = select! {
						r = me.fetch_do(r#in) => r,
						false = token.future() => Ok(())
					};

					if let Err(out) = result {
						me.ops.out(id, out);
					}
				}
			}
		})
	}

	async fn fetch_do(&self, r#in: FetchIn) -> Result<(), TaskOut> {
		self.fetch.fetch(r#in).await.map_err(Into::into)
	}

	fn preload(&self, rx: async_priority_channel::Receiver<PreloadIn, u8>) -> JoinHandle<()> {
		let me = self.clone();
		tokio::spawn(async move {
			loop {
				if let Ok((r#in, _)) = rx.recv().await {
					let id = r#in.id();
					let Some(token) = me.ongoing.lock().get_token(id) else {
						continue;
					};

					let result = select! {
						r = me.preload_do(r#in) => r,
						false = token.future() => Ok(())
					};

					if let Err(out) = result {
						me.ops.out(id, out);
					}
				}
			}
		})
	}

	async fn preload_do(&self, r#in: PreloadIn) -> Result<(), TaskOut> {
		self.preload.preload(r#in).await.map_err(Into::into)
	}

	fn size(&self, rx: async_priority_channel::Receiver<SizeIn, u8>) -> JoinHandle<()> {
		let me = self.clone();
		tokio::spawn(async move {
			loop {
				if let Ok((r#in, _)) = rx.recv().await {
					let id = r#in.id();
					let Some(token) = me.ongoing.lock().get_token(id) else {
						continue;
					};

					let result = select! {
						r = me.size_do(r#in) => r,
						false = token.future() => Ok(())
					};

					if let Err(out) = result {
						me.ops.out(id, out);
					}
				}
			}
		})
	}

	async fn size_do(&self, r#in: SizeIn) -> Result<(), TaskOut> {
		self.size.size(r#in).await.map_err(Into::into)
	}

	fn process(&self, rx: async_priority_channel::Receiver<ProcessIn, u8>) -> JoinHandle<()> {
		let me = self.clone();
		tokio::spawn(async move {
			loop {
				if let Ok((r#in, _)) = rx.recv().await {
					let id = r#in.id();
					let Some(token) = me.ongoing.lock().get_token(id) else {
						continue;
					};

					let result = select! {
						r = me.process_do(r#in) => r,
						false = token.future() => Ok(())
					};

					if let Err(out) = result {
						me.ops.out(id, out);
					}
				}
			}
		})
	}

	async fn process_do(&self, r#in: ProcessIn) -> Result<(), TaskOut> {
		match r#in {
			ProcessIn::Block(r#in) => self.process.block(r#in).await.map_err(Into::into),
			ProcessIn::Orphan(r#in) => self.process.orphan(r#in).await.map_err(Into::into),
			ProcessIn::Bg(r#in) => self.process.bg(r#in).await.map_err(Into::into),
		}
	}

	fn hook(&self, rx: async_priority_channel::Receiver<HookIn, u8>) -> JoinHandle<()> {
		let me = self.clone();
		tokio::spawn(async move {
			loop {
				if let Ok((r#in, _)) = rx.recv().await {
					let id = r#in.id();
					if !me.ongoing.lock().exists(id) {
						continue;
					}
					me.hook_do(r#in).await;
				}
			}
		})
	}

	async fn hook_do(&self, r#in: HookIn) {
		match r#in {
			HookIn::Copy(r#in) => self.hook.copy(r#in).await,
			HookIn::Cut(r#in) => self.hook.cut(r#in).await,
			HookIn::Delete(r#in) => self.hook.delete(r#in).await,
			HookIn::Trash(r#in) => self.hook.trash(r#in).await,
			HookIn::Download(r#in) => self.hook.download(r#in).await,
			HookIn::Upload(r#in) => self.hook.upload(r#in).await,
		}
	}

	fn op(&self, mut rx: mpsc::UnboundedReceiver<TaskOp>) -> JoinHandle<()> {
		let me = self.clone();
		tokio::spawn(async move {
			while let Some(op) = rx.recv().await {
				let id = op.id;
				let mut ongoing = me.ongoing.lock();
				let Some(task) = ongoing.get_mut(id) else {
					me.complete_transfer(id);
					continue;
				};

				op.out.reduce(task);
				if !task.prog.cooked() && task.done.completed() != Some(false) {
					continue; // Not cooked yet, also not canceled
				} else if task.prog.cleaned() == Some(false) {
					continue; // Failed to clean up
				} else if let Some(hook) = task.hook.take() {
					me.hook.submit(hook, LOW);
				} else {
					ongoing.fulfill(id);
					drop(ongoing);
					me.complete_transfer(id);
				}
			}
		})
	}

	#[cfg(test)]
	fn __test_make_copy(id: u64, from: &std::path::Path, to: &std::path::Path) -> FileIn {
		FileIn::Copy(crate::file::FileInCopy {
			id: yazi_shared::Id(id),
			from: yazi_shared::url::UrlBuf::from(from),
			to: yazi_shared::url::UrlBuf::from(to),
			force: false,
			cha: None,
			follow: false,
			retry: 0,
			done: Default::default(),
		})
	}
}

fn kind_of(r#in: &FileIn) -> &'static str {
	match r#in {
		FileIn::Copy(_) | FileIn::CopyDo(_) => "copy",
		FileIn::Cut(_) | FileIn::CutDo(_) => "cut",
		FileIn::Link(_) | FileIn::LinkDo(_) => "link",
		FileIn::Hardlink(_) | FileIn::HardlinkDo(_) => "hardlink",
		FileIn::Delete(_) | FileIn::DeleteDo(_) => "delete",
		FileIn::Trash(_) | FileIn::TrashDo(_) => "trash",
		FileIn::Download(_) | FileIn::DownloadDo(_) => "download",
		FileIn::Upload(_) | FileIn::UploadDo(_) => "upload",
	}
}

fn io_paths(r#in: &FileIn) -> Option<(String, String)> {
	match r#in {
		FileIn::Copy(i) | FileIn::CopyDo(i) => Some((i.from.display().to_string(), i.to.display().to_string())),
		FileIn::Cut(i) | FileIn::CutDo(i) => Some((i.from.display().to_string(), i.to.display().to_string())),
		_ => None,
	}
}

fn blocked_job_of(r#in: &FileIn) -> Option<PausedTransferJob> {
	match r#in {
		FileIn::Copy(i) | FileIn::CopyDo(i) => Some(PausedTransferJob {
			kind: TransferJobKind::Copy,
			from: i.from.clone(),
			to: i.to.clone(),
			force: i.force,
			follow: i.follow,
		}),
		FileIn::Cut(i) | FileIn::CutDo(i) => Some(PausedTransferJob {
			kind: TransferJobKind::Cut,
			from: i.from.clone(),
			to: i.to.clone(),
			force: i.force,
			follow: i.follow,
		}),
		_ => None,
	}
}

fn same_job(a: &PausedTransferJob, b: &PausedTransferJob) -> bool {
	a.kind == b.kind
		&& a.from == b.from
		&& a.to == b.to
		&& a.force == b.force
		&& a.follow == b.follow
}

#[cfg(test)]
mod tests {
	use super::{Runner, TransferQueue};

	#[test]
	fn single_active_and_fifo_progression() {
		let root = std::env::temp_dir().join(format!("yazi-q-{}", std::process::id()));
		std::fs::create_dir_all(&root).unwrap();
		let a = root.join("a");
		let b = root.join("b");

		let mut q = TransferQueue::default();
		q.pending.push_back((Runner::__test_make_copy(1, &a, &b), 1));
		q.pending.push_back((Runner::__test_make_copy(2, &a, &b), 1));

		let (first, _, _) = q.take_next().unwrap();
		assert_eq!(first.id().get(), 1);
		assert!(q.take_next().is_none());

		q.mark_complete(first.id());
		let (second, _, _) = q.take_next().unwrap();
		assert_eq!(second.id().get(), 2);
	}

	#[test]
	fn blocked_queue_retries_when_rechecked() {
		let root = std::env::temp_dir().join(format!("yazi-q2-{}", std::process::id()));
		std::fs::create_dir_all(&root).unwrap();
		let a = root.join("a");
		let b = root.join("b");

		let mut q = TransferQueue::default();
		q.pending.push_back((Runner::__test_make_copy(3, &a, &b), 1));

		let (job, pri, from_blocked) = q.take_next().unwrap();
		assert!(!from_blocked);
		q.mark_blocked(job, pri);

		let (retry, _, from_blocked) = q.take_next().unwrap();
		assert!(from_blocked);
		assert_eq!(retry.id().get(), 3);
	}
}
