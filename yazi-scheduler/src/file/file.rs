use std::{collections::HashMap, mem, path::{Path, PathBuf}};

use anyhow::{Context, Result, anyhow};
use parking_lot::Mutex;
use tokio::{io::{self, ErrorKind::NotFound}, sync::mpsc};
use tracing::{debug, warn};
use yazi_config::YAZI;
use yazi_fs::{Cwd, FsHash128, FsUrl, cha::Cha, ok_or_not_found, path::path_relative_to, provider::{Attrs, FileHolder, Provider, local::Local}};
use yazi_shared::{path::PathCow, url::{AsUrl, UrlBuf, UrlCow, UrlLike}};
use yazi_vfs::{VfsCha, maybe_exists, provider::{self, DirEntry}, unique_file};

use super::{FileIn, FileInCopy, FileInCut, FileInDelete, FileInHardlink, FileInLink, FileInTrash};
use crate::{LOW, NORMAL, TaskOp, TaskOps, ctx, file::{FileInDownload, FileInUpload, FileOutCopy, FileOutCopyDo, FileOutCut, FileOutCutDo, FileOutDelete, FileOutDeleteDo, FileOutDownload, FileOutDownloadDo, FileOutHardlink, FileOutHardlinkDo, FileOutLink, FileOutTrash, FileOutUpload, FileOutUploadDo, Transaction, Traverse}, hook::{HookInOutCopy, HookInOutCut}, ok_or_not_found, progress_or_break};

pub(crate) struct File {
	ops: TaskOps,
	tx:  async_priority_channel::Sender<FileIn, u8>,
	reservations: Mutex<HashMap<yazi_shared::Id, (PathBuf, u64)>>,
	reserved_by_dest: Mutex<HashMap<PathBuf, u64>>,
	#[cfg(test)]
	mocked_available: Mutex<HashMap<PathBuf, u64>>,
}

#[derive(Debug, Clone)]
pub(crate) struct CapacityBlocked {
	pub(crate) dest:      PathBuf,
	pub(crate) needed:    u64,
	pub(crate) available: u64,
	pub(crate) reserved:  u64,
}

impl File {
	pub(crate) fn new(
		ops: &mpsc::UnboundedSender<TaskOp>,
		tx: async_priority_channel::Sender<FileIn, u8>,
	) -> Self {
		Self {
			ops: ops.into(),
			tx,
			reservations: Default::default(),
			reserved_by_dest: Default::default(),
			#[cfg(test)]
			mocked_available: Default::default(),
		}
	}

	pub(crate) async fn copy(&self, mut task: FileInCopy) -> Result<(), FileOutCopy> {
		let id = task.id;
		let needed = task.init().await?.len;
		self.reserve_or_fail(id, &task.to, needed)?;

		if !task.force {
			task.to = unique_file(mem::take(&mut task.to), task.init().await?.is_dir())
				.await
				.context("Cannot determine unique destination name")?;
		}

		self.ops.out(id, HookInOutCopy::from(&task));
		super::traverse::<FileOutCopy, _, _, _, _, _>(
			task,
			async |dir| match provider::create_dir(dir).await {
				Err(e) if e.kind() != io::ErrorKind::AlreadyExists => Err(e)?,
				_ => Ok(()),
			},
			async |task, cha| {
				Ok(if cha.is_orphan() || (cha.is_link() && !task.follow) {
					self.ops.out(id, FileOutCopy::New(0));
					self.requeue(task.into_link(), NORMAL);
				} else {
					self.ops.out(id, FileOutCopy::New(cha.len));
					self.requeue(task, LOW);
				})
			},
			|err| {
				self.ops.out(id, FileOutCopy::Deform(err));
			},
		)
		.await?;

		Ok(self.ops.out(id, FileOutCopy::Succ))
	}

	pub(crate) async fn copy_do(&self, mut task: FileInCopy) -> Result<(), FileOutCopyDo> {
		ok_or_not_found!(task, Transaction::unlink(&task.to).await);
		let mut it =
			ctx!(task, provider::copy_with_progress(&task.from, &task.to, task.cha.unwrap()).await)?;

		loop {
			match progress_or_break!(it, task.done) {
				Ok(0) => break,
				Ok(n) => self.ops.out(task.id, FileOutCopyDo::Adv(n)),
				Err(e) if e.kind() == NotFound => {
					warn!("Copy task partially done: {task:?}");
					break;
				}
				// Operation not permitted (os error 1)
				// Attribute not found (os error 93)
				Err(e)
					if task.retry < YAZI.tasks.bizarre_retry
						&& matches!(e.raw_os_error(), Some(1) | Some(93)) =>
				{
					task.retry += 1;
					self.ops.out(task.id, FileOutCopyDo::Log(format!("Retrying due to error: {e}")));
					return Ok(self.requeue(task, LOW));
				}
				Err(e) => ctx!(task, Err(e))?,
			}
		}
		Ok(self.ops.out(task.id, FileOutCopyDo::Succ))
	}

	pub(crate) async fn cut(&self, mut task: FileInCut) -> Result<(), FileOutCut> {
		let id = task.id;
		let needed = task.init().await?.len;
		self.reserve_or_fail(id, &task.to, needed)?;

		if !task.force {
			task.to = unique_file(mem::take(&mut task.to), task.init().await?.is_dir())
				.await
				.context("Cannot determine unique destination name")?;
		}

		self.ops.out(id, HookInOutCut::from(&task));
		if !task.follow && ok_or_not_found(provider::rename(&task.from, &task.to).await).is_ok() {
			return Ok(self.ops.out(id, FileOutCut::Succ));
		}

		let (mut links, mut files) = (vec![], vec![]);
		let reorder = task.follow && ctx!(task, provider::capabilities(&task.from).await)?.symlink;

		super::traverse::<FileOutCut, _, _, _, _, _>(
			task,
			async |dir| match provider::create_dir(dir).await {
				Err(e) if e.kind() != io::ErrorKind::AlreadyExists => Err(e)?,
				_ => Ok(()),
			},
			|task, cha| {
				let nofollow = cha.is_orphan() || (cha.is_link() && !task.follow);
				self.ops.out(id, FileOutCut::New(if nofollow { 0 } else { cha.len }));

				if nofollow {
					self.requeue(task.into_link(), NORMAL);
				} else {
					match (cha.is_link(), reorder) {
						(_, false) => self.requeue(task, LOW),
						(true, true) => links.push(task),
						(false, true) => files.push(task),
					}
				};

				async { Ok(()) }
			},
			|err| {
				self.ops.out(id, FileOutCut::Deform(err));
			},
		)
		.await?;

		if !links.is_empty() {
			let (tx, mut rx) = mpsc::channel(1);
			for task in links {
				self.requeue(task.with_drop(&tx), LOW);
			}
			drop(tx);
			while rx.recv().await.is_some() {}
		}

		for task in files {
			self.requeue(task, LOW);
		}

		Ok(self.ops.out(id, FileOutCut::Succ))
	}

	pub(crate) async fn cut_do(&self, mut task: FileInCut) -> Result<(), FileOutCutDo> {
		ok_or_not_found!(task, Transaction::unlink(&task.to).await);
		let mut it =
			ctx!(task, provider::copy_with_progress(&task.from, &task.to, task.cha.unwrap()).await)?;

		loop {
			match progress_or_break!(it, task.done) {
				Ok(0) => {
					provider::remove_file(&task.from).await.ok();
					break;
				}
				Ok(n) => self.ops.out(task.id, FileOutCutDo::Adv(n)),
				Err(e) if e.kind() == NotFound => {
					warn!("Cut task partially done: {task:?}");
					break;
				}
				// Operation not permitted (os error 1)
				// Attribute not found (os error 93)
				Err(e)
					if task.retry < YAZI.tasks.bizarre_retry
						&& matches!(e.raw_os_error(), Some(1) | Some(93)) =>
				{
					task.retry += 1;
					self.ops.out(task.id, FileOutCutDo::Log(format!("Retrying due to error: {e}")));
					return Ok(self.requeue(task, LOW));
				}
				Err(e) => ctx!(task, Err(e))?,
			}
		}
		Ok(self.ops.out(task.id, FileOutCutDo::Succ))
	}

	pub(crate) async fn link(&self, mut task: FileInLink) -> Result<(), FileOutLink> {
		if !task.force {
			task.to =
				unique_file(task.to, false).await.context("Cannot determine unique destination name")?;
		}

		Ok(self.requeue(task, NORMAL))
	}

	pub(crate) async fn link_do(&self, task: FileInLink) -> Result<(), FileOutLink> {
		let mut src: PathCow = if task.resolve {
			ok_or_not_found!(
				task,
				provider::read_link(&task.from).await,
				return Ok(self.ops.out(task.id, FileOutLink::Succ))
			)
			.into()
		} else {
			task.from.loc().into()
		};

		if task.relative {
			let canon = ctx!(task, provider::canonicalize(task.to.parent().unwrap()).await)?;
			src = ctx!(task, path_relative_to(canon.loc(), src))?;
		}

		ok_or_not_found!(task, provider::remove_file(&task.to).await);
		ctx!(
			task,
			provider::symlink(&task.to, src, async || {
				Ok(match task.cha {
					Some(cha) => cha.is_dir(),
					None => Self::cha(&task.from, task.resolve, None).await?.is_dir(),
				})
			})
			.await
		)?;

		if task.delete {
			provider::remove_file(&task.from).await.ok();
		}
		Ok(self.ops.out(task.id, FileOutLink::Succ))
	}

	pub(crate) async fn hardlink(&self, mut task: FileInHardlink) -> Result<(), FileOutHardlink> {
		let id = task.id;

		if !task.force {
			task.to =
				unique_file(task.to, false).await.context("Cannot determine unique destination name")?;
		}

		super::traverse::<FileOutHardlink, _, _, _, _, _>(
			task,
			async |dir| match provider::create_dir(dir).await {
				Err(e) if e.kind() != io::ErrorKind::AlreadyExists => Err(e)?,
				_ => Ok(()),
			},
			async |task, _cha| {
				self.ops.out(id, FileOutHardlink::New);
				Ok(self.requeue(task, NORMAL))
			},
			|err| {
				self.ops.out(id, FileOutHardlink::Deform(err));
			},
		)
		.await?;

		Ok(self.ops.out(id, FileOutHardlink::Succ))
	}

	pub(crate) async fn hardlink_do(&self, task: FileInHardlink) -> Result<(), FileOutHardlinkDo> {
		let src = if !task.follow {
			UrlCow::from(&task.from)
		} else if let Ok(p) = provider::canonicalize(&task.from).await {
			UrlCow::from(p)
		} else {
			UrlCow::from(&task.from)
		};

		ok_or_not_found!(task, provider::remove_file(&task.to).await);
		ok_or_not_found!(task, provider::hard_link(&src, &task.to).await);

		Ok(self.ops.out(task.id, FileOutHardlinkDo::Succ))
	}

	pub(crate) async fn delete(&self, task: FileInDelete) -> Result<(), FileOutDelete> {
		let id = task.id;

		super::traverse::<FileOutDelete, _, _, _, _, _>(
			task,
			async |_dir| Ok(()),
			async |task, cha| {
				self.ops.out(id, FileOutDelete::New(cha.len));
				Ok(self.requeue(task, NORMAL))
			},
			|_err| {},
		)
		.await?;

		Ok(self.ops.out(id, FileOutDelete::Succ))
	}

	pub(crate) async fn delete_do(&self, task: FileInDelete) -> Result<(), FileOutDeleteDo> {
		match provider::remove_file(&task.target).await {
			Ok(()) => {}
			Err(e) if e.kind() == NotFound => {}
			Err(_) if !maybe_exists(&task.target).await => {}
			Err(e) => ctx!(task, Err(e))?,
		}
		Ok(self.ops.out(task.id, FileOutDeleteDo::Succ(task.cha.unwrap().len)))
	}

	pub(crate) async fn trash(&self, task: FileInTrash) -> Result<(), FileOutTrash> {
		Ok(self.requeue(task, LOW))
	}

	pub(crate) async fn trash_do(&self, task: FileInTrash) -> Result<(), FileOutTrash> {
		ctx!(task, provider::trash(&task.target).await)?;
		Ok(self.ops.out(task.id, FileOutTrash::Succ))
	}

	pub(crate) async fn download(&self, task: FileInDownload) -> Result<(), FileOutDownload> {
		let id = task.id;

		super::traverse::<FileOutDownload, _, _, _, _, _>(
			task,
			async |dir| {
				let dir = dir.to_owned();
				tokio::task::spawn_blocking(move || _ = Cwd::ensure(dir.as_url())).await.ok();
				Ok(())
			},
			async |task, cha| {
				Ok(if cha.is_orphan() {
					Err(anyhow!("Failed to work on {task:?}: source of symlink doesn't exist"))?
				} else {
					self.ops.out(id, FileOutDownload::New(cha.len));
					self.requeue(task, LOW);
				})
			},
			|err| {
				self.ops.out(id, FileOutDownload::Deform(err));
			},
		)
		.await?;

		Ok(self.ops.out(id, FileOutDownload::Succ))
	}

	pub(crate) async fn download_do(
		&self,
		mut task: FileInDownload,
	) -> Result<(), FileOutDownloadDo> {
		let cha = task.cha.unwrap();

		let cache = ctx!(task, task.target.cache(), "Cannot determine cache path")?;
		let cache_tmp = ctx!(task, Transaction::tmp(&cache).await, "Cannot determine download cache")?;

		let mut it = ctx!(task, provider::copy_with_progress(&task.target, &cache_tmp, cha).await)?;
		loop {
			match progress_or_break!(it, task.done) {
				Ok(0) => {
					Local::regular(&cache).remove_dir_all().await.ok();
					ctx!(task, provider::rename(cache_tmp, cache).await, "Cannot persist downloaded file")?;

					let lock = ctx!(task, task.target.cache_lock(), "Cannot determine cache lock")?;
					let hash = format!("{:x}", cha.hash_u128());
					ctx!(task, Local::regular(&lock).write(hash).await, "Cannot lock cache")?;

					break;
				}
				Ok(n) => self.ops.out(task.id, FileOutDownloadDo::Adv(n)),
				Err(e) if e.kind() == NotFound => {
					warn!("Download task partially done: {task:?}");
					break;
				}
				// Operation not permitted (os error 1)
				// Attribute not found (os error 93)
				Err(e)
					if task.retry < YAZI.tasks.bizarre_retry
						&& matches!(e.raw_os_error(), Some(1) | Some(93)) =>
				{
					task.retry += 1;
					self.ops.out(task.id, FileOutDownloadDo::Log(format!("Retrying due to error: {e}")));
					return Ok(self.requeue(task, LOW));
				}
				Err(e) => ctx!(task, Err(e))?,
			}
		}
		Ok(self.ops.out(task.id, FileOutDownloadDo::Succ))
	}

	pub(crate) async fn upload(&self, task: FileInUpload) -> Result<(), FileOutUpload> {
		let id = task.id;

		super::traverse::<FileOutUpload, _, _, _, _, _>(
			task,
			async |_dir| Ok(()),
			async |task, cha| {
				let cache = ctx!(task, task.cache.as_ref(), "Cannot determine cache path")?;

				Ok(match Self::cha(cache, true, None).await {
					Ok(c) if c.mtime == cha.mtime => {}
					Ok(c) => {
						self.ops.out(id, FileOutUpload::New(c.len));
						self.requeue(task, LOW);
					}
					Err(e) if e.kind() == NotFound => {}
					Err(e) => ctx!(task, Err(e))?,
				})
			},
			|err| {
				self.ops.out(id, FileOutUpload::Deform(err));
			},
		)
		.await?;

		Ok(self.ops.out(id, FileOutUpload::Succ))
	}

	pub(crate) async fn upload_do(&self, task: FileInUpload) -> Result<(), FileOutUploadDo> {
		let cha = task.cha.unwrap();
		let cache = ctx!(task, task.cache.as_ref(), "Cannot determine cache path")?;
		let lock = ctx!(task, task.target.cache_lock(), "Cannot determine cache lock")?;

		let hash = ctx!(task, Local::regular(&lock).read_to_string().await, "Cannot read cache lock")?;
		let hash = ctx!(task, u128::from_str_radix(&hash, 16), "Cannot parse hash from lock")?;
		if hash != cha.hash_u128() {
			Err(anyhow!("Failed to work on: {task:?}: remote file has changed since last download"))?;
		}

		let tmp =
			ctx!(task, Transaction::tmp(&task.target).await, "Cannot determine temporary upload path")?;
		let mut it = ctx!(
			task,
			provider::copy_with_progress(cache, &tmp, Attrs {
				mode:  Some(cha.mode),
				atime: None,
				btime: None,
				mtime: None,
			})
			.await
		)?;

		loop {
			match progress_or_break!(it, task.done) {
				Ok(0) => {
					let cha =
						ctx!(task, Self::cha(&task.target, true, None).await, "Cannot stat original file")?;
					if hash != cha.hash_u128() {
						Err(anyhow!("Failed to work on: {task:?}: remote file has changed during upload"))?;
					}

					ctx!(task, provider::rename(&tmp, &task.target).await, "Cannot persist uploaded file")?;

					let cha =
						ctx!(task, Self::cha(&task.target, true, None).await, "Cannot stat uploaded file")?;
					let hash = format!("{:x}", cha.hash_u128());
					ctx!(task, Local::regular(&lock).write(hash).await, "Cannot lock cache")?;

					break;
				}
				Ok(n) => self.ops.out(task.id, FileOutUploadDo::Adv(n)),
				Err(e) => ctx!(task, Err(e))?,
			}
		}
		Ok(self.ops.out(task.id, FileOutUploadDo::Succ))
	}

	pub(super) async fn cha<U>(url: U, follow: bool, entry: Option<DirEntry>) -> io::Result<Cha>
	where
		U: AsUrl,
	{
		let cha = if let Some(entry) = entry {
			entry.metadata().await?
		} else {
			provider::symlink_metadata(url.as_url()).await?
		};
		Ok(if follow { Cha::from_follow(url, cha).await } else { cha })
	}
}

#[cfg(test)]
mod tests {
	use std::path::PathBuf;

	use yazi_shared::url::UrlBuf;

	use crate::file::{File, FileIn, FileInCopy};

	impl File {
		fn __test_set_available(&self, path: PathBuf, bytes: u64) {
			self.mocked_available.lock().insert(path, bytes);
		}

		fn __test_clear_available(&self) {
			self.mocked_available.lock().clear();
		}
	}

	#[test]
	fn available_bytes_uses_existing_parent_for_nonexistent_dest() {
		let dir = std::env::temp_dir().join(format!("yazi-file-test-{}", std::process::id()));
		std::fs::create_dir_all(&dir).unwrap();
		let deep = dir.join("missing").join("child").join("file.bin");

		let bytes = File::available_bytes(&deep).unwrap();
		assert!(bytes > 0);
	}

	#[test]
	fn check_transfer_slot_blocks_when_reserved_exhausts_capacity() {
		let (op_tx, _op_rx) = tokio::sync::mpsc::unbounded_channel();
		let (tx, _rx) = async_priority_channel::unbounded();
		let file = File::new(&op_tx, tx);

		let dir = std::env::temp_dir().join(format!("yazi-file-test-rsv-{}", std::process::id()));
		std::fs::create_dir_all(&dir).unwrap();
		let src = dir.join("src.bin");
		std::fs::write(&src, vec![0u8; 8]).unwrap();

		let id = yazi_shared::Id(42);
		let dest = dir.join("dest.bin");
		file.reserved_by_dest.lock().insert(dest.clone(), u64::MAX);
		let r#in = FileIn::Copy(FileInCopy {
			id,
			from: UrlBuf::from(PathBuf::from(&src)),
			to: UrlBuf::from(PathBuf::from(&dest)),
			force: false,
			cha: None,
			follow: false,
			retry: 0,
			done: Default::default(),
		});

		assert!(file.check_transfer_slot(&r#in).is_err());
	}

	#[test]
	fn release_reservation_cleans_destination_bucket() {
		let (op_tx, _op_rx) = tokio::sync::mpsc::unbounded_channel();
		let (tx, _rx) = async_priority_channel::unbounded();
		let file = File::new(&op_tx, tx);

		let id = yazi_shared::Id(7);
		let dest = std::env::temp_dir().join(format!("yazi-file-test-clean-{}", std::process::id()));
		file.reservations.lock().insert(id, (dest.clone(), 128));
		file.reserved_by_dest.lock().insert(dest.clone(), 128);

		file.release_reservation(id);

		assert!(!file.reservations.lock().contains_key(&id));
		assert!(!file.reserved_by_dest.lock().contains_key(&dest));
	}

	#[test]
	fn prestart_revalidation_can_block_after_enqueue() {
		let (op_tx, _op_rx) = tokio::sync::mpsc::unbounded_channel();
		let (tx, _rx) = async_priority_channel::unbounded();
		let file = File::new(&op_tx, tx);

		let dir = std::env::temp_dir().join(format!("yazi-file-test-prestart-{}", std::process::id()));
		std::fs::create_dir_all(&dir).unwrap();
		let src = dir.join("src.bin");
		std::fs::write(&src, vec![0u8; 16]).unwrap();
		let dest = dir.join("dest.bin");

		let id = yazi_shared::Id(88);
		let r#in = FileIn::Copy(FileInCopy {
			id,
			from: UrlBuf::from(PathBuf::from(&src)),
			to: UrlBuf::from(PathBuf::from(&dest)),
			force: false,
			cha: None,
			follow: false,
			retry: 0,
			done: Default::default(),
		});

		file.__test_set_available(dest.clone(), 64);
		assert!(file.check_transfer_slot(&r#in).is_ok());

		file.__test_set_available(dest.clone(), 0);
		assert!(file.check_transfer_slot(&r#in).is_err());

		file.__test_clear_available();
	}
}

impl File {
	pub(crate) fn reserve_transfer_slot(&self, r#in: &FileIn) -> Result<(), CapacityBlocked> {
		debug!("reserve_transfer_slot: id={:?}", r#in.id());
		let Some((id, dest, needed)) = self.transfer_need(r#in).ok().flatten() else {
			debug!("reserve_transfer_slot: skip non-local or unsupported transfer input");
			return Ok(());
		};

		if self.reservations.lock().contains_key(&id) {
			debug!("reserve_transfer_slot: already reserved id={id:?}");
			return Ok(());
		}

		self.check_transfer_slot_inner(&dest, needed)?;

		let mut by_dest = self.reserved_by_dest.lock();
		let reserved = *by_dest.get(&dest).unwrap_or(&0);
		by_dest.insert(dest.clone(), reserved.saturating_add(needed));
		drop(by_dest);
		self.reservations.lock().insert(id, (dest, needed));
		debug!("reserve_transfer_slot: reserved id={id:?}");
		Ok(())
	}

	pub(crate) fn check_transfer_slot(&self, r#in: &FileIn) -> Result<(), CapacityBlocked> {
		debug!("check_transfer_slot: id={:?}", r#in.id());
		let Some((_, dest, needed)) = self.transfer_need(r#in).ok().flatten() else {
			debug!("check_transfer_slot: skip non-local or unsupported transfer input");
			return Ok(());
		};

		self.check_transfer_slot_inner(&dest, needed)
	}

	#[inline]
	pub(crate) fn submit(&self, r#in: impl Into<FileIn>, priority: u8) {
		_ = self.tx.try_send(r#in.into(), priority);
	}

	#[inline]
	fn requeue(&self, r#in: impl Into<FileIn>, priority: u8) {
		_ = self.tx.try_send(r#in.into().into_doable(), priority);
	}

	pub(crate) fn release_reservation(&self, id: yazi_shared::Id) {
		let Some((dest, size)) = self.reservations.lock().remove(&id) else {
			return;
		};

		let mut by_dest = self.reserved_by_dest.lock();
		let Some(total) = by_dest.get_mut(&dest) else {
			warn!("Missing destination reservation bucket for task {id:?} at {}", dest.display());
			return;
		};

		if *total < size {
			warn!(
				"Reservation underflow for destination {}: total={}B, releasing={}B",
				dest.display(),
				*total,
				size
			);
		}
		*total = total.saturating_sub(size);
		if *total == 0 {
			by_dest.remove(&dest);
		}
	}

	fn reserve_or_fail(&self, id: yazi_shared::Id, to: &UrlBuf, needed: u64) -> Result<()> {
		if self.reservations.lock().contains_key(&id) {
			return Ok(());
		}

		let Some(dest) = to.as_local().map(PathBuf::from) else {
			return Ok(());
		};

		let available = self.available_bytes_for(&dest)?;
		let mut by_dest = self.reserved_by_dest.lock();
		let reserved = *by_dest.get(&dest).unwrap_or(&0);
		if needed > available.saturating_sub(reserved) {
			return Err(anyhow!(
				"Not enough destination space: needed={}B, available={}B, reserved={}B",
				needed,
				available,
				reserved
			));
		}

		by_dest.insert(dest.clone(), reserved.saturating_add(needed));
		drop(by_dest);
		self.reservations.lock().insert(id, (dest, needed));
		Ok(())
	}

	fn transfer_need(&self, r#in: &FileIn) -> io::Result<Option<(yazi_shared::Id, PathBuf, u64)>> {
		let (id, from, to) = match r#in {
			FileIn::Copy(r#in) | FileIn::CopyDo(r#in) => (r#in.id, &r#in.from, &r#in.to),
			FileIn::Cut(r#in) | FileIn::CutDo(r#in) => (r#in.id, &r#in.from, &r#in.to),
			_ => return Ok(None),
		};

		let Some(dest) = to.as_local().map(PathBuf::from) else {
			return Ok(None);
		};
		let Some(from) = from.as_local() else {
			return Ok(None);
		};

		let needed = match Self::required_bytes(Path::new(from)) {
			Ok(v) => v,
			Err(e) => {
				warn!("transfer_need required_bytes failed for {}: {e}", Path::new(from).display());
				return Err(e);
			}
		};
		Ok(Some((id, dest, needed)))
	}

	fn check_transfer_slot_inner(&self, dest: &PathBuf, needed: u64) -> Result<(), CapacityBlocked> {
		let available = self.available_bytes_for(dest).unwrap_or(u64::MAX);
		let by_dest = self.reserved_by_dest.lock();
		let reserved = *by_dest.get(dest).unwrap_or(&0);
		if needed > available.saturating_sub(reserved) {
			Err(CapacityBlocked {
				dest: dest.clone(),
				needed,
				available,
				reserved,
			})
		} else {
			Ok(())
		}
	}

	fn required_bytes(path: &Path) -> io::Result<u64> {
		debug!("required_bytes: {}", path.display());
		let meta = std::fs::symlink_metadata(path)?;
		if !meta.is_dir() {
			return Ok(meta.len());
		}

		let mut stack = vec![path.to_path_buf()];
		let mut total = 0u64;

		while let Some(dir) = stack.pop() {
			for entry in std::fs::read_dir(&dir)? {
				let entry = entry?;
				let meta = entry.metadata()?;
				if meta.is_dir() {
					stack.push(entry.path());
				} else {
					total = total.saturating_add(meta.len());
				}
			}
		}

		Ok(total)
	}

	fn available_bytes_for(&self, path: &PathBuf) -> io::Result<u64> {
		#[cfg(test)]
		if let Some(v) = self.mocked_available.lock().get(path).copied() {
			return Ok(v);
		}

		Self::available_bytes(path)
	}

	#[cfg(unix)]
	fn available_bytes(path: &PathBuf) -> io::Result<u64> {
		use std::ffi::CString;

		let existing = if path.exists() {
			path.as_path()
		} else {
			path.ancestors().find(|p| p.exists()).unwrap_or(path.as_path())
		};

		let c = CString::new(existing.as_os_str().as_encoded_bytes())
			.map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid path"))?;
		let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
		if unsafe { libc::statvfs(c.as_ptr(), &mut st) } != 0 {
			return Err(io::Error::last_os_error());
		}
		Ok((st.f_bavail as u64).saturating_mul(st.f_frsize as u64))
	}

	#[cfg(not(unix))]
	fn available_bytes(_path: &PathBuf) -> io::Result<u64> {
		Ok(u64::MAX)
	}
}
