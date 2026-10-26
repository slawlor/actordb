//! Atomic journal replacement. Keep publication behind both durability barriers.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;

use crate::checkpoint::CheckpointError;

pub(super) fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), CheckpointError> {
    let temporary = path.with_extension("pending");
    #[cfg(test)]
    faults::check(path, faults::Stage::Create)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temporary)?;
    #[cfg(test)]
    if faults::take(path, faults::Stage::Write) {
        // Model a write failing after a partial temporary-file update.
        file.write_all(&bytes[..bytes.len() / 2])?;
        return Err(faults::error().into());
    }
    file.write_all(bytes)?;
    #[cfg(test)]
    faults::check(path, faults::Stage::FileSync)?;
    file.sync_all()?;
    #[cfg(test)]
    faults::check(path, faults::Stage::Rename)?;
    fs::rename(&temporary, path)?;
    // This fault point intentionally follows the real rename: the visible
    // journal can advance despite the operation returning a durability error.
    #[cfg(test)]
    faults::check(path, faults::Stage::ParentSync)?;
    File::open(
        path.parent()
            .ok_or_else(|| CheckpointError::Invalid("file lacks parent directory".into()))?,
    )?
    .sync_all()?;
    Ok(())
}

#[cfg(test)]
pub(super) mod faults {
    use std::cell::RefCell;
    use std::path::{Path, PathBuf};

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum Stage {
        Create,
        Write,
        FileSync,
        Rename,
        ParentSync,
    }
    pub(crate) const STAGES: [Stage; 5] = [
        Stage::Create,
        Stage::Write,
        Stage::FileSync,
        Stage::Rename,
        Stage::ParentSync,
    ];
    thread_local! {
        static NEXT: RefCell<Option<(PathBuf, Stage)>> = const { RefCell::new(None) };
    }
    pub(crate) struct FaultGuard(Option<(PathBuf, Stage)>);
    impl Drop for FaultGuard {
        fn drop(&mut self) {
            NEXT.with(|next| *next.borrow_mut() = self.0.take());
        }
    }
    pub(crate) fn inject(path: &Path, stage: Stage) -> FaultGuard {
        FaultGuard(NEXT.with(|next| next.replace(Some((path.to_path_buf(), stage)))))
    }
    pub(crate) fn take(path: &Path, stage: Stage) -> bool {
        NEXT.with(|next| {
            let mut next = next.borrow_mut();
            if next
                .as_ref()
                .is_some_and(|(target, point)| target == path && *point == stage)
            {
                next.take();
                true
            } else {
                false
            }
        })
    }
    pub(crate) fn error() -> std::io::Error {
        std::io::Error::other("injected membership journal I/O failure")
    }
    pub(crate) fn check(path: &Path, stage: Stage) -> Result<(), std::io::Error> {
        if take(path, stage) {
            Err(error())
        } else {
            Ok(())
        }
    }
}
