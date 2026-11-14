use crate::runtime::{BindSource, project_name};
use anyhow::ensure;
use runner_core::fs::{private_directory, relative_path};
use std::{
    collections::BTreeMap,
    ffi::CString,
    fs::{File, OpenOptions},
    io::Write,
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    },
    path::{Path, PathBuf},
};

pub fn stage_deployment(
    root: &Path,
    key: &str,
    files: &BTreeMap<String, String>,
    compose: &str,
) -> anyhow::Result<PathBuf> {
    project_name("validate", key)?;
    let parent = private_directory(&root.join(key))?;
    let release = parent.join(format!("release-{}", runner_core::random_id()));
    std::fs::DirBuilder::new().mode(0o700).create(&release)?;
    let result = (|| {
        for (name, contents) in files
            .iter()
            .filter(|(n, _)| n.as_str() != "compose.yaml")
            .map(|(n, c)| (n.as_str(), c.as_str()))
            .chain(std::iter::once(("compose.yaml", compose)))
        {
            let target = release.join(relative_path(name)?);
            let parent = target.parent().unwrap();
            std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o755)
                .create(parent)?;
            let mut folder = parent;
            while folder != release {
                std::fs::set_permissions(folder, std::fs::Permissions::from_mode(0o755))?;
                folder = folder.parent().unwrap();
            }
            let mode = if matches!(name, ".env" | "compose.yaml") {
                0o600
            } else {
                0o644
            };
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .custom_flags(libc::O_NOFOLLOW)
                .mode(mode)
                .open(target)?;
            file.set_permissions(std::fs::Permissions::from_mode(mode))?;
            file.write_all(contents.as_bytes())?;
        }
        Ok::<_, anyhow::Error>(())
    })();
    if let Err(error) = result {
        let _ = std::fs::remove_dir_all(&release);
        return Err(error);
    }
    Ok(release)
}
fn directory(path: &Path) -> anyhow::Result<File> {
    Ok(OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?)
}
fn open_child(
    parent: &File,
    name: &str,
    flags: libc::c_int,
    mode: libc::mode_t,
) -> anyhow::Result<File> {
    let name = CString::new(name)?;
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            mode as libc::c_uint,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}
fn mkdir_child(parent: &File, name: &str) -> anyhow::Result<()> {
    let name = CString::new(name)?;
    let result = unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o755) };
    if result < 0 {
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::AlreadyExists {
            return Err(error.into());
        }
    }
    Ok(())
}
pub fn write_data_files(root: &Path, files: &BTreeMap<String, String>) -> anyhow::Result<()> {
    if files.is_empty() {
        return Ok(());
    }
    std::fs::create_dir_all(root)?;
    let top = directory(root)?;
    for (name, content) in files {
        relative_path(name)?;
        let parts: Vec<_> = name.split('/').collect();
        let mut parent = top.try_clone()?;
        for part in &parts[..parts.len() - 1] {
            mkdir_child(&parent, part)?;
            parent = open_child(&parent, part, libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
        }
        let temporary = format!(".runner-write-{}", runner_core::random_id());
        let result = (|| {
            let mut file = open_child(
                &parent,
                &temporary,
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
                0o644,
            )?;
            file.write_all(content.as_bytes())?;
            let from = CString::new(temporary.clone())?;
            let to = CString::new(*parts.last().unwrap())?;
            let result = unsafe {
                libc::renameat(
                    parent.as_raw_fd(),
                    from.as_ptr(),
                    parent.as_raw_fd(),
                    to.as_ptr(),
                )
            };
            if result < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            Ok::<_, anyhow::Error>(())
        })();
        if result.is_err() {
            let name = CString::new(temporary)?;
            unsafe {
                libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), 0);
            }
        }
        result?;
    }
    Ok(())
}
pub fn validate_bind_sources(root: &Path, sources: &[BindSource]) -> anyhow::Result<()> {
    if sources.is_empty() {
        return Ok(());
    }
    let top = directory(root)?;
    for source in sources {
        if source.path == "." {
            continue;
        }
        let name = source.path.strip_prefix("./").unwrap_or(&source.path);
        relative_path(name)?;
        let parts: Vec<_> = name.split('/').collect();
        let mut parent = top.try_clone()?;
        for (i, part) in parts.iter().enumerate() {
            let flags = libc::O_RDONLY
                | libc::O_NONBLOCK
                | if i + 1 < parts.len() {
                    libc::O_DIRECTORY
                } else {
                    0
                };
            let file = match open_child(&parent, part, flags, 0) {
                Ok(file) => file,
                Err(e)
                    if e.downcast_ref::<std::io::Error>()
                        .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
                {
                    mkdir_child(&parent, part)?;
                    open_child(&parent, part, libc::O_RDONLY | libc::O_DIRECTORY, 0)?
                }
                Err(e) => return Err(e),
            };
            let metadata = file.metadata()?;
            ensure!(
                metadata.is_dir() || (metadata.is_file() && metadata.nlink() == 1),
                "bind source must be a directory or a file without hard links"
            );
            parent = file;
        }
    }
    Ok(())
}
