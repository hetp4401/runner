use std::{
    fs::{File, OpenOptions},
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Component, Path, PathBuf},
};

pub fn relative_path(path: &str) -> anyhow::Result<PathBuf> {
    anyhow::ensure!(
        !path.is_empty() && !path.contains('\\') && !path.starts_with('/'),
        "invalid file path"
    );
    anyhow::ensure!(
        path.split('/')
            .all(|p| !p.is_empty() && p != "." && p != ".."),
        "invalid file path"
    );
    anyhow::ensure!(
        Path::new(path)
            .components()
            .all(|p| matches!(p, Component::Normal(_))),
        "invalid file path"
    );
    Ok(path.into())
}

pub fn private_directory(path: &Path) -> anyhow::Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut current = PathBuf::new();
    for part in absolute.components() {
        anyhow::ensure!(
            !matches!(part, Component::ParentDir),
            "parent traversal in managed path"
        );
        current.push(part);
        if !current.exists() {
            let mut builder = std::fs::DirBuilder::new();
            use std::os::unix::fs::DirBuilderExt;
            match builder.mode(0o700).create(&current) {
                Ok(_) => (),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => (),
                Err(e) => return Err(e.into()),
            }
        }
        let meta = std::fs::symlink_metadata(&current)?;
        anyhow::ensure!(
            meta.is_dir() && !meta.file_type().is_symlink(),
            "managed directory contains a symlink or non-directory"
        );
    }
    anyhow::ensure!(
        std::fs::canonicalize(&absolute)? == absolute,
        "managed directory resolves through a symlink"
    );
    Ok(absolute)
}

pub fn atomic_write(path: &Path, content: &[u8], mode: u32) -> anyhow::Result<()> {
    atomic_file(path, mode, |file| {
        file.write_all(content)?;
        Ok(())
    })
}

pub fn atomic_json(path: &Path, value: &impl serde::Serialize) -> anyhow::Result<()> {
    atomic_file(path, 0o600, |file| {
        let mut writer = std::io::BufWriter::with_capacity(16 << 10, file);
        serde_json::to_writer(&mut writer, value)?;
        writer.flush()?;
        Ok(())
    })
}

fn atomic_file(
    path: &Path,
    mode: u32,
    write: impl FnOnce(&mut File) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("file has no parent"))?;
    let temporary = parent.join(format!(".runner-write-{}", crate::random_id()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(mode)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&temporary)?;
        file.set_permissions(std::fs::Permissions::from_mode(mode))?;
        write(&mut file)?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temporary);
    }
    result
}
