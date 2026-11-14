use anyhow::ensure;
use runner_core::fs::{atomic_json, private_directory};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::unix::{fs::OpenOptionsExt, process::CommandExt},
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Deserialize)]
pub struct Manifest {
    pub schema: u32,
    pub runtime: String,
    pub targets: BTreeMap<String, Artifact>,
}
#[derive(Deserialize)]
pub struct Artifact {
    pub sha256: String,
    pub size: u64,
    pub path: String,
}
#[derive(Serialize, Deserialize)]
struct Pointer {
    revision: String,
    target: String,
}
pub fn target() -> String {
    format!(
        "{}-{}",
        std::env::consts::ARCH,
        if cfg!(target_os = "linux") {
            if cfg!(target_env = "musl") {
                "unknown-linux-musl"
            } else {
                "unknown-linux-gnu"
            }
        } else if cfg!(target_os = "macos") {
            "apple-darwin"
        } else {
            "unsupported"
        }
    )
}
fn valid_hash(hash: &str) -> bool {
    hash.len() == 64
        && hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn hash_file(path: &Path) -> anyhow::Result<String> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    ensure!(
        file.metadata()?.is_file() && file.metadata()?.len() <= 32 << 20,
        "invalid agent binary"
    );
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 16384];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(hex::encode(hash.finalize()))
}
pub async fn install(
    base: &Path,
    origin: &str,
    client: &reqwest::Client,
) -> anyhow::Result<String> {
    let manifest: Manifest = runner_core::response_json(
        client
            .get(format!(
                "{}/agent/rust/manifest.json",
                origin.trim_end_matches('/')
            ))
            .send()
            .await?,
        64 << 10,
    )
    .await?;
    ensure!(
        manifest.schema == 1 && manifest.runtime == "rust",
        "unsupported agent manifest"
    );
    let target = target();
    let artifact = manifest
        .targets
        .get(&target)
        .ok_or_else(|| anyhow::anyhow!("no compatible agent binary is available"))?;
    ensure!(
        valid_hash(&artifact.sha256) && artifact.size > 0 && artifact.size <= 32 << 20,
        "invalid agent binary manifest"
    );
    ensure!(
        artifact.path.starts_with("/agent/rust/")
            && !artifact.path.contains("..")
            && !artifact.path.contains('?')
            && !artifact.path.contains('#'),
        "invalid agent binary path"
    );
    let root = private_directory(&base.join("agent-code"))?;
    let releases = private_directory(&root.join("releases"))?;
    let release = releases.join(format!("rust-{}", artifact.sha256));
    let binary = release.join("runner-agent");
    if binary.exists() {
        private_directory(&release)?;
        ensure!(
            hash_file(&binary)? == artifact.sha256,
            "cached agent checksum failed"
        );
    } else {
        let stage =
            private_directory(&releases.join(format!(".download-{}", runner_core::random_id())))?;
        let result = async {
            let mut response = client
                .get(format!("{}{}", origin.trim_end_matches('/'), artifact.path))
                .timeout(Duration::from_secs(120))
                .send()
                .await?;
            ensure!(response.status().is_success(), "agent download failed");
            ensure!(
                response.content_length().is_none_or(|n| n == artifact.size),
                "agent binary size differs"
            );
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .custom_flags(libc::O_NOFOLLOW)
                .mode(0o700)
                .open(stage.join("runner-agent"))?;
            let mut hash = Sha256::new();
            let mut size = 0_u64;
            while let Some(chunk) = response.chunk().await? {
                ensure!(
                    chunk.len() as u64 <= artifact.size.saturating_sub(size),
                    "agent binary is too large"
                );
                size += chunk.len() as u64;
                hash.update(&chunk);
                file.write_all(&chunk)?;
            }
            ensure!(
                size == artifact.size && hex::encode(hash.finalize()) == artifact.sha256,
                "agent binary checksum failed"
            );
            file.sync_all()?;
            File::open(&stage)?.sync_all()?;
            std::fs::rename(&stage, &release)?;
            File::open(&releases)?.sync_all()?;
            Ok::<_, anyhow::Error>(())
        }
        .await;
        let _ = std::fs::remove_dir_all(&stage);
        result?;
    }
    atomic_json(
        &root.join("rust-current.json"),
        &Pointer {
            revision: artifact.sha256.clone(),
            target,
        },
    )?;
    // Keep two recent releases and the executable running this process. Old
    // JavaScript releases belong to the legacy launcher and are left intact.
    let active = std::env::var("RUNNER_ACTIVE_BINARY_REVISION").unwrap_or_default();
    let mut old = std::fs::read_dir(&releases)?
        .filter_map(Result::ok)
        .filter(|e| {
            e.file_type().is_ok_and(|t| t.is_dir())
                && e.file_name()
                    .to_str()
                    .and_then(|n| n.strip_prefix("rust-"))
                    .is_some_and(valid_hash)
        })
        .collect::<Vec<_>>();
    old.sort_by_key(|e| std::cmp::Reverse(e.metadata().and_then(|m| m.modified()).ok()));
    for entry in old.into_iter().skip(2) {
        let name = entry.file_name();
        if name != format!("rust-{active}").as_str() && entry.path() != release {
            let _ = std::fs::remove_dir_all(entry.path());
        }
    }
    Ok(artifact.sha256.clone())
}
/// The image's original executable is also the launcher. It execs the verified
/// cached release in-place, so no supervisor/runtime remains resident per agent.
pub fn launch_cached() -> anyhow::Result<()> {
    let base = PathBuf::from(runner_core::env("RUNNER_DATA", "/var/lib/runner"));
    if !base.join("agent-code").exists() {
        return Ok(());
    }
    private_directory(&base.join("agent-code"))?;
    let pointer = base.join("agent-code/rust-current.json");
    let file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(pointer)
    {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    ensure!(
        file.metadata()?.len() < 4096,
        "invalid agent release pointer"
    );
    let pointer: Pointer = serde_json::from_reader(file)?;
    ensure!(
        valid_hash(&pointer.revision) && pointer.target == target(),
        "incompatible cached agent"
    );
    if std::env::var("RUNNER_ACTIVE_BINARY_REVISION")
        .ok()
        .as_deref()
        == Some(&pointer.revision)
    {
        return Ok(());
    }
    let binary = base
        .join("agent-code/releases")
        .join(format!("rust-{}", pointer.revision))
        .join("runner-agent");
    private_directory(binary.parent().unwrap())?;
    ensure!(
        hash_file(&binary)? == pointer.revision,
        "cached agent checksum failed"
    );
    let error = std::process::Command::new(binary)
        .args(std::env::args_os().skip(1))
        .env("RUNNER_ACTIVE_BINARY_REVISION", &pointer.revision)
        .env("RUNNER_INSTALL_REVISION", &pointer.revision)
        .exec();
    Err(error.into())
}
