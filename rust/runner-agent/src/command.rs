use std::{
    collections::{BTreeMap, VecDeque},
    process::Stdio,
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
    sync::Semaphore,
};

#[derive(Clone)]
pub struct Commands {
    environment: Arc<BTreeMap<String, String>>,
    slots: Arc<Semaphore>,
}
pub struct Output {
    pub ok: bool,
    pub out: String,
    pub stderr: String,
}
impl Commands {
    pub fn new(environment: &BTreeMap<String, String>) -> Self {
        Self {
            environment: Arc::new(
                environment
                    .iter()
                    .filter(|(key, _)| {
                        let upper = key.to_ascii_uppercase();
                        !["TOKEN", "SECRET", "PASSWORD", "KEY"]
                            .iter()
                            .any(|word| upper.contains(word))
                    })
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            ),
            slots: Arc::new(Semaphore::new(8)),
        }
    }
    pub fn process(
        &self,
        executable: &str,
        arguments: &[String],
        extra: &BTreeMap<String, String>,
    ) -> Command {
        use std::os::unix::process::CommandExt;
        let mut command = Command::new(executable);
        command
            .args(arguments)
            .env_clear()
            .envs(self.environment.iter())
            .envs(extra)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        command.as_std_mut().process_group(0);
        command
    }
    pub async fn run(
        &self,
        executable: &str,
        arguments: &[String],
        extra: &BTreeMap<String, String>,
        timeout: Duration,
        structured: bool,
    ) -> anyhow::Result<Output> {
        let _permit = self.slots.acquire().await?;
        let mut child = self.process(executable, arguments, extra).spawn()?;
        let pid = child.id();
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        let limit = if structured { 4 << 20 } else { 64 << 10 };
        let output = async {
            let (status, stdout, stderr) = tokio::join!(
                child.wait(),
                capture(stdout, limit, structured),
                capture(stderr, 16 << 10, false)
            );
            let stdout = stdout?;
            let stderr = stderr?;
            let status = status?;
            Ok::<_, anyhow::Error>(Output {
                ok: status.success(),
                out: String::from_utf8_lossy(&stdout).trim().into(),
                stderr: String::from_utf8_lossy(&stderr).trim().into(),
            })
        };
        match tokio::time::timeout(timeout, output).await {
            Ok(result) => result,
            Err(_) => {
                if let Some(pid) = pid {
                    unsafe {
                        libc::kill(-(pid as i32), libc::SIGKILL);
                    }
                }
                let _ = child.kill().await;
                anyhow::bail!("command timed out")
            }
        }
    }
    pub async fn docker(&self, arguments: &[String]) -> anyhow::Result<Output> {
        let structured = matches!(
            arguments.first().map(String::as_str),
            Some("inspect" | "ps")
        );
        self.run(
            "docker",
            arguments,
            &BTreeMap::new(),
            Duration::from_secs(30),
            structured,
        )
        .await
    }
}
async fn capture(
    mut input: impl AsyncRead + Unpin,
    limit: usize,
    reject_overflow: bool,
) -> anyhow::Result<Vec<u8>> {
    let mut chunks = VecDeque::new();
    let mut size = 0;
    let mut buffer = [0u8; 8192];
    let mut overflow = false;
    loop {
        let count = input.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        if reject_overflow && size + count > limit {
            overflow = true;
            continue;
        }
        chunks.push_back(buffer[..count].to_vec());
        size += count;
        while size > limit {
            if let Some(first) = chunks.pop_front() {
                size -= first.len();
            }
        }
    }
    anyhow::ensure!(
        !overflow,
        "command response exceeded the bounded output limit"
    );
    let mut output = Vec::with_capacity(size);
    for chunk in chunks {
        output.extend_from_slice(&chunk);
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn build_output_is_bounded_and_secrets_are_not_inherited() {
        let environment = BTreeMap::from([
            ("PATH".into(), "/usr/bin:/bin".into()),
            ("JOIN_TOKEN".into(), "private".into()),
        ]);
        let commands = Commands::new(&environment);
        let output = commands.run("sh", &["-c".into(), "test -z \"$JOIN_TOKEN\" || exit 9; i=0; while [ $i -lt 20000 ]; do echo abcdefghijklmnopqrstuvwxyz; i=$((i+1)); done; echo tail-marker".into()], &BTreeMap::new(), Duration::from_secs(10), false).await.unwrap();
        assert!(output.ok);
        assert!(output.out.len() <= 65536);
        assert!(output.out.ends_with("tail-marker"));
    }
}
