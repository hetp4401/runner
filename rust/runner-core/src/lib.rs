pub mod auth;
pub mod bounded;
pub mod fs;

pub fn json_size(value: &impl serde::Serialize) -> anyhow::Result<usize> {
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value)?;
    Ok(counter.0)
}

pub fn filename_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    const ORDER: &[u8] = b"_-.@/+0123456789abcdefghijklmnopqrstuvwxyz";
    let rank = |b: u8| {
        ORDER
            .iter()
            .position(|x| *x == b.to_ascii_lowercase())
            .unwrap_or(255)
    };
    a.bytes().map(rank).cmp(b.bytes().map(rank)).then_with(|| {
        a.bytes()
            .map(|c| c.is_ascii_uppercase())
            .cmp(b.bytes().map(|c| c.is_ascii_uppercase()))
    })
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn env(key: &str, fallback: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| fallback.into())
}

pub fn number(key: &str, fallback: u64) -> anyhow::Result<u64> {
    Ok(match std::env::var(key) {
        Ok(v) if !v.is_empty() => v.parse().map_err(|_| anyhow::anyhow!("invalid {key}"))?,
        _ => fallback,
    })
}

pub fn random_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// Connection pools and response sizes are deliberately finite. No cookies or
/// automatic redirects: a credential must never follow an untrusted redirect.
pub fn http_client(timeout: std::time::Duration) -> anyhow::Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .timeout(timeout)
        .connect_timeout(std::time::Duration::from_secs(4))
        .pool_max_idle_per_host(2)
        .pool_idle_timeout(std::time::Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()?)
}

pub async fn response_bytes(
    mut response: reqwest::Response,
    limit: usize,
) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(
        response.content_length().is_none_or(|n| n <= limit as u64),
        "response too large"
    );
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        anyhow::ensure!(
            chunk.len() <= limit.saturating_sub(bytes.len()),
            "response too large"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

pub async fn response_json<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
    limit: usize,
) -> anyhow::Result<T> {
    anyhow::ensure!(
        response.status().is_success(),
        "HTTP {}",
        response.status().as_u16()
    );
    Ok(serde_json::from_slice(
        &response_bytes(response, limit).await?,
    )?)
}

pub fn rss_bytes() -> u64 {
    #[cfg(target_os = "linux")]
    {
        let pages = std::fs::read_to_string("/proc/self/statm")
            .ok()
            .and_then(|s| {
                s.split_whitespace()
                    .nth(1)
                    .and_then(|n| n.parse::<u64>().ok())
            })
            .unwrap_or(0);
        let size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        pages.saturating_mul(size.max(0) as u64)
    }
    #[cfg(target_os = "macos")]
    {
        let mut info = std::mem::MaybeUninit::<libc::mach_task_basic_info>::zeroed();
        let mut count = libc::MACH_TASK_BASIC_INFO_COUNT;
        #[allow(deprecated)]
        let result = unsafe {
            libc::task_info(
                libc::mach_task_self(),
                libc::MACH_TASK_BASIC_INFO,
                info.as_mut_ptr().cast(),
                &mut count,
            )
        };
        if result == libc::KERN_SUCCESS {
            unsafe { info.assume_init().resident_size }
        } else {
            0
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        0
    }
}
