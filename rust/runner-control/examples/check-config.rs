//! Read-only migration preflight. Configuration JSON arrives on stdin so
//! credentials never need to appear in shell arguments or diagnostic output.
use runner_control::{control::Control, storage::Store};
use serde_json::json;
use std::collections::BTreeMap;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let env: BTreeMap<String, String> = serde_json::from_reader(std::io::stdin())?;
    anyhow::ensure!(
        env.get("CONFIG_STORAGE").is_some_and(|s| s == "d1"),
        "this preflight only reads D1 configuration"
    );
    let started = std::time::Instant::now();
    let store = Store::open(&env)
        .await
        .map_err(|_| anyhow::anyhow!("D1 read-only configuration load failed"))?;
    let c = Control::new(store, env, runner_core::now_ms())?;
    for p in c.projects.values() {
        for version in std::iter::once(p.version).chain(p.stable.filter(|v| *v != p.version)) {
            anyhow::ensure!(
                c.version(&p.name, version).ok().flatten().is_some(),
                "project {} version {} failed native parsing",
                p.name,
                version
            );
        }
    }
    let versions = c.store.rows("SELECT count(*) AS n FROM versions", &[])?[0]["n"].clone();
    println!(
        "{}",
        json!({"projects":c.projects.len(),"versions":versions,"configuration":c.store.diagnostics(),"memoryBytes":runner_core::rss_bytes(),"loadMs":started.elapsed().as_millis()})
    );
    Ok(())
}
