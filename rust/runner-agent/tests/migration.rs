use runner_agent::runtime::{equivalent_plan, fingerprint};
use serde_json::{Value, json};
#[test]
fn controller_yaml_format_changes_preserve_only_identical_workloads() -> anyhow::Result<()> {
    let before = json!({"app":"web","replica":2,"v":1,"port":43123,"ready":"/ready","compose":"services:\n  web:\n    image: nginx\n    ports:\n      - 43123:80\n","files":{".env":"TOKEN=old\n","index.html":"hello"},"managedCompose":"{\"services\":{\"web\":{\"image\":\"nginx\",\"ports\":[\"43123:80\"]}}}","services":["web"],"oneshotServices":[],"dataFiles":[]});
    let mut after = before.clone();
    let parsed: serde_yaml::Value = serde_yaml::from_str(before["compose"].as_str().unwrap())?;
    after["compose"] = json!(serde_yaml::to_string(&parsed)?);
    after["v"] = json!(2);
    after["revision"] = json!("new-controller-revision");
    assert_ne!(fingerprint(&before)?, fingerprint(&after)?);
    assert!(equivalent_plan(&before, &after));
    for (key, value) in [
        ("port", json!(40000)),
        ("ready", json!("/different")),
        ("replica", json!(3)),
        ("app", json!("other")),
        ("incarnation", json!("forced")),
        ("files", json!({".env":"TOKEN=changed\n"})),
        (
            "managedCompose",
            json!("{\"services\":{\"web\":{\"image\":\"different\"}}}"),
        ),
        (
            "compose",
            json!("services:\n  web:\n    image: different\n"),
        ),
        ("dataFiles", json!(["new-data-file"])),
    ] {
        let mut changed = after.clone();
        changed[key] = value;
        assert!(
            !equivalent_plan(&before, &changed),
            "must not adopt changed {key}"
        );
    }
    assert!(!equivalent_plan(&Value::Null, &after));
    Ok(())
}
