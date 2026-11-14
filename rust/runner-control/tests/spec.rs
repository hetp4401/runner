use runner_control::spec;
use serde_json::json;
#[test]
fn deployed_controller_specifications_and_plan_revisions_match() -> anyhow::Result<()> {
    let fixtures: Vec<serde_json::Value> =
        serde_json::from_str(include_str!("fixtures/spec.json"))?;
    for (i, case) in fixtures.iter().enumerate() {
        let result = spec::build(&case["body"], case["trusted"] == true);
        assert_eq!(
            result.is_ok(),
            case["ok"] == true,
            "fixture {i}: {result:?}"
        );
        if let Ok(spec) = result {
            let value = serde_json::to_value(&spec)?;
            assert_eq!(value, case["spec"], "fixture {i} source");
            let mut plan = value;
            plan["app"] = json!("example");
            plan["replica"] = json!(2);
            plan["v"] = json!(3);
            let mut actual = spec::agent_plan(plan)?;
            let mut expected = case["plan"].clone();
            // JSON object serialization order is not semantic inside managedCompose.
            actual["managedCompose"] =
                serde_json::from_str(actual["managedCompose"].as_str().unwrap())?;
            expected["managedCompose"] =
                serde_json::from_str(expected["managedCompose"].as_str().unwrap())?;
            assert_eq!(actual, expected, "fixture {i} managed plan");
            assert_eq!(
                json!(spec::claims(&spec.compose)),
                case["claims"],
                "fixture {i} claims"
            );
            assert_eq!(
                json!(spec::policy(&spec.compose)),
                case["policy"],
                "fixture {i} policy"
            );
            assert_eq!(
                json!(spec::fixed_ports(&spec.compose, false)),
                case["fixed"]
            );
            assert_eq!(json!(spec::fixed_ports(&spec.compose, true)), case["nodup"]);
        }
    }
    Ok(())
}
#[test]
fn untrusted_compose_cannot_reach_host_or_race_bind_sources() {
    for extra in [
        "    privileged: true\n",
        "    network_mode: host\n",
        "    volumes: [\"/var/run/docker.sock:/var/run/docker.sock\"]\n",
        "    volumes: [\"./:/app\", \"./data:/data\"]\n",
        "    ports: [\"2019:80\"]\n",
        "    env_file: ../secret.env\n",
        "    extra_hosts: [\"host:host-gateway\"]\n",
    ] {
        let compose = format!("services:\n  app:\n    image: busybox\n{extra}");
        assert!(
            spec::build(&json!({"compose":compose}), false).is_err(),
            "accepted {extra}"
        );
    }
    let merged = "x-hidden: &hidden\n  privileged: true\nservices:\n  app:\n    <<: *hidden\n    image: busybox\n";
    assert!(spec::build(&json!({"compose":merged}), false).is_err());
    for volumes in [
        "[\"./:/app:ro\",\"./data:/data\"]",
        "[\"./data:/one\",\"./data:/two\"]",
    ] {
        assert!(spec::build(&json!({"compose":format!("services:\n  app:\n    image: busybox\n    volumes: {volumes}\n")}),false).is_ok());
    }
}
#[test]
fn staged_plan_preserves_data_paths_and_content_identity() -> anyhow::Result<()> {
    let plan = json!({"app":"web","replica":1,"v":1,"compose":"services:\n  web:\n    image: busybox\n    volumes: [\"./data:/data\",\"./config.txt:/config.txt:ro\"]\n    env_file: .env\n    depends_on:\n      migration:\n        condition: service_completed_successfully\n  migration:\n    image: busybox\n","port":null,"files":{"data/seed.txt":"seed","config.txt":"config",".env":"TOKEN='private'","Dockerfile":"FROM busybox"}});
    let first = spec::agent_plan(plan.clone())?;
    let managed: serde_json::Value =
        serde_json::from_str(first["managedCompose"].as_str().unwrap())?;
    assert_eq!(
        managed["services"]["web"]["volumes"],
        json!([
            "${RUNNER_APP_DATA}/data:/data",
            "${RUNNER_APP_DATA}/config.txt:/config.txt:ro"
        ])
    );
    assert_eq!(
        first["dataFiles"],
        json!({"data/seed.txt":"seed","config.txt":"config"})
    );
    assert_eq!(first["oneshotServices"], json!(["migration"]));
    let mut next = plan;
    next["v"] = json!(2);
    assert_eq!(
        spec::agent_plan(next.clone())?["revision"],
        first["revision"]
    );
    next["files"][".env"] = json!("TOKEN='changed'");
    assert_ne!(spec::agent_plan(next)?["revision"], first["revision"]);
    Ok(())
}
