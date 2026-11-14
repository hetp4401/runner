use runner_control::{
    control::{Control, n, s},
    error::HttpError,
    spec,
    storage::Store,
};
use serde_json::{Value, json};
fn normalized(mut value: Value) -> anyhow::Result<Value> {
    if let Some(map) = value.as_object_mut() {
        map.remove("recovery");
    }
    for key in ["env", "keys"] {
        if let Some(values) = value.get_mut(key).and_then(Value::as_array_mut) {
            values.sort_by(|a, b| a.as_str().cmp(&b.as_str()));
        }
    }
    for plan in value
        .get_mut("desired")
        .and_then(Value::as_object_mut)
        .into_iter()
        .flat_map(|m| m.values_mut())
    {
        plan["compose"] = spec::parse(s(plan, "compose"))?;
        plan["managedCompose"] = serde_json::from_str(s(plan, "managedCompose"))?;
        plan.as_object_mut().unwrap().remove("revision");
    }
    Ok(value)
}
fn state(c: &Control) -> Value {
    let mut copies = serde_json::Map::new();
    for (name, list) in &c.copies {
        if list.is_empty() {
            continue;
        }
        let mut list = list.clone();
        list.sort_by_key(|x| (x.replica, x.machine));
        copies.insert(name.clone(), json!(list));
    }
    json!({"projects":c.projects.values().collect::<Vec<_>>(),"copies":copies,"runs":c.runs.values().map(|r|json!({"id":r.id,"machine":r.machine,"ready":r.ready,"handover":r.handover,"retire":r.retire,"drain":r.drain})).collect::<Vec<_>>(),"blocked":c.blocked})
}
#[tokio::test(flavor = "current_thread")]
async fn native_scheduler_replays_deployed_controller_decisions() -> anyhow::Result<()> {
    let fixture: Value = serde_json::from_str(include_str!("fixtures/trace.json"))?;
    let mut c = Control::new(
        Store::memory()?,
        serde_json::from_value(fixture["env"].clone())?,
        n(&fixture, "initial"),
    )?;
    for (i, step) in fixture["trace"].as_array().unwrap().iter().enumerate() {
        let args = step["args"].as_array().unwrap();
        let now = n(step, "now");
        let string = |i: usize| args[i].as_str().unwrap();
        let num = |i: usize| {
            args[i]
                .as_u64()
                .or_else(|| args[i].as_str().and_then(|s| s.parse().ok()))
                .unwrap()
        };
        c.store.begin()?;
        let result = match s(step, "method") {
            "sync" => c.sync(&args[0], None, None, now),
            "putProject" => {
                c.put_project(string(0), args[1].clone(), args[2]["trusted"] == true, now)
            }
            "putEnv" => c.put_env(string(0), &args[1], now),
            "move" => c.move_copy(
                string(0),
                num(1),
                Some(num(2)),
                Some(num(3) as u32),
                "hand",
                None,
                now,
            ),
            "setEnabled" => c.set_enabled(string(0), args[1] == true, now),
            "roll" => c.roll(args.first().and_then(Value::as_u64), now),
            "cancelRoll" => c.cancel_roll(now),
            "deleteProject" => c.delete_project(string(0)),
            other => panic!("missing trace method {other}"),
        };
        let actual = match result {
            Ok(value) => json!({"result":normalized(value)?}),
            Err(error) => {
                json!({"error":{"status":error.downcast_ref::<HttpError>().map(|e|e.status).unwrap_or(500)}})
            }
        };
        assert_eq!(actual, step["expected"], "step {i} {}", s(step, "method"));
        c.store.commit().await?;
        // Projects are a keyed set even though Node's Map preserves insertion order.
        let mut expected = step["state"].clone();
        expected["projects"]
            .as_array_mut()
            .unwrap()
            .sort_by(|a, b| s(a, "name").cmp(s(b, "name")));
        assert_eq!(
            state(&c),
            expected,
            "state after step {i} {}",
            s(step, "method")
        );
    }
    Ok(())
}
