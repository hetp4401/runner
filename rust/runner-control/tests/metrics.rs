use runner_control::{
    control::{Control, n},
    model::Run,
    storage::Store,
};
use serde_json::{Value, json};

fn equivalent(actual: &Value, expected: &Value, path: &str) {
    match (actual, expected) {
        (Value::Number(a), Value::Number(b)) => {
            let (a, b) = (a.as_f64().unwrap(), b.as_f64().unwrap());
            assert!(
                (a - b).abs() <= 1e-8 * b.abs().max(1.),
                "{path}: {a} != {b}"
            );
        }
        (Value::Array(a), Value::Array(b)) => {
            assert_eq!(a.len(), b.len(), "{path}");
            for (i, (a, b)) in a.iter().zip(b).enumerate() {
                equivalent(a, b, &format!("{path}/{i}"));
            }
        }
        (Value::Object(a), Value::Object(b)) => {
            assert_eq!(a.len(), b.len(), "{path}: object keys differ");
            for (key, b) in b {
                equivalent(
                    a.get(key).unwrap_or(&Value::Null),
                    b,
                    &format!("{path}/{key}"),
                );
            }
        }
        _ => assert_eq!(actual, expected, "{path}"),
    }
}

#[test]
fn history_and_live_metrics_match_deployed_controller() -> anyhow::Result<()> {
    let fixture: Value = serde_json::from_str(include_str!("fixtures/metrics.json"))?;
    let mut c = Control::new(
        Store::memory()?,
        serde_json::from_value(fixture["env"].clone())?,
        n(&fixture, "initial"),
    )?;
    c.store.begin()?;
    let mut now = 0;
    for event in fixture["events"].as_array().unwrap() {
        now = n(event, "now");
        if event["cleanup"] == true {
            c.cleanup(now)?;
        } else {
            let run: Run = serde_json::from_value(event["run"].clone())?;
            c.runs.insert(run.id.clone(), run.clone());
            c.take_metrics(&run, &event["metrics"], now)?;
        }
    }
    equivalent(&c.live_now(now), &fixture["expected"]["live"], "live");
    for range in ["1h", "6h", "24h", "7d", "30d"] {
        equivalent(
            &json!(c.history(range, now)?),
            &fixture["expected"]["history"][range],
            range,
        );
    }
    Ok(())
}
