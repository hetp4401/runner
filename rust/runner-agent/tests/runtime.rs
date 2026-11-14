use runner_agent::{files::*, runtime::*};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    os::unix::fs::{PermissionsExt, symlink},
};

#[test]
fn deployed_fingerprints_and_namespaces_match() {
    let fixture: Value = serde_json::from_str(include_str!("fixtures/runtime.json")).unwrap();
    assert_eq!(
        namespace("https://control.example.test/path").unwrap(),
        fixture["namespace"]
    );
    for row in fixture["fixtures"].as_array().unwrap() {
        assert_eq!(
            fingerprint(&row["want"]).unwrap(),
            row["fingerprint"],
            "{}",
            row["want"]
        );
    }
}

#[test]
fn crashes_resume_but_reboots_and_upgrades_get_new_incarnations() {
    let saved = json!({"control":"https://control.test","bootId":"boot1","started":100,"run":"old","agent":"host1"});
    assert_eq!(
        resume(&saved, "boot1", "https://control.test", None, 200).unwrap(),
        (100, "old".into(), None)
    );
    let reboot = resume(&saved, "boot2", "https://control.test", None, 200).unwrap();
    assert_eq!(reboot.0, 200);
    assert_ne!(reboot.1, "old");
    assert_eq!(reboot.2, Some("old".into()));
    assert!(resume(&saved, "boot1", "https://different.test", None, 200).is_err());
    assert_ne!(
        resume(
            &saved,
            "boot1",
            "https://control.test",
            Some("new-version"),
            200
        )
        .unwrap()
        .1,
        "old"
    );
    let mut twice = saved.clone();
    twice["run"] = json!(reboot.1);
    twice["previousRun"] = json!("old");
    assert_eq!(
        resume(&twice, "boot3", "https://control.test", None, 300)
            .unwrap()
            .2,
        Some("old".into())
    );
}

#[test]
fn ownership_cannot_cross_fleets_or_infrastructure() {
    let legacy = std::path::Path::new("/fleet/projects/web");
    let container = json!({"Config":{"Labels":{"com.docker.compose.project.working_dir":legacy,"com.docker.compose.project.config_files":"/fleet/projects/web/compose.yaml"}}});
    assert!(owns_compose(&container, "owner", "web", legacy));
    let mut foreign = container.clone();
    foreign["Config"]["Labels"][OWNER_LABEL] = json!("other-fleet");
    assert!(!owns_compose(&foreign, "owner", "web", legacy));
    let infrastructure = json!({"Config":{"Labels":{OWNER_LABEL:"owner",COPY_LABEL:"app","io.runners.component":"router"}}});
    assert!(!owns_infrastructure(
        &infrastructure,
        "owner",
        "router",
        legacy,
        "/etc/router"
    ));
}

#[test]
fn app_writable_symlinks_and_hardlinks_cannot_escape() {
    let temp = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(temp.path()).unwrap();
    let outside = root.join("outside");
    let data = root.join("data");
    std::fs::create_dir(&outside).unwrap();
    std::fs::create_dir(&data).unwrap();
    let victim = outside.join("victim");
    std::fs::write(&victim, "original").unwrap();
    symlink(&outside, data.join("link")).unwrap();
    assert!(
        write_data_files(
            &data,
            &BTreeMap::from([("link/victim".into(), "escape".into())])
        )
        .is_err()
    );
    std::fs::hard_link(&victim, data.join("hardlink")).unwrap();
    symlink(&victim, data.join("symlink")).unwrap();
    write_data_files(
        &data,
        &BTreeMap::from([
            ("hardlink".into(), "safe".into()),
            ("symlink".into(), "safe".into()),
            ("nested/file".into(), "ok".into()),
        ]),
    )
    .unwrap();
    assert_eq!(std::fs::read_to_string(victim).unwrap(), "original");
    assert!(
        validate_bind_sources(
            &data,
            &[BindSource {
                path: "link".into(),
                read_only: false
            }]
        )
        .is_err()
    );
    validate_bind_sources(
        &data,
        &[
            BindSource {
                path: "nested".into(),
                read_only: false,
            },
            BindSource {
                path: "missing/dir".into(),
                read_only: false,
            },
        ],
    )
    .unwrap();
    let release = stage_deployment(
        &root.join("managed"),
        "web",
        &BTreeMap::from([
            ("nested/source".into(), "code".into()),
            (".env".into(), "SECRET=fake".into()),
        ]),
        "{}",
    )
    .unwrap();
    for (path, expected) in [
        (release.clone(), 0o700),
        (release.join("nested"), 0o755),
        (release.join("nested/source"), 0o644),
        (release.join(".env"), 0o600),
        (release.join("compose.yaml"), 0o600),
    ] {
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
            expected
        );
    }
    assert!(
        stage_deployment(
            &root.join("managed"),
            "evil",
            &BTreeMap::from([("../escape".into(), "no".into())]),
            "{}"
        )
        .is_err()
    );
}

#[test]
fn one_shot_dependencies_and_missing_services_keep_their_meaning() {
    let worker = json!({"Config":{"Labels":{"com.docker.compose.service":"worker"}},"State":{"Running":true,"Status":"running"}});
    let once = json!({"Config":{"Labels":{"com.docker.compose.service":"migrate","io.runners.oneshot":"true"}},"State":{"Running":false,"Status":"exited","ExitCode":0}});
    assert_eq!(container_health(&[worker.clone(), once], &[], &[]), None);
    assert_eq!(
        container_health(&[worker], &["worker".into(), "missing".into()], &[]),
        Some("an application service is missing")
    );
}
