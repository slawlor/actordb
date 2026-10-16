use std::process::Command;

#[test]
fn help_lists_every_existing_command_without_running_tasks() {
    let output = Command::new(env!("CARGO_BIN_EXE_xtask"))
        .arg("--help")
        .output()
        .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    for command in [
        "coverage",
        "vars",
        "ci",
        "powerset",
        "bloat-deps",
        "bloat-time",
        "docs",
    ] {
        assert!(help.contains(command), "missing {command} from help");
    }
}

#[test]
fn invalid_cli_exits_unsuccessfully_before_running_tasks() {
    for arguments in [
        vec![],
        vec!["unknown"],
        vec!["bloat-deps"],
        vec!["bloat-time", "--package"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_xtask"))
            .args(arguments)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert!(!output.stderr.is_empty());
    }
}

#[test]
fn vars_keeps_the_existing_read_only_root_output() {
    let output = Command::new(env!("CARGO_BIN_EXE_xtask"))
        .arg("vars")
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8(output.stdout)
        .unwrap()
        .starts_with("root: "));
    assert!(output.stderr.is_empty());
}
