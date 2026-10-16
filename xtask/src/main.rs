// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

use anyhow::{bail, Context, Result};
use clap::{Arg, ArgAction, ArgMatches, Command};

#[derive(Debug, PartialEq, Eq)]
enum Task {
    Coverage { dev: bool },
    Vars,
    Ci,
    Powerset,
    BloatDeps { package: String },
    BloatTime { package: String },
    Docs,
}

fn cli() -> Command {
    let package = || {
        Arg::new("package")
            .short('p')
            .long("package")
            .help("package to build")
            .required(true)
    };
    Command::new("xtask")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("coverage").arg(
                Arg::new("dev")
                    .short('d')
                    .long("dev")
                    .help("generate an html report")
                    .action(ArgAction::SetTrue),
            ),
        )
        .subcommand(Command::new("vars"))
        .subcommand(Command::new("ci"))
        .subcommand(Command::new("powerset"))
        .subcommand(Command::new("bloat-deps").arg(package()))
        .subcommand(Command::new("bloat-time").arg(package()))
        .subcommand(Command::new("docs"))
}

fn task_from(matches: &ArgMatches) -> Result<Task> {
    match matches.subcommand() {
        Some(("coverage", arguments)) => Ok(Task::Coverage {
            dev: arguments.get_flag("dev"),
        }),
        Some(("vars", _)) => Ok(Task::Vars),
        Some(("ci", _)) => Ok(Task::Ci),
        Some(("powerset", _)) => Ok(Task::Powerset),
        Some(("bloat-deps", arguments)) => Ok(Task::BloatDeps {
            package: arguments
                .get_one::<String>("package")
                .context("please provide a package with -p")?
                .clone(),
        }),
        Some(("bloat-time", arguments)) => Ok(Task::BloatTime {
            package: arguments
                .get_one::<String>("package")
                .context("please provide a package with -p")?
                .clone(),
        }),
        Some(("docs", _)) => Ok(Task::Docs),
        _ => bail!("a supported xtask subcommand is required"),
    }
}

fn main() -> Result<()> {
    match task_from(&cli().get_matches())? {
        Task::Coverage { dev } => xtaskops::tasks::coverage(dev),
        Task::Vars => {
            println!("root: {:?}", xtaskops::ops::root_dir());
            Ok(())
        }
        Task::Ci => xtaskops::tasks::ci(),
        Task::Powerset => xtaskops::tasks::powerset(),
        Task::BloatDeps { package } => xtaskops::tasks::bloat_deps(&package),
        Task::BloatTime { package } => xtaskops::tasks::bloat_time(&package),
        Task::Docs => xtaskops::tasks::docs(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(arguments: &[&str]) -> Task {
        task_from(&cli().try_get_matches_from(arguments).unwrap()).unwrap()
    }

    #[test]
    fn every_existing_command_keeps_its_dispatch_identity() {
        cli().debug_assert();
        for (command, expected) in [
            ("vars", Task::Vars),
            ("ci", Task::Ci),
            ("powerset", Task::Powerset),
            ("docs", Task::Docs),
            ("coverage", Task::Coverage { dev: false }),
        ] {
            assert_eq!(parse(&["xtask", command]), expected);
        }
    }

    #[test]
    fn coverage_preserves_both_development_flags() {
        for flag in ["-d", "--dev"] {
            assert_eq!(
                parse(&["xtask", "coverage", flag]),
                Task::Coverage { dev: true }
            );
        }
    }

    #[test]
    fn bloat_commands_preserve_both_package_flags() {
        for flag in ["-p", "--package"] {
            assert_eq!(
                parse(&["xtask", "bloat-deps", flag, "actordb"]),
                Task::BloatDeps {
                    package: "actordb".into()
                }
            );
            assert_eq!(
                parse(&["xtask", "bloat-time", flag, "actordb"]),
                Task::BloatTime {
                    package: "actordb".into()
                }
            );
        }
    }

    #[test]
    fn malformed_arguments_fail_before_dispatch() {
        for arguments in [
            vec!["xtask"],
            vec!["xtask", "unknown"],
            vec!["xtask", "coverage", "--dev", "false"],
            vec!["xtask", "bloat-deps"],
            vec!["xtask", "bloat-time", "--package"],
            vec!["xtask", "docs", "--dev"],
        ] {
            assert!(cli().try_get_matches_from(arguments).is_err());
        }
    }
}
