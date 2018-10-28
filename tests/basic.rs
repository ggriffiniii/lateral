extern crate tempfile;

use std::env;
use std::fs;
use std::io::{self, BufRead};
use std::path;
use std::process;
use std::thread;
use std::time;

#[derive(Debug)]
struct TestHarness {
    bin_path: path::PathBuf,
    tmp_dir: tempfile::TempDir,
}

impl TestHarness {
    fn new() -> Self {
        let mut root_path = env::current_exe().unwrap().parent().unwrap().to_path_buf();
        if root_path.ends_with("deps") {
            root_path.pop();
        }
        let bin_path = root_path.join("lateral");
        let tmp_dir = tempfile::tempdir_in(&root_path).unwrap();
        let th = TestHarness { bin_path, tmp_dir };
        assert!(th.run_lateral_cmd(&["start"]).success());
        fs::File::create(th.log_path()).unwrap();
        th
    }

    fn socket_path(&self) -> path::PathBuf {
        self.tmp_dir.path().join("socket")
    }

    fn log_path(&self) -> path::PathBuf {
        self.tmp_dir.path().join("log")
    }

    fn kill(&self) {
        let _ = self
            .lateral_cmd(&["kill"])
            .stdout(process::Stdio::null())
            .stderr(process::Stdio::null())
            .status();
    }

    fn lateral_cmd<I, S>(&self, args: I) -> process::Command
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let mut cmd = process::Command::new(&self.bin_path);
        cmd.arg("--socket");
        cmd.arg(self.socket_path());
        cmd.args(args);
        cmd
    }

    fn run_lateral_cmd<I, S>(&self, args: I) -> process::ExitStatus
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        self.lateral_cmd(args)
            .status()
            .expect("failed to start lateral")
    }

    fn log_reader(&self) -> io::BufReader<fs::File> {
        io::BufReader::new(fs::File::open(self.log_path()).unwrap())
    }
}

impl Drop for TestHarness {
    fn drop(&mut self) {
        self.kill();
    }
}

#[test]
fn basic() {
    let th = TestHarness::new();
    assert!(th.run_lateral_cmd(&["wait"]).success());
}

mod max_paralleism {
    use super::*;
    macro_rules! max_parallelism_test {
        ($testname:ident, $max_parallelism:expr, $num_commands:expr) => {
            #[test]
            fn $testname() {
                let th = TestHarness::new();
                assert!(
                    th.run_lateral_cmd(&["config", "--parallel", &format!("{}", $max_parallelism)])
                        .success()
                );
                for _ in 0..$num_commands {
                    assert!(
                        th.run_lateral_cmd(&[
                            "run",
                            "--",
                            "/bin/bash",
                            "-c",
                            &format!(
                                "echo started >> {0}; sleep 1; echo finished >> {0}",
                                th.log_path().to_string_lossy()
                            )
                        ]).success()
                    );
                }
                assert!(th.run_lateral_cmd(&["wait"]).success());
                let logfile = th.log_reader();
                let max_parallelism = logfile
                    .lines()
                    .scan(0, |running, line| {
                        match line.unwrap().as_str() {
                            "started" => *running += 1,
                            "finished" => *running -= 1,
                            unexpected => panic!("{:?}", unexpected),
                        }
                        Some(*running)
                    }).max()
                    .unwrap();
                assert!(max_parallelism <= $max_parallelism);
            }
        };
    }

    max_parallelism_test!(one, 1, 3);
    max_parallelism_test!(two, 2, 3);
    max_parallelism_test!(ten, 10, 11);
    max_parallelism_test!(twenty, 20, 100);
}

#[test]
fn pause() {
    // start lateral with a max parallelism of 3.
    // Run 5 "long running" commands.
    // While the first 3 commands are running. Change the max parallelism setting to 0.
    // Verify that the 2 pending commands do not run and that wait does not succeed.
    // Change max parallelism back to a non-zero value and ensure wait does succeed.
    let th = TestHarness::new();
    assert!(th.run_lateral_cmd(&["config", "--parallel", "3"]).success());
    for _ in 0..5 {
        assert!(
            th.run_lateral_cmd(&[
                "run",
                "--",
                "/bin/bash",
                "-c",
                &format!(
                    "echo started >> {0}; sleep 5; echo finished >> {0}",
                    th.log_path().to_string_lossy()
                )
            ]).success()
        );
    }
    let mut wait_cmd = th.lateral_cmd(&["wait"]).spawn().unwrap();
    thread::sleep(time::Duration::from_secs(2));
    assert!(th.run_lateral_cmd(&["config", "--parallel", "0"]).success());
    assert!(wait_cmd.try_wait().unwrap().is_none());
    thread::sleep(time::Duration::from_secs(4));
    assert!(wait_cmd.try_wait().unwrap().is_none());
    let logfile = th.log_reader();
    let currently_running = logfile
        .lines()
        .scan(0, |running, line| {
            match line.unwrap().as_str() {
                "started" => *running += 1,
                "finished" => *running -= 1,
                unexpected => panic!("{:?}", unexpected),
            }
            Some(*running)
        }).last()
        .unwrap();
    assert_eq!(0, currently_running);
    assert!(th.run_lateral_cmd(&["config", "--parallel", "3"]).success());
    assert!(wait_cmd.wait().unwrap().success());
}

#[test]
fn pause_from_beginning() {
    // start lateral with a max parallelism of 0.
    // Run 5 commands.
    // Verify that wait does not succeed and nothing runs until the max
    // parallelism is set to non-zero.
    let th = TestHarness::new();
    assert!(th.run_lateral_cmd(&["config", "--parallel", "0"]).success());
    for _ in 0..5 {
        assert!(
            th.run_lateral_cmd(&[
                "run",
                "--",
                "/bin/bash",
                "-c",
                &format!(
                    "echo started >> {0}; echo finished >> {0}",
                    th.log_path().to_string_lossy()
                )
            ]).success()
        );
    }
    let mut wait_cmd = th.lateral_cmd(&["wait"]).spawn().unwrap();
    thread::sleep(time::Duration::from_secs(2));
    let logfile = th.log_reader();
    let commands_started = logfile
        .lines()
        .filter(|line| line.as_ref().unwrap().as_str() == "started")
        .count();
    assert_eq!(0, commands_started);
    assert!(wait_cmd.try_wait().unwrap().is_none());
    assert!(
        th.run_lateral_cmd(&["config", "--parallel", "10"])
            .success()
    );
    assert!(wait_cmd.wait().unwrap().success());
    let logfile = th.log_reader();
    let commands_finished = logfile
        .lines()
        .filter(|line| line.as_ref().unwrap().as_str() == "finished")
        .count();
    assert_eq!(5, commands_finished);
}

#[test]
fn exit_status() {
    let th = TestHarness::new();
    assert!(
        th.run_lateral_cmd(&["run", "--", "/bin/bash", "-c", "exit 0"])
            .success()
    );
    assert!(
        th.run_lateral_cmd(&["run", "--", "/bin/bash", "-c", "exit 42"])
            .success()
    );
    assert!(!th.run_lateral_cmd(&["wait"]).success());
}
