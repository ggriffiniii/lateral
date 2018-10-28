use crossbeam_channel as channel;
use nix::unistd::{fork, getpid, ForkResult};
use std;
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use workpool;
use {
    net::{read_req, write_resp, ClosedOr},
    ConfigResp, Error, GetPidResp, GlobalOpts, KillResp, RunResp, ServerRunSpec, ShutdownResp,
    WaitResp,
};

// Options for the start subcommand.
#[derive(StructOpt, Debug, Clone)]
pub struct Opts {
    #[structopt(long = "parallel", short = "p", default_value = "10")]
    parallel: i32,
    #[structopt(long = "foreground", short = "f")]
    foreground: bool,
}

pub fn execute(global_opts: &GlobalOpts, opts: &Opts) -> Result<(), Error> {
    debug!("start command");
    if let Some(parent) = global_opts.socket_path().parent() {
        std::fs::create_dir_all(parent)?;
    }
    let listener = UnixListener::bind(&global_opts.socket_path())?;
    if !opts.foreground {
        match fork()? {
            ForkResult::Parent { .. } => return Ok(()),
            ForkResult::Child => {}
        }
    }
    Server::run(global_opts, opts.clone(), listener)
}

#[derive(Debug, Clone)]
struct Server(Arc<Mutex<ServerData>>);

#[derive(Debug)]
struct ServerData {
    global_opts: GlobalOpts,
    opts: Opts,
    stop_handle: stoppable_listener::StopHandle,
    state: State,
}

#[derive(Debug)]
enum State {
    Running {
        runner: workpool::dynamic_pool::DynamicPool<ServerRunSpec, RunSpecWorker, RunSpecReducer>,
    },
    Waiting {
        wait_handle: workpool::dynamic_pool::WaitHandle<bool>,
    },
}

impl Server {
    fn lock(&self) -> MutexGuard<ServerData> {
        self.0.lock().expect("mutex is poisoned")
    }

    fn run(global_opts: &GlobalOpts, opts: Opts, listener: UnixListener) -> Result<(), Error> {
        let (stop_handle, mut stoppable_listener) =
            stoppable_listener::Listener::from_listener(listener)?;
        let runner = workpool::new()
            .set_worker(RunSpecWorker)
            .set_reducer(RunSpecReducer::default())
            .set_concurrency_limit(opts.parallel.into())
            .create_dynamic_pool();
        let srv = Server(Arc::new(Mutex::new(ServerData {
            global_opts: global_opts.clone(),
            opts,
            stop_handle,
            state: State::Running { runner },
        })));
        let res = srv.handle_incoming(&mut stoppable_listener);
        debug!("server done");
        res
    }

    fn handle_incoming(
        &self,
        incoming: &mut stoppable_listener::Listener<UnixListener>,
    ) -> Result<(), Error> {
        let (tx, rx) = channel::bounded(0);
        let mut threads = Vec::new();
        for accept_result in incoming {
            let (socket, _addr) = match accept_result {
                Err(e) => {
                    debug!("error accepting socket: {}", e);
                    continue;
                }
                Ok(x) => x,
            };
            select! {
                send(tx, socket) => {
                    debug!("reusing idle thread");
                },
                default => {
                    let thread = thread::spawn({
                        let this = self.clone();
                        let rx = rx.clone();
                        move || {
                            for socket in rx {
                                if let Err(e) = this.handle_socket(socket) {
                                    debug!("Error handling socket: {}", e);
                                }
                                debug!("socket closed");
                            }
                        }
                    });
                    threads.push(thread);
                    debug!("starting thread {}", threads.len());
                    tx.send(socket);
                },
            };
        }
        debug!("listener stopped. waiting for current connections to terminate.");
        drop(tx);
        for thread in threads {
            let _ = thread.join();
        }
        Ok(())
    }

    fn handle_socket(&self, mut socket: UnixStream) -> Result<(), Error> {
        use Req::*;
        debug!("new socket from: {:?}", socket.peer_addr());
        loop {
            let req = match read_req(&socket) {
                Ok(ClosedOr::Other(req)) => req,
                Ok(ClosedOr::Closed) => return Ok(()),
                Err(e) => return Err(e),
            };
            match req {
                Config { parallel } => write_resp(&mut socket, self.handle_config(parallel))?,
                Run(run_spec) => write_resp(&mut socket, self.handle_run(run_spec))?,
                Wait => write_resp(&mut socket, self.handle_wait())?,
                Shutdown => write_resp(&mut socket, self.handle_shutdown())?,
                GetPid => write_resp(&mut socket, self.handle_getpid())?,
                Kill => write_resp(&mut socket, self.handle_kill())?,
            }
        }
    }

    fn handle_config(&self, parallel: Option<i32>) -> ConfigResp {
        use self::State::*;
        debug!("handle_config");
        if let Some(parallel) = parallel {
            let mut this = self.lock();
            this.opts.parallel = parallel;
            match this.state {
                Running { ref runner } => runner.set_concurrency_limit(parallel.into()),
                Waiting { ref wait_handle } => wait_handle.set_concurrency_limit(parallel.into()),
            }
        }
    }

    fn handle_run(&self, run_spec: ServerRunSpec) -> RunResp {
        use self::State::*;
        debug!("handle_run");
        let this = self.lock();
        match this.state {
            Running { ref runner } => runner.add(run_spec),
            Waiting { .. } => {
                return Err(Error::new("wait in progress. No new command can be run."))
            }
        };
        Ok(())
    }

    fn handle_wait(&self) -> WaitResp {
        use self::State::*;
        debug!("handle_wait");
        let wait_handle = {
            let mut this = self.lock();
            ::take_mut::take(&mut this.state, |state| match state {
                Running { runner } => Waiting {
                    wait_handle: runner.wait_handle(),
                },
                state => state,
            });
            if let Waiting { ref wait_handle } = this.state {
                wait_handle.clone()
            } else {
                panic!("uh oh");
            }
        };
        debug!("waiting");
        *wait_handle.wait()
    }

    fn handle_shutdown(&self) -> ShutdownResp {
        use self::State::*;
        debug!("handle_shutdown");
        let wait_handle = {
            let mut this = self.lock();
            match this.state {
                Running { .. } => return Err(Error::new("shutdown can only happen after wait")),
                Waiting { ref wait_handle } => wait_handle.clone(),
            }
        };
        wait_handle.wait();
        let this = self.lock();
        this.stop_handle.stop_listening()?;
        Ok(())
    }

    fn handle_getpid(&self) -> GetPidResp {
        debug!("handle_getpid");
        getpid().into()
    }

    fn handle_kill(&self) -> KillResp {
        use libc::pid_t;
        use nix::sys::signal::{kill, Signal};
        use nix::unistd::{getpgid, Pid};

        debug!("handle_kill");
        let pgid: pid_t = getpgid(None).unwrap().into();
        let _ = kill(Pid::from_raw(-pgid), Some(Signal::SIGKILL));
    }
}

impl std::ops::Drop for ServerData {
    fn drop(&mut self) {
        debug!("dropping Server");
        if let Err(e) = std::fs::remove_file(&self.global_opts.socket_path()) {
            debug!("failed to remove socket file: {}", e);
        }
    }
}

#[derive(Debug)]
struct RunSpecWorker;
impl workpool::Worker<ServerRunSpec> for RunSpecWorker {
    type Output = std::io::Result<std::process::ExitStatus>;

    fn run(&self, run_spec: ServerRunSpec) -> std::io::Result<std::process::ExitStatus> {
        use nix::unistd::dup2;
        use std::os::unix::process::CommandExt;
        debug!("starting: {:?}", run_spec);
        let ServerRunSpec {
            args: (exe, args),
            env,
            fds,
        } = run_spec;
        let mut cmd = std::process::Command::new(exe);
        cmd.args(args);
        cmd.envs(env);
        cmd.before_exec(move || {
            for fdlink in &fds {
                if let Err(e) = dup2(fdlink.server_fd, fdlink.subprocess_fd) {
                    return Err(std::io::Error::new(std::io::ErrorKind::Other, e));
                }
            }
            Ok(())
        });
        cmd.status()
    }
}

#[derive(Debug)]
struct RunSpecReducer(bool);
impl Default for RunSpecReducer {
    fn default() -> RunSpecReducer {
        RunSpecReducer(true)
    }
}

impl workpool::Reducer<std::io::Result<std::process::ExitStatus>> for RunSpecReducer {
    type Output = bool;

    fn reduce(&mut self, input: std::io::Result<std::process::ExitStatus>) {
        match input {
            Ok(exit_status) if exit_status.success() => {}
            _ => self.0 = false,
        }
    }

    fn output(self) -> bool {
        self.0
    }
}
