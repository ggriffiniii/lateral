use crossbeam_channel as channel;
use crossbeam_utils::thread;
use std;
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::{Arc, Mutex, MutexGuard};
use workpool;
use crate::{resp, Error, GlobalOpts, ServerReq, ServerRunSpec, SocketClosedOr};

// Options for the start subcommand.
#[derive(StructOpt, Debug, Clone)]
pub struct Opts {
    #[structopt(long = "parallel", short = "p", default_value = "10")]
    parallel: i32,
    #[structopt(long = "foreground", short = "f")]
    foreground: bool,
}

pub fn execute(global_opts: &GlobalOpts, opts: &Opts) -> Result<(), Error> {
    use nix::unistd::{fork, setpgid, ForkResult, Pid};
    debug!("start command");
    if let Some(parent) = global_opts.socket_path().parent() {
        std::fs::create_dir_all(parent)?;
    }
    setpgid(Pid::from_raw(0), Pid::from_raw(0))?;
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
struct Server<'a>(Arc<Mutex<ServerData<'a>>>);

#[derive(Debug)]
struct ServerData<'a> {
    global_opts: &'a GlobalOpts,
    opts: Opts,
    stop_handle: stoppable_listener::StopHandle,
    state: State,
}

type WorkPool = workpool::dynamic_pool::DynamicPool<ServerRunSpec, RunSpecWorker, RunSpecReducer>;
type WaitHandle = workpool::dynamic_pool::WaitHandle<bool>;

#[derive(Debug)]
enum State {
    Running { runner: WorkPool },
    Waiting { wait_handle: WaitHandle },
}

impl<'a> Server<'a> {
    fn lock(&self) -> MutexGuard<ServerData<'a>> {
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
            global_opts,
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
        let mut thread_count = 0;
        let _ = thread::scope(|scope| {
            let (tx, rx) = channel::bounded(0);
            for accept_result in incoming {
                let socket = match accept_result {
                    Ok((socket, _addr)) => socket,
                    Err(err) => {
                        debug!("accept() failed: {}", err);
                        return;
                    }
                };
                select! {
                    send(tx, socket) => {
                        debug!("reusing idle thread");
                    },
                    default => {
                        debug!("starting thread {}", thread_count);
                        thread_count += 1;
                        scope.spawn({
                            let rx = rx.clone();
                            move |_| {
                                for socket in rx {
                                    if let Err(e) = self.handle_socket(socket) {
                                        debug!("Error handling socket: {}", e);
                                    }
                                    debug!("socket closed");
                                }
                            }
                        });
                        tx.send(socket);
                    }
                }
            }
            drop(tx);
        });
        Ok(())
    }

    fn handle_socket(&self, mut socket: UnixStream) -> Result<(), Error> {
        use crate::Req::*;
        debug!("new socket from: {:?}", socket.peer_addr());
        loop {
            let req = match ServerReq::read_from(&socket) {
                Ok(SocketClosedOr::Data(req)) => req,
                Ok(SocketClosedOr::SocketClosed) => return Ok(()),
                Err(e) => return Err(e),
            };
            match req {
                Config { parallel } => resp::write(&mut socket, self.handle_config(parallel))?,
                Run(run_spec) => resp::write(&mut socket, self.handle_run(run_spec))?,
                Wait => resp::write(&mut socket, self.handle_wait())?,
                Shutdown => resp::write(&mut socket, self.handle_shutdown())?,
                GetPid => resp::write(&mut socket, self.handle_getpid())?,
                Kill => resp::write(&mut socket, self.handle_kill())?,
            }
        }
    }

    fn handle_config(&self, parallel: Option<i32>) -> resp::Config {
        debug!("handle_config");
        if let Some(parallel) = parallel {
            self.lock().set_parallel(parallel);
        }
    }

    fn handle_run(&self, run_spec: ServerRunSpec) -> resp::Run {
        debug!("handle_run");
        self.lock().handle_run(run_spec)
    }

    fn handle_wait(&self) -> resp::Wait {
        debug!("handle_wait");
        let wait_handle = self.lock().handle_wait();
        debug!("waiting");
        *wait_handle.wait()
    }

    fn handle_shutdown(&self) -> resp::Shutdown {
        debug!("handle_shutdown");
        let wait_handle = self
            .lock()
            .get_wait_handle()
            .ok_or_else(|| Error::new("shutdown can only happen after wait"))?;
        wait_handle.wait();
        self.lock().stop_handle.stop_listening()?;
        Ok(())
    }

    fn handle_getpid(&self) -> resp::GetPid {
        debug!("handle_getpid");
        nix::unistd::getpid().into()
    }

    fn handle_kill(&self) -> resp::Kill {
        use libc::pid_t;
        use nix::sys::signal::{kill, Signal};
        use nix::unistd::{getpgid, Pid};

        debug!("handle_kill");
        let pgid: pid_t = getpgid(None).unwrap().into();
        let _ = kill(Pid::from_raw(-pgid), Some(Signal::SIGKILL));
    }
}

impl<'a> ServerData<'a> {
    fn set_parallel(&mut self, parallel: i32) {
        self.opts.parallel = parallel;
        match self.state {
            State::Running { ref runner } => runner.set_concurrency_limit(parallel.into()),
            State::Waiting { ref wait_handle } => {
                wait_handle.set_concurrency_limit(parallel.into())
            }
        }
    }

    fn handle_run(&mut self, run_spec: ServerRunSpec) -> resp::Run {
        debug!("handle_run");
        match self.state {
            State::Running { ref runner } => {
                runner.add(run_spec);
                Ok(())
            }
            State::Waiting { .. } => {
                Err(Error::new("wait in progress. No new command can be run."))
            }
        }
    }

    fn handle_wait(&mut self) -> WaitHandle {
        ::take_mut::take(&mut self.state, |state| match state {
            State::Running { runner } => State::Waiting {
                wait_handle: runner.wait_handle(),
            },
            state => state,
        });
        self.get_wait_handle()
            .expect("expected to be in wait state")
    }

    fn get_wait_handle(&self) -> Option<WaitHandle> {
        if let State::Waiting { wait_handle } = &self.state {
            Some(wait_handle.clone())
        } else {
            None
        }
    }
}

impl<'a> std::ops::Drop for ServerData<'a> {
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
