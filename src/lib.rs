extern crate byteorder;
#[macro_use]
extern crate crossbeam_channel;
extern crate libc;
#[macro_use]
extern crate log;
extern crate mio;
extern crate nix;
extern crate pretty_env_logger;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate structopt;
#[macro_use]
extern crate structopt_derive;
extern crate dirs;
extern crate stoppable_listener;
extern crate take_mut;
extern crate workpool;

mod cmds;
mod net;

use std::ffi::OsString;
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use structopt::StructOpt;

#[derive(Debug, Serialize, Deserialize)]
pub struct Error {
    pub exit_code: i32,
    pub msg: String,
    pub show_msg: bool,
}

impl Error {
    fn new<S>(msg: S) -> Error
    where
        S: Into<String>,
    {
        Error {
            exit_code: -1,
            msg: msg.into(),
            show_msg: true,
        }
    }
}

impl<E> From<E> for Error
where
    E: std::error::Error,
{
    fn from(e: E) -> Self {
        Error {
            exit_code: -1,
            msg: format!("{}", e),
            show_msg: true,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.msg)
    }
}

#[derive(StructOpt, Debug)]
#[structopt(
    name = "lateral",
    about = "A simple process parallelizer to make lives better at the commandline.",
    raw(setting = "structopt::clap::AppSettings::TrailingVarArg")
)]
pub struct Opts {
    #[structopt(flatten)]
    global_opts: GlobalOpts,

    #[structopt(subcommand)]
    cmd: CommandOpts,
}

/// Options that apply to all commands.
#[derive(StructOpt, Debug, Clone)]
pub struct GlobalOpts {
    #[structopt(
        long = "socket",
        short = "s",
        help = "UNIX domain socket path (default $HOME/.lateral/socket.$SESSIONID)",
        parse(from_os_str)
    )]
    socket_arg: Option<PathBuf>,
}

impl GlobalOpts {
    fn socket_path(&self) -> PathBuf {
        self.socket_arg.clone().unwrap_or_else(|| {
            let homedir = dirs::home_dir().unwrap_or_else(|| "".into());
            // getsid with None always succeeds, so unwrap here is fine.
            let sid = nix::unistd::getsid(None).unwrap();
            let filename = Path::new("socket").with_extension(sid.to_string());
            [homedir.as_ref(), Path::new(".lateral"), filename.as_ref()]
                .iter()
                .collect()
        })
    }
}

#[derive(StructOpt, Debug)]
enum CommandOpts {
    #[structopt(name = "config")]
    Config(cmds::config::Opts),
    #[structopt(name = "start")]
    Start(cmds::start::Opts),
    #[structopt(name = "run")]
    Run(cmds::run::Opts),
    #[structopt(name = "wait")]
    Wait(cmds::wait::Opts),
    #[structopt(name = "shutdown")]
    Shutdown,
    #[structopt(name = "getpid")]
    GetPid,
    #[structopt(name = "kill")]
    Kill,
}

impl Opts {
    pub fn from_args() -> Self {
        <Opts as StructOpt>::from_args()
    }
}

/// The lateral server needs to keep a mapping between the file descriptor number
/// that exists in the server process and the file descriptor it's going to
/// represent in the subprocess. For example when a command is launched via
/// `lateral run -- echo foo > /tmp/bar`. The client process will send it's fd1
/// (/tmp/bar) to the server. When the server receives the file descriptor via a
/// unix domain socket it will be mapped to a different fd number within the
/// server process (say fd 42). This structure is used within the server to say
/// server_fd: 42 maps to subprocess_fd: 1 for the subprocess associated with
/// this lateral run invocation.
#[derive(Debug, Deserialize)]
pub(crate) struct FdLink {
    server_fd: RawFd,
    subprocess_fd: RawFd,
}

impl Drop for FdLink {
    fn drop(&mut self) {
        let _ = nix::unistd::close(self.server_fd);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RunSpec<T> {
    args: (OsString, Vec<OsString>),
    env: Vec<(OsString, OsString)>,
    fds: T,
}

type ClientRunSpec = RunSpec<Vec<RawFd>>;
type ServerRunSpec = RunSpec<Vec<FdLink>>;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Req<T> {
    Config { parallel: Option<i32> },
    Run(RunSpec<T>),
    Wait,
    Shutdown,
    GetPid,
    Kill,
}

/// ClientReq is the type of Req that the clients send to the server. ServerReq
/// is the type of Req that the server handles after parsing. These only differ
/// in the file descriptor information stored in Run requests. When the client
/// issues a run request it includes in the json request the list of file
/// descriptors it's sending over [0, 1, 2, etc.]. It then passes those file
/// descriptors over a unix domain socket. When the server parses the request it
/// joins the client provided list with the file descriptors received (which have
/// different fd numbers that are valid within the server process) and links them
/// together.
type ClientReq = Req<Vec<RawFd>>;
type ServerReq = Req<Vec<FdLink>>;

impl ClientReq {
    fn with_linked_fds(self, server_fds: Vec<RawFd>) -> Result<ServerReq, Error> {
        Ok(match self {
            Req::Config { parallel } => Req::Config { parallel },
            Req::Run(RunSpec { fds, env, args }) => {
                if server_fds.len() != fds.len() {
                    return Err(Error::new(format!(
                        "mismatched number of requested fds and server fds; ({} vs {})",
                        server_fds.len(),
                        fds.len()
                    )));
                }
                Req::Run(RunSpec {
                    args,
                    env,
                    fds: fds
                        .into_iter()
                        .zip(server_fds)
                        .map(|(subprocess_fd, server_fd)| FdLink {
                            subprocess_fd,
                            server_fd,
                        }).collect(),
                })
            }
            Req::Wait => Req::Wait,
            Req::Shutdown => Req::Shutdown,
            Req::GetPid => Req::GetPid,
            Req::Kill => Req::Kill,
        })
    }
}

impl<'a> From<&'a ServerReq> for ClientReq {
    fn from(req: &'a ServerReq) -> ClientReq {
        match req {
            Req::Config { parallel } => Req::Config {
                parallel: *parallel,
            },
            Req::Run(run_spec) => Req::Run(RunSpec {
                args: run_spec.args.clone(),
                env: run_spec.env.clone(),
                fds: run_spec
                    .fds
                    .iter()
                    .map(|&FdLink { subprocess_fd, .. }| subprocess_fd)
                    .collect(),
            }),
            Req::Wait => Req::Wait,
            Req::Shutdown => Req::Shutdown,
            Req::GetPid => Req::GetPid,
            Req::Kill => Req::Kill,
        }
    }
}

/// Response types for each of the valid requests.
type ConfigResp = ();
type RunResp = Result<(), Error>;
type WaitResp = bool;
type KillResp = ();
type ShutdownResp = Result<(), Error>;
type GetPidResp = libc::pid_t;

pub fn execute(opts: &Opts) -> Result<(), Error> {
    use cmds::{config, getpid, kill, run, shutdown, start, wait};
    use CommandOpts::{Config, GetPid, Kill, Run, Shutdown, Start, Wait};
    match opts.cmd {
        Config(ref config_opts) => config::execute(&opts.global_opts, config_opts),
        Start(ref start_opts) => start::execute(&opts.global_opts, start_opts),
        Run(ref run_opts) => run::execute(&opts.global_opts, run_opts),
        Wait(ref wait_opts) => wait::execute(&opts.global_opts, wait_opts),
        Shutdown => shutdown::execute(&opts.global_opts),
        GetPid => getpid::execute(&opts.global_opts),
        Kill => kill::execute(&opts.global_opts),
    }
}
