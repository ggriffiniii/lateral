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

use std::ffi::OsString;
use std::os::unix::io::RawFd;
use std::os::unix::net::UnixStream;
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
    author = "",
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
    /// Change the server configuration
    #[structopt(name = "config")]
    Config(cmds::config::Opts),

    /// Start the lateral background server
    #[structopt(name = "start")]
    Start(cmds::start::Opts),

    /// Run the given command in the lateral server
    #[structopt(name = "run")]
    Run(cmds::run::Opts),

    /// Wait for all currently inserted tasks to finish
    #[structopt(name = "wait")]
    Wait(cmds::wait::Opts),

    /// Print pid of server to stdout
    #[structopt(name = "getpid")]
    GetPid,

    /// Kill the server with fire
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
    fds: Vec<T>,
}

type ClientRunSpec = RunSpec<RawFd>;
type ServerRunSpec = RunSpec<FdLink>;

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
type ClientReq = Req<RawFd>;
type ServerReq = Req<FdLink>;

impl ClientReq {
    pub(crate) fn write_to(&self, socket: &UnixStream) -> Result<(), Error> {
        use byteorder::{BigEndian, ByteOrder};
        use nix::sys::socket::{sendmsg, ControlMessage, MsgFlags};
        use nix::sys::uio::IoVec;
        use std::os::unix::io::AsRawFd;
        debug!("Writing request: {:?}", self);

        let json_buf = serde_json::to_vec(self)?;
        let mut length_buf = [0; 4];
        BigEndian::write_u32(&mut length_buf, json_buf.len() as u32);

        let mut cmsg = [ControlMessage::ScmRights(&[]); 1];
        let cmsgs = if let Req::Run(RunSpec { ref fds, .. }) = *self {
            cmsg[0] = ControlMessage::ScmRights(fds);
            &cmsg[0..1]
        } else {
            &cmsg[0..0]
        };
        let n = sendmsg(
            socket.as_raw_fd(),
            &[IoVec::from_slice(&length_buf), IoVec::from_slice(&json_buf)],
            cmsgs,
            MsgFlags::empty(),
            None,
        )?;

        if n != json_buf.len() + length_buf.len() {
            return Err(Error::new("failed to write the entire request"));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) enum SocketClosedOr<T> {
    SocketClosed,
    Data(T),
}

impl ServerReq {
    pub(crate) fn read_from(socket: &UnixStream) -> Result<SocketClosedOr<ServerReq>, Error> {
        let req = Self::real_read_req(socket);
        debug!("Read request: {:?}", req);
        req
    }

    fn real_read_req(socket: &UnixStream) -> Result<SocketClosedOr<ServerReq>, Error> {
        use byteorder::{BigEndian, ByteOrder};
        enum SliceOrVec<'a, T: 'a> {
            Slice(&'a mut [T]),
            Vec(Vec<T>),
        }

        impl<'a, T> std::ops::Deref for SliceOrVec<'a, T> {
            type Target = [T];
            fn deref(&self) -> &Self::Target {
                match self {
                    SliceOrVec::Slice(slice) => slice,
                    SliceOrVec::Vec(vec) => vec,
                }
            }
        }

        impl<'a, T> std::ops::DerefMut for SliceOrVec<'a, T> {
            fn deref_mut(&mut self) -> &mut Self::Target {
                match self {
                    SliceOrVec::Slice(slice) => slice,
                    SliceOrVec::Vec(vec) => vec,
                }
            }
        }

        let mut msg_buf = [0; 4096];
        let mut fds: Vec<RawFd> = Vec::new();
        let res = Self::read_exact(socket, &mut msg_buf[..4], &mut fds)?;
        if let SocketClosedOr::SocketClosed = res {
            return Ok(SocketClosedOr::SocketClosed);
        }
        let msg_size = BigEndian::read_u32(&msg_buf) as usize;
        let mut buf: SliceOrVec<u8> = if msg_size < msg_buf.len() {
            SliceOrVec::Slice(&mut msg_buf[..msg_size])
        } else {
            SliceOrVec::Vec(vec![0; msg_size])
        };
        Self::read_exact(socket, &mut buf, &mut fds)?;
        let req: ClientReq = serde_json::from_slice(&buf)?;
        Ok(SocketClosedOr::Data(req.with_linked_fds(fds)?))
    }

    fn read_exact(
        socket: &UnixStream,
        mut buf: &mut [u8],
        fds: &mut Vec<RawFd>,
    ) -> Result<SocketClosedOr<()>, Error> {
        while !buf.is_empty() {
            match Self::read(socket, buf, fds) {
                Ok(0) => return Ok(SocketClosedOr::SocketClosed),
                Ok(n) => {
                    let tmp = buf;
                    buf = &mut tmp[n..];
                }
                Err(nix::Error::Sys(nix::errno::Errno::EINTR)) => {}
                Err(e) => return Err(Error::new(format!("{}", e))),
            }
        }
        Ok(SocketClosedOr::Data(()))
    }

    fn read(
        socket: &UnixStream,
        buf: &mut [u8],
        fds: &mut Vec<RawFd>,
    ) -> Result<usize, nix::Error> {
        use nix::sys::socket::{recvmsg, CmsgSpace, ControlMessage, MsgFlags};
        use nix::sys::uio::IoVec;
        use std::os::unix::io::AsRawFd;

        let mut cmsg: CmsgSpace<([RawFd; 1024])> = CmsgSpace::new();
        let recv = recvmsg(
            socket.as_raw_fd(),
            &[IoVec::from_mut_slice(buf)],
            Some(&mut cmsg),
            MsgFlags::empty(),
        )?;

        let received_fds = recv
            .cmsgs()
            .filter_map(|x| {
                if let ControlMessage::ScmRights(fds) = x {
                    Some(fds)
                } else {
                    None
                }
            })
            .flat_map(|fds| fds);
        let mut fd_count = fds.len();
        fds.extend(received_fds);
        fd_count = fds.len() - fd_count;
        debug!("read {} bytes and {} fds", recv.bytes, fd_count);
        Ok(recv.bytes)
    }
}

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
                        })
                        .collect(),
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

mod resp {
    use crate::Error;
    use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
    use std::fmt::Debug;
    use std::io;
    /// Response types for each of the valid requests.
    pub(crate) type Config = ();
    pub(crate) type Run = Result<(), Error>;
    pub(crate) type Wait = bool;
    pub(crate) type Kill = ();
    pub(crate) type Shutdown = Result<(), Error>;
    pub(crate) type GetPid = libc::pid_t;

    pub(crate) fn write<W, RESP>(mut w: W, resp: RESP) -> Result<(), io::Error>
    where
        W: io::Write,
        RESP: serde::Serialize + Debug,
    {
        debug!("Writing response: {:?}", &resp);
        let buf = serde_json::to_vec(&resp)?;
        w.write_u32::<BigEndian>(buf.len() as u32)?;
        w.write_all(&buf)?;
        Ok(())
    }

    pub(crate) fn read_from<R, RESP>(r: R) -> Result<RESP, io::Error>
    where
        R: io::Read,
        RESP: serde::de::DeserializeOwned + Debug,
    {
        let resp = real_read_resp(r);
        debug!("Read response: {:?}", resp);
        resp
    }

    fn real_read_resp<R, RESP>(mut r: R) -> Result<RESP, io::Error>
    where
        R: io::Read,
        RESP: serde::de::DeserializeOwned + Debug,
    {
        let len = r.read_u32::<BigEndian>()?;
        Ok(serde_json::from_reader(r.take(u64::from(len)))?)
    }
}

pub fn execute(opts: &Opts) -> Result<(), Error> {
    use crate::cmds::{config, getpid, kill, run, start, wait};
    use crate::CommandOpts::{Config, GetPid, Kill, Run, Start, Wait};
    match opts.cmd {
        Config(ref config_opts) => config::execute(&opts.global_opts, config_opts),
        Start(ref start_opts) => start::execute(&opts.global_opts, start_opts),
        Run(ref run_opts) => run::execute(&opts.global_opts, run_opts),
        Wait(ref wait_opts) => wait::execute(&opts.global_opts, wait_opts),
        GetPid => getpid::execute(&opts.global_opts),
        Kill => kill::execute(&opts.global_opts),
    }
}
