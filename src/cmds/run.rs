use std::env;
use std::ffi::OsString;
use std::os::unix::{io::RawFd, net::UnixStream};
use {resp, ClientRunSpec, Error, GlobalOpts, Req};

/// Options for the run subcommand.
#[derive(StructOpt, Debug)]
pub struct Opts {
    #[structopt(parse(from_os_str))]
    args: Vec<OsString>,
}

pub fn execute(global_opts: &GlobalOpts, opts: &Opts) -> Result<(), Error> {
    debug!("run command");
    let fds = get_open_fds();
    let (exe, args) = if let Some((exe, args)) = opts.args.split_first() {
        (exe.clone(), args.to_vec())
    } else {
        return Err(Error::new("no arguments provided"));
    };
    let socket = UnixStream::connect(&global_opts.socket_path())?;
    Req::Run(ClientRunSpec {
        args: (exe, args),
        env: env::vars_os().collect(),
        fds,
    }).write_to(&socket)?;
    let resp: resp::Run = resp::read_from(&socket)?;
    resp
}

fn get_open_fds() -> Vec<RawFd> {
    use nix::fcntl::{fcntl, FcntlArg};
    use nix::unistd::{sysconf, SysconfVar};
    let max_fd = match sysconf(SysconfVar::OPEN_MAX) {
        Ok(Some(max_fd)) => max_fd,
        _ => 32768,
    };
    (0..max_fd)
        .map(|i| i as RawFd)
        .filter(|fd| fcntl(*fd, FcntlArg::F_GETFD).is_ok())
        .collect()
}
