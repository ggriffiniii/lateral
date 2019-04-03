use std::os::unix::net::UnixStream;
use crate::{resp, Error, GlobalOpts, Req};

pub fn execute(global_opts: &GlobalOpts) -> Result<(), Error> {
    debug!("getpid command");
    let socket = UnixStream::connect(&global_opts.socket_path())?;
    Req::GetPid.write_to(&socket)?;
    let _resp: resp::GetPid = resp::read_from(&socket)?;
    Ok(())
}
