use crate::{Error, GlobalOpts, Req};
use std::os::unix::net::UnixStream;

pub fn execute(global_opts: &GlobalOpts) -> Result<(), Error> {
    debug!("kill command");
    let socket = UnixStream::connect(&global_opts.socket_path())?;
    Req::Kill.write_to(&socket)?;
    Ok(())
}
