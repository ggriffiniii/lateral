use std::os::unix::net::UnixStream;
use {net::write_req, Error, GlobalOpts, Req};

pub fn execute(global_opts: &GlobalOpts) -> Result<(), Error> {
    debug!("kill command");
    let mut socket = UnixStream::connect(&global_opts.socket_path())?;
    write_req(&mut socket, &Req::Kill)?;
    Ok(())
}
