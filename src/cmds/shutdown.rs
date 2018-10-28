use std::os::unix::net::UnixStream;
use {
    net::{read_resp, write_req},
    Error, GlobalOpts, Req, ShutdownResp,
};

pub fn execute(global_opts: &GlobalOpts) -> Result<(), Error> {
    debug!("shutdown command");
    let mut socket = UnixStream::connect(&global_opts.socket_path())?;
    write_req(&mut socket, &Req::Shutdown)?;
    read_resp::<_, ShutdownResp>(&socket)?
}
