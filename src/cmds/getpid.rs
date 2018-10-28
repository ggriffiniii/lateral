use std::os::unix::net::UnixStream;
use {
    net::{read_resp, write_req},
    Error, GetPidResp, GlobalOpts, Req,
};

pub fn execute(global_opts: &GlobalOpts) -> Result<(), Error> {
    debug!("getpid command");
    let mut socket = UnixStream::connect(&global_opts.socket_path())?;
    write_req(&mut socket, &Req::GetPid)?;
    let resp: GetPidResp = read_resp(&socket)?;
    println!("{}", resp);
    Ok(())
}
