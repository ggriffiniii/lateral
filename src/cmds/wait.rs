use std::os::unix::net::UnixStream;
use {
    net::{read_resp, write_req},
    Error, GlobalOpts, Req, ShutdownResp, WaitResp,
};

/// Options for the wait subcommand.
#[derive(StructOpt, Debug)]
pub struct Opts {
    #[structopt(short = "n")]
    no_shutdown: bool,
}

pub fn execute(global_opts: &GlobalOpts, opts: &Opts) -> Result<(), Error> {
    debug!("wait command");
    let mut socket = UnixStream::connect(&global_opts.socket_path())?;
    let res = wait(&mut socket);

    if !opts.no_shutdown {
        if let Err(e) = shutdown(&mut socket) {
            debug!("failed to shutdown the server: {}", e)
        }
    }
    res
}

fn wait(socket: &mut UnixStream) -> Result<(), Error> {
    write_req(socket, &Req::Wait)?;
    let resp: WaitResp = read_resp(socket)?;
    if resp {
        Ok(())
    } else {
        Err(Error {
            exit_code: -1,
            msg: "".into(),
            show_msg: false,
        })
    }
}

fn shutdown(socket: &mut UnixStream) -> Result<(), Error> {
    write_req(socket, &Req::Shutdown)?;
    read_resp::<_, ShutdownResp>(socket)?
}
