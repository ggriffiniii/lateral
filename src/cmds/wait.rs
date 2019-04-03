use crate::{resp, Error, GlobalOpts, Req};
use std::os::unix::net::UnixStream;

/// Options for the wait subcommand.
#[derive(StructOpt, Debug)]
pub struct Opts {
    #[structopt(short = "n")]
    no_shutdown: bool,
}

pub fn execute(global_opts: &GlobalOpts, opts: &Opts) -> Result<(), Error> {
    debug!("wait command");
    let socket = UnixStream::connect(&global_opts.socket_path())?;
    let res = wait(&socket);

    if !opts.no_shutdown {
        if let Err(e) = shutdown(&socket) {
            debug!("failed to shutdown the server: {}", e)
        }
    }
    res
}

fn wait(socket: &UnixStream) -> Result<(), Error> {
    Req::Wait.write_to(socket)?;
    let resp: resp::Wait = resp::read_from(socket)?;
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

fn shutdown(socket: &UnixStream) -> Result<(), Error> {
    Req::Shutdown.write_to(socket)?;
    resp::read_from::<_, resp::Shutdown>(socket)?
}
