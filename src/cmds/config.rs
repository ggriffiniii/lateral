use std::os::unix::net::UnixStream;
use {
    net::{read_resp, write_req},
    ConfigResp, Error, GlobalOpts, Req,
};

/// Options for the config subcommand.
#[derive(StructOpt, Debug)]
pub struct Opts {
    #[structopt(long = "parallel", short = "p")]
    parallel: Option<i32>,
}

pub fn execute(global_opts: &GlobalOpts, opts: &Opts) -> Result<(), Error> {
    debug!("config command");
    let mut socket = UnixStream::connect(&global_opts.socket_path())?;
    write_req(
        &mut socket,
        &Req::Config {
            parallel: opts.parallel,
        },
    )?;
    read_resp::<_, ConfigResp>(&socket)?;
    Ok(())
}
