use std::os::unix::net::UnixStream;
use {resp, Error, GlobalOpts, Req};

/// Options for the config subcommand.
#[derive(StructOpt, Debug)]
pub struct Opts {
    #[structopt(long = "parallel", short = "p")]
    parallel: Option<i32>,
}

pub fn execute(global_opts: &GlobalOpts, opts: &Opts) -> Result<(), Error> {
    debug!("config command");
    let socket = UnixStream::connect(&global_opts.socket_path())?;
    Req::Config {
        parallel: opts.parallel,
    }.write_to(&socket)?;
    resp::read_from::<_, resp::Config>(&socket)?;
    Ok(())
}
