extern crate lateral;
#[macro_use]
extern crate log;
extern crate pretty_env_logger;

fn main() {
    pretty_env_logger::init();
    let opts = lateral::Opts::from_args();
    debug!("{:#?}", opts);
    if let Err(e) = lateral::execute(&opts) {
        if e.show_msg {
            eprintln!("Error: {}", e);
        }
        std::process::exit(e.exit_code);
    }
}
