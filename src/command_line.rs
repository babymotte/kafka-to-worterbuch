use std::env;

use clap::Parser;
use env_logger::Env;

#[derive(Parser)]
#[clap(version, about, long_about = None)]
pub struct Options {
    /// Increase verbosity, and can be used multiple times
    #[arg(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Use TLS when connecting to worterbuch server. By default TLS is off
    #[arg(short = 's', long)]
    pub tls: Option<bool>,

    /// Worterbuch server address (IP or hostname). Default is 'localhost'
    #[arg(short, long)]
    pub address: Option<String>,

    /// Worterbuch server port. Default is '8080'
    #[arg(short, long)]
    pub port: Option<u16>,

    /// Name of the application this instance synchronizes to worterbuch
    #[arg(index = 1)]
    pub application: String,
}

pub fn parse() -> Options {
    let mut opts = Options::parse();

    let debug_level = match opts.verbose {
        0 => "info",
        1 => "debug",
        _ => "trace",
    };
    env_logger::Builder::from_env(Env::default().default_filter_or(debug_level)).init();

    if opts.tls.is_none() {
        opts.tls = env::var("WORTERBUCH_PROTO")
            .ok()
            .map(|it| &it == "wss")
            .or_else(|| Some(false));
    }

    if opts.address.is_none() {
        opts.address = env::var("WORTERBUCH_HOST_ADDRESS")
            .ok()
            .or_else(|| Some("localhost".to_owned()));
    }

    if opts.port.is_none() {
        opts.port = env::var("WORTERBUCH_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .or_else(|| Some(8080));
    }

    opts
}
