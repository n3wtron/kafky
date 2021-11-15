use std::fs::create_dir;
use std::io::{stdin, stdout, Write};
use std::path::PathBuf;

use std::string::String;



use log::debug;

use client::kafky_client::KafkyClient;
use errors::KafkyError;

use crate::cmd::command::KafkyCmd;
use crate::config::KafkyConfig;

mod client;
mod cmd;
mod config;
mod errors;

fn main() -> Result<(), errors::KafkyError> {
    env_logger::init();

    let mut config_path = home::home_dir().ok_or(KafkyError::HomeFolderNotFound())?;
    debug!("home folder:{:?}", config_path);
    config_path.push(".kafky/config.yml");

    let cfg = match config::KafkyConfig::load(&config_path) {
        Ok(config) => Ok(config),
        Err(e) => match e {
            KafkyError::ConfigurationNotFound(_) => match create_configuration_sample(&config_path)
            {
                Ok(cfg) => KafkyCmd::open_editor(&config_path).map(|_| cfg),
                Err(e2) => Err(e2),
            },
            _ => Err(e),
        },
    }?;

    let cmd = KafkyCmd::new(&cfg)?;
    cmd.exec().map_err(|e| {
        cmd.print_help();
        e
    })
}

fn create_configuration_sample(config_file: &PathBuf) -> Result<KafkyConfig, KafkyError> {
    print!(
        "Configuration file {} not found, do you want to create a sample one? [y/N]: ",
        config_file.display()
    );
    let kafky_directory = config_file.parent().unwrap();
    if !kafky_directory.exists() {
        create_dir(kafky_directory)?;
    }
    stdout().flush().unwrap();
    let mut answer = String::new();
    stdin().read_line(&mut answer).unwrap();
    if answer.trim().eq_ignore_ascii_case("Y") {
        println!("creating sample..");
        config::create_sample(config_file)?;
        let cfg = config::KafkyConfig::load(config_file)?;
        return Ok(cfg);
    } else {
        Err(KafkyError::ConfigurationNotFound(
            config_file.as_os_str().to_str().unwrap().to_string(),
        ))
    }
}
