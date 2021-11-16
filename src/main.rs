use crate::cmd::config::ConfigCmd;
use crate::cmd::root::RootCmd;
use crate::config::KafkyConfig;
use client::kafky_client::KafkyClient;
use errors::KafkyError;
use log::debug;
use std::path::Path;

mod client;
mod cmd;
mod config;
mod errors;

fn main() -> Result<(), errors::KafkyError> {
    env_logger::init();

    let mut config_path = home::home_dir().ok_or(KafkyError::HomeFolderNotFound())?;
    debug!("home folder:{:?}", config_path);
    config_path.push(".kafky/config.yml");

    let cfg = load_config_or_create(&config_path)?;

    RootCmd::exec(RootCmd::command(&cfg).get_matches(), &cfg)
}

fn load_config_or_create(config_path: &Path) -> Result<KafkyConfig, KafkyError> {
    match config::KafkyConfig::load(&config_path) {
        Ok(config) => Ok(config),
        Err(e) => match e {
            KafkyError::ConfigurationNotFound(_) => {
                match KafkyConfig::create_configuration_sample(&config_path) {
                    Ok(cfg) => ConfigCmd::open_editor(&config_path).map(|_| cfg),
                    Err(e2) => Err(e2),
                }
            }
            _ => Err(e),
        },
    }
}
