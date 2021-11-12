use std::io::{stdin, stdout, Write};
use std::process::Command;
use std::string::String;
use std::sync::Arc;

use clap::ArgMatches;
use log::debug;

use client::kafky_client::KafkyClient;
use errors::KafkyError;

use crate::cmd::command::KafkyCmd;
use crate::config::KafkyConfig;

mod cmd;
mod errors;
mod config;
mod client;

#[tokio::main]
async fn main() -> Result<(), errors::KafkyError> {
    env_logger::init();

    let mut home_folder = home::home_dir().map(|home| home.into_os_string()).ok_or(KafkyError::HomeFolderNotFound())?;
    home_folder.push("/.kafky.yml");
    let cfg = match config::KafkyConfig::load(home_folder.to_str().unwrap()) {
        Ok(config) => { Ok(config) }
        Err(e) => {
            match e {
                KafkyError::ConfigurationNotFound(_) => create_configuration_sample(home_folder.to_str().unwrap()),
                _ => { Err(e) }
            }
        }
    }?;

    let command = KafkyCmd::new()?;
    let app = command.create_command();
    let app_matches = app.get_matches();

    let environment = String::from(app_matches.value_of("environment").unwrap());
    let credential = get_credential(&app_matches, &cfg, &environment)?;
    let kafky_client = Arc::new(KafkyClient::new(cfg, environment, credential));

    match app_matches.subcommand() {
        ("show", Some(matches)) => { command.show_exec(matches, kafky_client.clone()) }
        ("produce", Some(matches)) => { command.produce_exec(matches, kafky_client.clone()) }
        ("consume", Some(matches)) => { command.consume_exec(matches, kafky_client.clone()) }
        (_, _) => Err(KafkyError::InvalidCommand())
    }
}

fn get_credential(app_matches: &ArgMatches, cfg: &KafkyConfig, environment: &String) -> Result<String, KafkyError> {
    app_matches.value_of("credential")
        .map(|cred| cred.to_string())
        .or(cfg.get_environment(environment)
            .and_then(|e| {
                if e.credentials.len() == 1 {
                    let first_credential = e.credentials.first().map(|c| c.name.clone()).unwrap();
                    return Some(first_credential);
                }
                return None;
            })
        ).ok_or(KafkyError::NoCredentialSpecified())
}

fn create_configuration_sample(config_file: &str) -> Result<KafkyConfig, KafkyError> {
    print!("Configuration file {} not found, do you want to create a sample one? [y/N]: ", config_file);
    stdout().flush().unwrap();
    let mut answer = String::new();
    stdin().read_line(&mut answer).unwrap();
    if answer.trim().eq_ignore_ascii_case("Y") {
        println!("creating sample..");
        config::create_sample(config_file)?;

        print!("Which editor do you prefer ot open it (vim/nano/..)? ");
        stdout().flush().unwrap();
        answer.clear();
        stdin().read_line(&mut answer).unwrap();
        let mut open_cmd = answer;
        open_cmd.pop();
        open_cmd.push_str(" ");
        open_cmd.push_str(config_file);

        let sh_path = which::which("sh").expect(&*format!("bash (sh) not found"));

        let mut editor_cmd = Command::new(sh_path);
        let final_editor_cmd = editor_cmd
            .arg("-c")
            .arg(open_cmd);

        debug!("editor command {:?}",&final_editor_cmd);

        final_editor_cmd.spawn()
            .expect("Fail to execute the editor")
            .wait().unwrap();

        config::KafkyConfig::load(config_file)
    } else {
        Err(KafkyError::ConfigurationNotFound(config_file.to_string()))
    }
}
