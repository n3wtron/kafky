use crate::{KafkyCmd, KafkyError};
use clap::{App, ArgMatches, SubCommand};
use log::debug;
use std::io::{stdin, stdout, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

impl<'a> KafkyCmd<'a> {
    pub fn get_config_command() -> App<'a, 'a> {
        SubCommand::with_name("config")
            .subcommand(SubCommand::with_name("edit").help("edit the kafky configuration"))
    }

    pub fn config_exec(
        &self,
        app_matches: &ArgMatches<'_>,
        config_file: &Path,
    ) -> Result<(), KafkyError> {
        if let Some(_) = app_matches.subcommand_matches("edit") {
            return Self::open_editor(config_file);
        }
        return Ok(());
    }

    pub fn open_editor(config_file: &Path) -> Result<(), KafkyError> {
        print!("Which editor do you prefer ot open it (vim/nano/..)? ");
        stdout().flush().unwrap();
        let mut answer = String::new();
        stdin().read_line(&mut answer).unwrap();
        let mut open_cmd = answer;
        open_cmd.pop();
        open_cmd.push_str(" ");
        open_cmd.push_str(config_file.as_os_str().to_str().unwrap());

        let sh_path = which::which("sh").expect(&*format!("bash (sh) not found"));

        let mut editor_cmd = Command::new(sh_path);
        let final_editor_cmd = editor_cmd.arg("-c").arg(open_cmd);
        debug!("editor command {:?}", &final_editor_cmd);

        final_editor_cmd
            .spawn()
            .expect("Fail to execute the editor")
            .wait()
            .unwrap();
        Ok(())
    }
}
