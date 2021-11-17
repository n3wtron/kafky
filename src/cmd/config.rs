use crate::KafkyError;
use clap::{App, ArgMatches, SubCommand};
use log::debug;
use std::io::{stdin, stdout, Write};
use std::path::Path;
use std::process::Command;

pub struct ConfigCmd {}

impl ConfigCmd {
    pub(super) fn command<'a>() -> App<'a, 'a> {
        SubCommand::with_name("config")
            .about("kafky configuration")
            .subcommand(SubCommand::with_name("edit").about("edit the kafky configuration"))
    }

    pub(super) fn exec(app_matches: &ArgMatches, config_file: &Path) -> Result<(), KafkyError> {
        if app_matches.subcommand_matches("edit").is_some() {
            return Self::open_editor(config_file);
        }
        Self::command().print_help().expect("error printing help");
        Ok(())
    }

    pub fn open_editor(config_file: &Path) -> Result<(), KafkyError> {
        print!("Which editor do you prefer ot open it (vim/nano/..)? ");
        stdout().flush().unwrap();
        let mut answer = String::new();
        stdin().read_line(&mut answer).unwrap();
        let mut open_cmd = answer;
        open_cmd.pop();
        open_cmd.push(' ');
        open_cmd.push_str(config_file.as_os_str().to_str().unwrap());

        let sh_path = which::which("sh").expect(&*"bash (sh) not found".to_string());

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
