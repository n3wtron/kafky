use std::fs;
use std::fs::create_dir;

use std::io::{stdin, stdout, Write};
use std::path::Path;

use config::{Config, File};
use log::debug;
use serde::{Deserialize, Serialize};

use crate::errors::KafkyError;

#[derive(Debug, Deserialize, Serialize)]
pub struct KafkyPrivateKey {
    #[serde(flatten)]
    pub key: KafkyPEM,
    pub password: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum KafkyPEM {
    Path(String),
    Base64(String),
    Pem(String),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct KafkySSLCredential {
    pub truststore: KafkyPEM,
    pub certificate: KafkyPEM,
    #[serde(rename = "privateKey")]
    pub private_key: KafkyPrivateKey,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct KafkyPlainCredential {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum KafkyCredentialKind {
    Ssl(KafkySSLCredential),
    Plain(KafkyPlainCredential),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct KafkyCredential {
    pub name: String,
    #[serde(flatten)]
    pub credential: KafkyCredentialKind,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct KafkyEnvironment {
    pub name: String,
    pub brokers: Vec<String>,
    pub credentials: Vec<KafkyCredential>,
}

impl KafkyEnvironment {
    pub fn get_credential(&self, credential: &str) -> Option<&KafkyCredential> {
        self.credentials.iter().find(|c| c.name.eq(credential))
    }

    pub fn get_credential_names(&self) -> Vec<String> {
        self.credentials.iter().map(|c| c.name.clone()).collect()
    }
}

fn empty_path<'a>() -> &'a Path {
    Path::new("")
}

#[derive(Debug, Deserialize, Serialize)]
pub struct KafkyConfig<'a> {
    #[serde(skip)]
    #[serde(default = "empty_path")]
    path: &'a Path,
    pub environments: Vec<KafkyEnvironment>,
}

impl<'a> KafkyConfig<'a> {
    pub fn load(config_file: &'a Path) -> Result<Self, KafkyError> {
        debug!("checking configuration file presence {:?}", config_file);
        if !config_file.exists() {
            return Err(KafkyError::ConfigurationNotFound(
                config_file.as_os_str().to_str().unwrap().to_string(),
            ));
        }
        let mut cfg = Config::default();
        debug!("loading configuration {:?}", &config_file);
        cfg.merge(File::from(config_file))?;

        cfg.try_into::<KafkyConfig>()
            .map(|mut kafky_cfg| {
                kafky_cfg.path = config_file;
                kafky_cfg
            })
            .map_err(|e| e.into())
    }

    pub fn get_environment(&self, environment: &str) -> Option<&KafkyEnvironment> {
        self.environments.iter().find(|e| e.name.eq(environment))
    }

    pub fn get_environment_names(&self) -> Vec<&str> {
        self.environments.iter().map(|e| e.name.as_str()).collect()
    }

    pub fn path(&self) -> &'a Path {
        self.path
    }

    pub fn config_folder(&self) -> &'a Path {
        self.path
            .parent()
            .expect("error getting parent config path")
    }

    fn create_sample(config_file_path: &Path) -> Result<(), KafkyError> {
        let env = KafkyEnvironment {
            name: "sample-env".to_string(),
            brokers: vec!["localhost:9094".to_string()],
            credentials: vec![
                KafkyCredential {
                    name: "plain-cred".to_string(),
                    credential: KafkyCredentialKind::Plain(KafkyPlainCredential {
                        username: "kafka-user".to_string(),
                        password: "kafka-password".to_string(),
                    }),
                },
                KafkyCredential {
                    name: "ssl-cred".to_string(),
                    credential: KafkyCredentialKind::Ssl(KafkySSLCredential {
                        truststore: KafkyPEM::Pem(
                            "-----BEGIN CERTIFICATE-----\
                    DQEJARYAMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCJ9WRanG/fUvcfKiGl
                    DQEJARYAMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCJ9WRanG/fUvcfKiGl
                    DQEJARYAMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCJ9WRanG/fUvcfKiGl
                    DQEJARYAMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCJ9WRanG/fUvcfKiGl
                    DQEJARYAMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCJ9WRanG/fUvcfKiGl
                    -----END CERTIFICATE-----\
                    "
                            .to_string(),
                        ),
                        certificate: KafkyPEM::Path("/my.cert.pem".to_string()),
                        private_key: KafkyPrivateKey {
                            key: KafkyPEM::Base64("bXkgcHJpdmF0ZSBrZXk=".to_string()),
                            password: Some("my-cert-password".to_string()),
                        },
                    }),
                },
            ],
        };
        let config = KafkyConfig {
            path: config_file_path,
            environments: vec![env],
        };
        let mut config_file = fs::File::create(config_file_path)
            .map_err(|e| KafkyError::CannotCreateSampleConfig(format!("{}", e)))?;
        let yaml = serde_yaml::to_string(&config).unwrap();
        config_file
            .write(yaml.as_ref())
            .map_err(|e| KafkyError::CannotCreateSampleConfig(format!("{}", e)))?;
        Ok(())
    }

    pub fn create_configuration_sample(config_file: &'a Path) -> Result<KafkyConfig, KafkyError> {
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
            Self::create_sample(config_file)?;
            let cfg = Self::load(config_file)?;
            Ok(cfg)
        } else {
            Err(KafkyError::ConfigurationNotFound(
                config_file.as_os_str().to_str().unwrap().to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indoc::indoc;
    #[test]
    fn parse_test() -> Result<(), KafkyError> {
        let mut tmp_cfg = tempfile::Builder::new().suffix(".yml").tempfile().unwrap();
        let yml_cfg = indoc! {"
environments:
  - name: test
    brokers:
      - localhost:9094
    credentials:
      - name: cred
        ssl:
          truststore:
            path: ./caroot.cer
          certificate:
            path: ./producer.cer
          privateKey:
            path: /producer.pkcs8
            password: priv-key-password
        "};
        write!(tmp_cfg, "{}", yml_cfg).expect("error writing yml");
        let cfg = KafkyConfig::load(tmp_cfg.path())?;
        assert_eq!(cfg.environments.len(), 1);
        Ok(())
    }
}
