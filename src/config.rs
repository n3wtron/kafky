use std::fs;

use std::io::Write;
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
    PATH(String),
    BASE64(String),
    PEM(String),
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
    SSL(KafkySSLCredential),
    PLAIN(KafkyPlainCredential),
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
    pub fn get_credential(&self, credential: &String) -> Option<&KafkyCredential> {
        self.credentials.iter().find(|c| c.name.eq(credential))
    }

    pub fn get_credential_names(&self) -> Vec<String> {
        self.credentials.iter().map(|c| c.name.clone()).collect()
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct KafkyConfig {
    pub environments: Vec<KafkyEnvironment>,
}

impl KafkyConfig {
    pub fn load(config_file: &str) -> Result<Self, KafkyError> {
        if !Path::new(config_file).exists() {
            return Err(KafkyError::ConfigurationNotFound(config_file.to_string()));
        }
        let mut cfg = Config::default();
        debug!("loading configuration {}", &config_file);
        cfg.merge(File::with_name(config_file))?;

        cfg.try_into().map_err(|e| e.into())
    }

    pub fn get_environment(&self, environment: &String) -> Option<&KafkyEnvironment> {
        self.environments.iter().find(|e| e.name.eq(environment))
    }

    pub fn get_environment_names(&self) -> Vec<String> {
        self.environments.iter().map(|e| e.name.clone()).collect()
    }
}

pub(crate) fn create_sample(config_file_path: &str) -> Result<(), KafkyError> {
    let env = KafkyEnvironment {
        name: "sample-env".to_string(),
        brokers: vec!["localhost:9094".to_string()],
        credentials: vec![
            KafkyCredential {
                name: "plain-cred".to_string(),
                credential: KafkyCredentialKind::PLAIN(KafkyPlainCredential {
                    username: "kafka-user".to_string(),
                    password: "kafka-password".to_string(),
                }),
            },
            KafkyCredential {
                name: "ssl-cred".to_string(),
                credential: KafkyCredentialKind::SSL(KafkySSLCredential {
                    truststore: KafkyPEM::PEM(
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
                    certificate: KafkyPEM::PATH("/my.cert.pem".to_string()),
                    private_key: KafkyPrivateKey {
                        key: KafkyPEM::BASE64("bXkgcHJpdmF0ZSBrZXk=".to_string()),
                        password: Some("my-cert-password".to_string()),
                    },
                }),
            },
        ],
    };
    let config = KafkyConfig {
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
