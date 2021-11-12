use config::{Config, ConfigError, File};
use crate::errors::KafkyError;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct KafkyPrivateKey {
    #[serde(flatten)]
    pub key: KafkyPEM,
    pub password: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KafkyPEM {
    PATH(String),
    BASE64(String),
    PEM(String),
}

#[derive(Debug, Deserialize)]
pub struct KafkySSLCredential {
    pub truststore: KafkyPEM,
    pub certificate: KafkyPEM,
    #[serde(rename = "privateKey")]
    pub private_key: KafkyPrivateKey,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KafkyCredentialKind {
    SSL(KafkySSLCredential),
    PLAIN,
}

#[derive(Debug, Deserialize)]
pub struct KafkyCredential {
    pub name: String,
    #[serde(flatten)]
    pub credential: KafkyCredentialKind,
}

#[derive(Debug, Deserialize)]
pub struct KafkyEnvironment {
    pub name: String,
    pub brokers: Vec<String>,
    pub credentials: Vec<KafkyCredential>,
}


impl KafkyEnvironment {
    pub fn get_credential(&self, credential:&String) -> Option<&KafkyCredential> {
        self.credentials.iter().find(|c| c.name.eq(credential))
    }

    pub fn get_credential_names(&self) -> Vec<String> {
        self.credentials.iter().map(|c| c.name.clone()).collect()
    }
}


#[derive(Debug, Deserialize)]
pub struct KafkyConfig {
    pub environments: Vec<KafkyEnvironment>,
}

impl From<ConfigError> for KafkyError {
    fn from(cfg_error: ConfigError) -> Self {
        KafkyError::InvalidConfiguration(cfg_error.to_string())
    }
}

impl KafkyConfig {
    pub fn new(config_file: &str) -> Result<Self, KafkyError> {
        let mut cfg = Config::default();
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
