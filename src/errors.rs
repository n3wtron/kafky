use config::ConfigError;
use rdkafka::error::KafkaError;
use std::io;
use std::str;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum KafkyError {
    #[error("Configuration not found: {0}")]
    ConfigurationNotFound(String),
    #[error("Invalid configuration {0}")]
    InvalidConfiguration(String),
    #[error("Environment parameter is mandatory")]
    EnvironmentParamNotFound(),
    #[error("Environment not found {0}, available environment {1}")]
    EnvironmentNotFound(String, String),
    #[error("No credential specified, available credentials {0}")]
    NoCredentialSpecified(String),
    #[error("Parse error :{0}")]
    ParseError(String),
    #[error("Credential not found {0}, in the environment {1} available credentials {2}")]
    CredentialNotFound(String, String, String),
    #[error("Kafka error:{0}")]
    KafkaError(String),
    #[error("Invalid command")]
    InvalidCommand(),
    #[error("Invalid offset")]
    InvalidOffset(),
    #[error("Error creating sample config: {0}")]
    CannotCreateSampleConfig(String),
    #[error("Topic not found: {0}")]
    TopicNotFound(String),
    #[error("Invalid json: {0}")]
    InvalidJson(String),
}

impl From<KafkaError> for KafkyError {
    fn from(kafka_error: KafkaError) -> Self {
        KafkyError::KafkaError(kafka_error.to_string())
    }
}

impl From<ConfigError> for KafkyError {
    fn from(cfg_error: ConfigError) -> Self {
        KafkyError::InvalidConfiguration(cfg_error.to_string())
    }
}

impl From<serde_json::Error> for KafkyError {
    fn from(json_error: serde_json::Error) -> Self {
        KafkyError::InvalidJson(json_error.to_string())
    }
}

impl From<io::Error> for KafkyError {
    fn from(err: io::Error) -> KafkyError {
        KafkyError::ParseError(err.to_string())
    }
}

impl From<str::Utf8Error> for KafkyError {
    fn from(err: str::Utf8Error) -> KafkyError {
        KafkyError::ParseError(err.to_string())
    }
}
