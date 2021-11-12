use thiserror::Error;

#[derive(Debug, Error)]
pub enum KafkyError {
    #[error("home folder not found")]
    HomeFolderNotFound(),
    #[error("Invalid configuration {0}")]
    InvalidConfiguration(String),
    #[error("Environment not found {0}, available environment {1}")]
    EnvironmentNotFound(String, String),
    #[error("No credential specified, available credential")]
    NoCredentialSpecified(),
    #[error("Credential not found {0}, in the environment {1} available credentials {2}")]
    CredentialNotFound(String, String, String),
    #[error("Kafka error:{0}")]
    KafkaError(String),
    #[error("Invalid command")]
    InvalidCommand(),
    #[error("Invalid offset")]
    InvalidOffset(),
}


