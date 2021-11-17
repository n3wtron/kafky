use std::sync::{Arc, Mutex};

use log::debug;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::BaseConsumer;
use rdkafka::producer::BaseProducer;
use rdkafka::ClientConfig;

use crate::config::{KafkyConfig, KafkyCredentialKind, KafkyPEM};
use crate::KafkyError;

pub(crate) struct KafkyClient<'a> {
    kafky_config: &'a KafkyConfig<'a>,
    environment: String,
    credential: String,
    producer: Mutex<Option<Arc<BaseProducer>>>,
    util_consumer: Mutex<Option<Arc<BaseConsumer>>>,
    admin_client: Mutex<Option<Arc<AdminClient<DefaultClientContext>>>>,
}

impl<'a> KafkyClient<'a> {
    pub fn new(config: &'a KafkyConfig, environment: String, credential: String) -> Self {
        KafkyClient {
            kafky_config: config,
            environment,
            credential,
            producer: Mutex::new(None),
            util_consumer: Mutex::new(None),
            admin_client: Mutex::new(None),
        }
    }

    pub(super) fn config_builder(&self) -> ClientConfig {
        let environment = self
            .kafky_config
            .get_environment(&self.environment)
            .ok_or_else(|| {
                KafkyError::EnvironmentNotFound(
                    self.environment.to_string(),
                    self.kafky_config.get_environment_names().join(","),
                )
            })
            .unwrap();
        let credential = environment
            .get_credential(&self.credential)
            .ok_or_else(|| {
                KafkyError::CredentialNotFound(
                    self.credential.to_string(),
                    self.environment.to_string(),
                    environment.get_credential_names().join(","),
                )
            })
            .unwrap();

        let brokers = environment.brokers.join(",");
        let mut client_config_builder = ClientConfig::new();
        client_config_builder.set("bootstrap.servers", brokers);
        client_config_builder.set_log_level(RDKafkaLogLevel::Debug);
        debug!("{:?}", client_config_builder);

        match &credential.credential {
            KafkyCredentialKind::Ssl(ssl_cred) => {
                client_config_builder.set("security.protocol", "ssl");
                match &ssl_cred.truststore {
                    KafkyPEM::Path(path) => {
                        client_config_builder.set("ssl.ca.location", path);
                    }
                    KafkyPEM::Base64(b64) => {
                        let decoded_pem = base64::decode(b64).expect(&*format!(
                            "Invalid truststore base64 for the environment:{} credential:{}",
                            self.environment, self.credential
                        ));
                        let pem = String::from_utf8(decoded_pem).unwrap();
                        client_config_builder.set("ssl.ca.pem", pem);
                    }
                    KafkyPEM::Pem(pem) => {
                        client_config_builder.set("ssl.ca.pem", pem);
                    }
                }
                match &ssl_cred.certificate {
                    KafkyPEM::Path(path) => {
                        client_config_builder.set("ssl.certificate.location", path);
                    }
                    KafkyPEM::Base64(b64) => {
                        let decoded_pem = base64::decode(b64).expect(&*format!(
                            "Invalid certificate base64 for the environment:{} credential:{}",
                            self.environment, self.credential
                        ));
                        let pem = String::from_utf8(decoded_pem).unwrap();
                        client_config_builder.set("ssl.certificate.pem", pem);
                    }
                    KafkyPEM::Pem(pem) => {
                        client_config_builder.set("ssl.certificate.pem", pem);
                    }
                }
                match &ssl_cred.private_key.key {
                    KafkyPEM::Path(path) => {
                        client_config_builder.set("ssl.key.location", path);
                    }
                    KafkyPEM::Base64(b64) => {
                        let decoded_pem = base64::decode(b64).expect(&*format!(
                            "Invalid private key base64 for the environment:{} credential:{}",
                            self.environment, self.credential
                        ));
                        let pem = String::from_utf8(decoded_pem).unwrap();
                        client_config_builder.set("ssl.key.pem", pem);
                    }
                    KafkyPEM::Pem(pem) => {
                        client_config_builder.set("ssl.key.pem", pem);
                    }
                }
                match &ssl_cred.private_key.password {
                    None => {}
                    Some(key_password) => {
                        client_config_builder.set("ssl.key.password", key_password);
                    }
                }
            }
            KafkyCredentialKind::Plain(plain_creds) => {
                client_config_builder.set("security.protocol", "plaintext");
                client_config_builder.set("sasl.username", &plain_creds.username);
                client_config_builder.set("sasl.password", &plain_creds.password);
            }
        };
        client_config_builder
    }

    pub(super) fn get_producer(&self) -> Result<Arc<BaseProducer>, KafkyError> {
        let mut mtx_producer = self.producer.lock().unwrap();
        let opt_producer = (*mtx_producer).as_ref();
        match opt_producer {
            None => {
                let producer: Arc<BaseProducer> = Arc::new(
                    self.config_builder()
                        .set("message.timeout.ms", "5000")
                        .create()?,
                );
                *mtx_producer = Some(producer.clone());
                Ok(producer)
            }
            Some(producer) => Ok(producer.clone()),
        }
    }

    pub(super) fn get_util_consumer(&self) -> Result<Arc<BaseConsumer>, KafkyError> {
        let mut mtx_consumer = self.util_consumer.lock().unwrap();
        let util_consumer = (*mtx_consumer).as_ref();
        match util_consumer {
            None => {
                let consumer: Arc<BaseConsumer> = Arc::new(self.config_builder().create()?);
                *mtx_consumer = Some(consumer.clone());
                Ok(consumer)
            }
            Some(consumer) => Ok(consumer.clone()),
        }
    }

    pub(super) fn get_admin_client(
        &self,
    ) -> Result<Arc<AdminClient<DefaultClientContext>>, KafkyError> {
        let mut mtx_consumer = self.admin_client.lock().unwrap();
        let admin_client = (*mtx_consumer).as_ref();
        match admin_client {
            None => {
                let client: Arc<AdminClient<DefaultClientContext>> =
                    Arc::new(self.config_builder().create()?);
                *mtx_consumer = Some(client.clone());
                Ok(client)
            }
            Some(client) => Ok(client.clone()),
        }
    }
}
