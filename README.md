# Kafky

Kafka Terminal Client entirely written in RUST.

## Features
- [x] auth
    - [x] mutual ssl
    - [x] plain
- [x] environments & credentials in a sharded configuration file ~/.kafky/config.yml
- [x] create/delete/get topic
- [x] get consumer groups
    - [x] lag calculation
- [x] consume messages from multiple topics
    - [x] auto commit
    - [x] different reset
    - [x] json format
    - [x] key / value
- [x] produce messages
  - [x] message history / search
- [ ] brew installation

## Installation

### Cargo

```bash
$ cargo install --git https://github.com/n3wtron/kafky
```

### Brew

Work in progress

## Configuration

location `$HOME/.kafky/config.yml`

```yaml
---
environments:
  - name: sample-env
    brokers:
      - "localhost:9094"
    truststore:
      #path:
      #base64:
      pem: |
        -----BEGIN CERTIFICATE-----
        DQEJARYAMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCJ9WRanG/fUvcfKiGl\n                    DQEJARYAMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCJ9WRanG/fUvcfKiGl\n                    DQEJARYAMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCJ9WRanG/fUvcfKiGl\n                    DQEJARYAMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCJ9WRanG/fUvcfKiGl\n                    DQEJARYAMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCJ9WRanG/fUvcfKiGl
        -----END CERTIFICATE-----
    credentials:
      - name: plain-cred
        plain:
          username: kafka-user
          password: kafka-password
      - name: ssl-cred
        ssl:
          certificate:
            path: /my.cert.pem
            #pem:
            #base64:
          privateKey:
            base64: bXkgcHJpdmF0ZSBrZXk=
            #pem:
            #path:
            password: my-cert-password
```

## Commands

#### Get topics

```bash
USAGE:
    kafky get topics --output-format <format>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -o, --output-format <format>     [default: text]  [possible values: table, text, json]

```

##### list topics

```bash
$ kafly -e sample-env -c plain-cred get topics
bar
foo
```

#### table format

```bash
$ kafly -e sample-env -c plain-cred get topics -o table
TOPIC               PARTITION  LEADER  ISR    REPLICAS
foo                 0          3       3      3
foo                 0          1       1      1
bar                 0          1       1      1
bar                 1          1       1      1
bar                 2          1       1      1
```

#### json format

```bash
$ kafly -e sample-env -c plain-cred get topics -o json
{"name":"test-c","partitions":[{"id":0,"leader":3,"replicas":[3],"isrs":[3]}]}
{"name":"test-b","partitions":[{"id":0,"leader":1,"replicas":[1],"isrs":[1]}]}
{"name":"bar","partitions":[{"id":0,"leader":1,"replicas":[1],"isrs":[1]},{"id":1,"leader":1,"replicas":[1],"isrs":[1]},{"id":2,"leader":1,"replicas":[1],"isrs":[1]}]}
```

### Get Consumer groups
```bash
USAGE:
    kafky get consumer-groups [OPTIONS] --output-format <format>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -o, --output-format <format>     [default: table]  [possible values: table, json]
    -g, --group <groups>...         filter by group names
        --timeout <timeout>         timeout in seconds reading __consumer_offsets topic [default: 10]
    -t, --topic <topics>...         filter by topic names
```

**WARNING**: This command is reading from the `__consumer_offsets` topic, for `--timeout` seconds (default is 10secs), increase it to have more reliable data

#### Example
```bash
$> kafky -e sample-env -c plain-cred get consumer-groups

GROUP     TOPIC  PARTITION  OFFSET  LAG
ssl-host  bar    0          10      14
ssl-host  bar    2          10      12
ssl-host  bar    1          14      8
test      bar    2          11      11
test      bar    0          11      13
test      bar    1          16      6
my-host   bar    1          22      0
my-host   bar    0          22      2
my-host   bar    2          22      0
```

### Consume
```bash
USAGE:
    kafky consume [FLAGS] [OPTIONS] --consumer-group <CONSUMER GROUP NAME> --topic <TOPIC_NAME>...

FLAGS:
    -a, --autocommit    
        --earliest      read from the earliest offset
    -h, --help          Prints help information
        --latest        read from the latest offset (default)
        --timestamp     print timestamp message (works only with text format)
    -V, --version       Prints version information

OPTIONS:
    -c, --consumer-group <CONSUMER GROUP NAME>     [default: $HOSTNAME]
    -o, --output-format <format>                   [default: text]  [possible values: json, text]
    -k, --key-separator <key-separator>           
    -t, --topic <TOPIC_NAME>...
```

#### Examples

##### Simple
```bash
$ kafky -e sample-env -c plain-cred consume -t bar
```

Consume messages from `bar` topic in the `sample-env` environment using `plain-cred` credential

By default, it will consume from the **latest** offset **without** commit

##### From the beginning

```bash
$ kafky -e sample-env -c plain-cred consume -t bar --earliest
```

##### Auto commit

```bash
$ kafky -e sample-env -c plain-cred consume -t bar --auto-commit --consumer-group kafky-group
```

By default, the consumer group is the `$HOSTNAME`

##### Key separator

```bash
$ kafky -e sample-env -c plain-cred consume -t bar --key-separator ::
```

##### Multiple topics

```bash
$ kafky -e sample-env -c plain-cred consume -t bar -t foo
```

##### Message timestamp

```bash
$ kafky -e sample-env -c plain-cred consume -t bar --timestamp
```

##### JSON format

```bash
$ kafky -e sample-env -c plain-cred consume -t bar -o json
```

### Produce

```bash
USAGE:
    kafky produce [OPTIONS] --topic <TOPIC_NAME>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -k, --key-separator <key-separator>    
    -t, --topic <TOPIC_NAME> 
```

#### Example

```bash
$ kafky -e sample-env -c plain-cred produce -t bar --key-separator ::
bar <- my-key::my-payload
bar <- _
```

#### Create Topics

```bash
USAGE:
    kafky create topics --partitions <partitions> --replication-factor <replication_factor> --topic <topic>...

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -p, --partitions <partitions>                    number of partitions
    -r, --replication-factor <replication_factor>    replication factor
    -t, --topic <topic>... 
```

##### Example
```bash
$ kafky -e sample-env -c plain-cred create topics -t topic-1 -t topic-2 --partition 3 --replication-factor 1 
```

#### Delete Topics

```bash
USAGE:
    kafky delete topics [FLAGS] --topic <topic>...

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information
    -y               without confirmation

OPTIONS:
    -t, --topic <topic>...    topic name
```


##### Example
```bash
$ kafky -e sample-env -c plain-cred delete topics -t topic-1 -t topic-2
Are you sure that you want to delete the topic-1, topic-2 topics? [y/N]
_ 
```

### Config

```bash
$ kafky config edit
```
