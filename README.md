# cassandra-stress

### RUN COMMAND
```
git clone https://github.com/bmgeek/cassandra-stress-rust.git && cd cassandra-stress-rust
cargo run -- -h HOST:PORT -q QUERY -t THREADS -p PARTITIONS
```

### FOR HELP USE --help
```
cargo run -- --help

cassandra-stress 0.1 beta
AKrupin
cassandra stress test READ

USAGE:
    cassandra_stress [OPTIONS] --host <HOST>

FLAGS:
        --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -h, --host <HOST>                IP or DNS without schema
    -p, --partition <PARTITIONS>    Operations across N partitions
    -q, --query <QUERY>              QUERY to DB with KEYSPACE
    -t, --thread <THREAD>            count threads
```

* HOST - ip address or DNS with port
* PARTITIONS - number of query
* QUERY - query to Cassandra DB (with keyspace)
* THREAD - count of threads