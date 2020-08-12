use cdrs::authenticators::NoneAuthenticator;
use cdrs::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool};
use cdrs::load_balancing::RoundRobinSync;
use cdrs::query::{ExecExecutor, PrepareExecutor};
use cdrs::cluster::session;
use cdrs::cluster::session::Session;

use std::thread;
use std::sync::Arc;

use time::Instant;
use std::time::Duration;

use clap::{App, Arg};

type CurrentSession = Session<RoundRobinSync<TcpConnectionPool<NoneAuthenticator>>>;

struct CassSession {
    host: String,
    query: String,
    partitions: i32,
    threads: i32
}

fn query_select(session: &CurrentSession, configs: &CassSession) { 
    let with_tracing = true;
    let with_warnings = false;
    let query = session.prepare_tw(
        &configs.query
        , with_tracing, with_warnings)
        .unwrap();
    session.exec_tw(&query, with_tracing, with_warnings).unwrap();
}

fn main() {
    let read_log_args = App::new("cassandra-stress")
        .version("0.1 beta")
        .about("cassandra stress test READ")
        .author("AKrupin")
        .arg(Arg::with_name("HOST")
            .short("h")
            .long("host")
            .help("IP or DNS without schema")
            .required(true)
            .takes_value(true))
        .arg(Arg::with_name("QUERY")
            .short("q")
            .long("query")
            .help("QUERY to DB with KEYSPACE")
            .takes_value(true))
        .arg(Arg::with_name("PARTITIONS")
            .short("p")
            .long("partition")
            .help("Operations across N partitions")
            .takes_value(true))
        .arg(Arg::with_name("THREAD")
            .short("t")
            .long("thread")
            .help("count threads")
            .takes_value(true))
        .get_matches();

    let host = read_log_args.value_of("HOST").unwrap();
    // надо будет сделать запрос по умолчанию и ввести эту опцию
    // let query = read_log_args.value_of("QUERY").unwrap_or({
    //     println!("don't get KEYSPACE, using as default");
    //     "empty"
    // });
    let count_threds = read_log_args.value_of("THREAD").unwrap_or("4");
    let partitions = read_log_args.value_of("PARTITIONS").unwrap_or("1000");
    let query = match read_log_args.value_of("QUERY") {
        Some(q) => q,
        None => {
            println!("it's a BETA VERSION, you need get a query!");
            std::process::exit(2)
        }
    };
    let cass_configs = Arc::new(
        CassSession {
            host: String::from(host),
            query: String::from(query),
            threads: count_threds.parse().unwrap(),
            partitions: partitions.parse().unwrap()
        }
    );

    let node = NodeTcpConfigBuilder::new(&cass_configs.host, NoneAuthenticator {})
        .max_size(5)
        .max_lifetime(Some(Duration::from_secs(30)))
        .idle_timeout(Some(Duration::from_secs(30)))
        .build();
    let cluster_config = ClusterTcpConfig(vec![node]);
    let no_compression: Arc<CurrentSession> = Arc::new(
        session::new(&cluster_config, RoundRobinSync::new()).expect("session should be created")
    );

    let start_loop = Instant::now();
    let mut threads = vec![];

    println!("Start stress with Threads: {} and Partitions: {}", cass_configs.threads, cass_configs.partitions);
    for thr in 0..cass_configs.threads {
        let clone_session = no_compression.clone();
        let clone_cass_configs = cass_configs.clone();
        let query_thr = thread::spawn(move || {
            for _ in 0..(clone_cass_configs.partitions/clone_cass_configs.threads) {
                query_select(&clone_session, &clone_cass_configs)
            }
        });
        threads.push(query_thr);
    }

    for trd in threads {
        match trd.join() {
            Ok(a) => a,
            Err(e) => println!("{:?}", e)
        };
    }

    let end_loop = Instant::now();
    println!("{:?}", end_loop-start_loop);
}
