#[macro_use]
extern crate log;

fn main() {
    example::main();
}

#[cfg(feature = "security")]
mod example {
    extern crate env_logger;
    extern crate getopts;

    use std::env;
    use std::fs;
    use std::io::{BufReader, Read, Write};
    use std::process;
    use std::sync::Arc;

    use kafka::client::{FetchOffset, KafkaClient, SecurityConfig};

    pub fn main() {
        env_logger::init();

        // ~ parse the command line arguments
        let cfg = match Config::from_cmdline() {
            Ok(cfg) => cfg,
            Err(e) => {
                println!("{}", e);
                process::exit(1);
            }
        };

        let mut rustls_config = {
            let mut config = rustls::ClientConfig::new();
            config
                .root_store
                .add_server_trust_anchors(&webpki_roots::TLS_SERVER_ROOTS);
            if let (Some(ccert), Some(ckey)) = (cfg.client_cert, cfg.client_key) {
                info!("loading cert-file={}, key-file={}", ccert, ckey);
                let certs = {
                    let file = fs::File::open(ccert).expect("cannot open private key file");
                    let mut reader = BufReader::new(file);
                    rustls::internal::pemfile::certs(&mut reader)
                        .expect("unable to read certs file")
                };

                let keys = {
                    let file = fs::File::open(ckey).expect("cannot open private key file");
                    let mut reader = BufReader::new(file);
                    rustls::internal::pemfile::rsa_private_keys(&mut reader)
                        .expect("unable to read key file")
                };

                let key = keys[0].clone();
                config.set_single_client_cert(certs, key);
            }

            if let Some(filename) = cfg.ca_cert {
                let keyfile = fs::File::open(&filename).expect("cannot open private key file");
                let mut reader = BufReader::new(keyfile);
                let keys = rustls::internal::pemfile::certs(&mut reader).unwrap();
                info!("loading ca-file={}", filename);
                config.root_store.add(&keys[0]);
            } else {
                // ~ allow client specify the CAs through the default paths:
                // "These locations are read from the SSL_CERT_FILE and
                // SSL_CERT_DIR environment variables if present, or defaults
                // specified at OpenSSL build time otherwise."
                // builder.set_default_verify_paths().unwrap();
            }
            config
        };

        // ~ instantiate KafkaClient with the previous OpenSSL setup
        let mut client = KafkaClient::new_secure(cfg.brokers, SecurityConfig::new(rustls_config));

        // ~ communicate with the brokers
        match client.load_metadata_all() {
            Err(e) => {
                println!("{:?}", e);
                drop(client);
                process::exit(1);
            }
            Ok(_) => {
                // ~ at this point we have successfully loaded
                // metadata via a secured connection to one of the
                // specified brokers

                if client.topics().len() == 0 {
                    println!("No topics available!");
                } else {
                    // ~ now let's communicate with all the brokers in
                    // the cluster our topics are spread over

                    let topics: Vec<String> = client.topics().names().map(Into::into).collect();
                    match client.fetch_offsets(topics.as_slice(), FetchOffset::Latest) {
                        Err(e) => {
                            println!("{:?}", e);
                            drop(client);
                            process::exit(1);
                        }
                        Ok(toffsets) => {
                            println!("Topic offsets:");
                            for (topic, mut offs) in toffsets {
                                offs.sort_by_key(|x| x.partition);
                                println!("{}", topic);
                                for off in offs {
                                    println!("\t{}: {:?}", off.partition, off.offset);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    struct Config {
        brokers: Vec<String>,
        client_cert: Option<String>,
        client_key: Option<String>,
        ca_cert: Option<String>,
        verify_hostname: bool,
    }

    impl Config {
        fn from_cmdline() -> Result<Config, String> {
            let mut opts = getopts::Options::new();
            opts.optflag("h", "help", "Print this help screen");
            opts.optopt(
                "",
                "brokers",
                "Specify kafka brokers (comma separated)",
                "HOSTS",
            );
            opts.optopt("", "ca-cert", "Specify the trusted CA certificates", "FILE");
            opts.optopt("", "client-cert", "Specify the client certificate", "FILE");
            opts.optopt(
                "",
                "client-key",
                "Specify key for the client certificate",
                "FILE",
            );
            opts.optflag(
                "",
                "no-hostname-verification",
                "Do not perform server hostname verification (insecure!)",
            );

            let args: Vec<_> = env::args().collect();
            let m = match opts.parse(&args[1..]) {
                Ok(m) => m,
                Err(e) => return Err(format!("{}", e)),
            };

            if m.opt_present("help") {
                let brief = format!("{} [options]", args[0]);
                return Err(opts.usage(&brief));
            };

            let brokers = m
                .opt_str("brokers")
                .map(|s| {
                    s.split(',')
                        .map(|s| s.trim().to_owned())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_else(|| vec!["localhost:9092".into()]);
            if brokers.is_empty() {
                return Err("Invalid --brokers specified!".to_owned());
            }

            Ok(Config {
                brokers,
                client_cert: m.opt_str("client-cert"),
                client_key: m.opt_str("client-key"),
                ca_cert: m.opt_str("ca-cert"),
                verify_hostname: !m.opt_present("no-hostname-verification"),
            })
        }
    }
}

#[cfg(not(feature = "security"))]
mod example {
    use std::process;

    pub fn main() {
        println!("example relevant only with the \"security\" feature enabled!");
        process::exit(1);
    }
}
