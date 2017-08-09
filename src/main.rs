#[macro_use]
extern crate clap;
extern crate eintopf;
extern crate differential_dataflow;
extern crate nix;
extern crate slog;
extern crate slog_term;
extern crate timely;
extern crate rand;

use std::time;

use rand::{Rng, SeedableRng, StdRng};

use differential_dataflow::input::Input;
use differential_dataflow::operators::*;

use eintopf::{Config, Eintopf};

const NANOS_PER_SEC: u64 = 1_000_000_000;
macro_rules! dur_to_ns {
    ($d:expr) => {{
        let d = $d;
        (d.as_secs() * NANOS_PER_SEC + d.subsec_nanos() as u64) as f64
    }}
}

fn main() {
    use clap::{Arg, App};

    let args = App::new("vote")
        .version("0.1")
        .about(
            "Benchmarks user-curated news aggregator throughput for in-memory Soup",
        )
        .arg(
            Arg::with_name("avg")
                .long("avg")
                .takes_value(false)
                .help("compute average throughput at the end of benchmark"),
        )
        .arg(
            Arg::with_name("cdf")
                .short("c")
                .long("cdf")
                .takes_value(false)
                .help(
                    "produce a CDF of recorded latencies for each client at the end",
                ),
        )
        .arg(
            Arg::with_name("stage")
                .short("s")
                .long("stage")
                .takes_value(false)
                .help(
                    "stage execution such that all writes are performed before all reads",
                ),
        )
        .arg(
            Arg::with_name("distribution")
                .short("d")
                .takes_value(true)
                .default_value("uniform")
                .help(
                    "run benchmark with the given article id distribution [uniform|zipf:exponent]",
                ),
        )
        .arg(
            Arg::with_name("ngetters")
                .short("g")
                .long("getters")
                .value_name("N")
                .default_value("1")
                .help("Number of GET clients to start"),
        )
        .arg(
            Arg::with_name("narticles")
                .short("a")
                .long("articles")
                .value_name("N")
                .default_value("100000")
                .help("Number of articles to prepopulate the database with"),
        )
        .arg(
            Arg::with_name("runtime")
                .short("r")
                .long("runtime")
                .value_name("N")
                .default_value("60")
                .help("Benchmark runtime in seconds"),
        )
        .arg(
            Arg::with_name("quiet")
                .short("q")
                .long("quiet")
                .help("No noisy output while running"),
        )
        .arg(
            Arg::with_name("core-pin")
                .long("core-pin")
                .help("Pin workers to specific cores"),
        )
        .arg(
            Arg::with_name("batch_size")
                .short("b")
                .long("batch-size")
                .takes_value(true)
                .default_value("1000")
                .help("Input batch size to use."),
        )
        .arg(
            Arg::with_name("read_mix")
                .long("read-mix")
                .takes_value(true)
                .default_value("19")
                .help("Number of reads to perform for each write"),
        )
        .arg(
            Arg::with_name("workers")
                .short("w")
                .long("workers")
                .value_name("N")
                .default_value("1")
                .help("Number of worker threads"),
        )
        .arg(
            Arg::with_name("timely_cluster_cfg")
                .long("timely-cluster")
                .takes_value(true)
                .help("Cluster config string to pass to timely."),
        )
        .get_matches();

    let narticles = value_t_or_exit!(args, "narticles", usize);
    let bsize = value_t_or_exit!(args, "batch_size", usize);
    let reads = value_t_or_exit!(args, "read_mix", usize);
    let runtime = value_t_or_exit!(args, "runtime", u64);
    let workers = value_t_or_exit!(args, "workers", usize);
    let cluster_cfg = args.value_of("timely_cluster_cfg");
    let pinning = args.is_present("core-pin");

    let e = Eintopf::new(Config {
        articles: narticles,
        batch: bsize,
        read_mix: reads,
        runtime: Some(runtime),
        workers: workers,
        cluster_cfg: cluster_cfg.map(|s| String::from(s)),
        pinning: pinning,
    });

    e.run()
}
