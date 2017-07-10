#[macro_use]
extern crate clap;
extern crate differential_dataflow;
extern crate slog;
extern crate slog_term;
extern crate timely;
extern crate rand;

use std::time;

use rand::{Rng, SeedableRng, StdRng};

use differential_dataflow::input::Input;
use differential_dataflow::operators::*;

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
        .about("Benchmarks user-curated news aggregator throughput for in-memory Soup")
        .arg(Arg::with_name("avg")
            .long("avg")
            .takes_value(false)
            .help("compute average throughput at the end of benchmark"))
        .arg(Arg::with_name("cdf")
            .short("c")
            .long("cdf")
            .takes_value(false)
            .help("produce a CDF of recorded latencies for each client at the end"))
        .arg(Arg::with_name("stage")
            .short("s")
            .long("stage")
            .takes_value(false)
            .help("stage execution such that all writes are performed before all reads"))
        .arg(Arg::with_name("distribution")
            .short("d")
            .takes_value(true)
            .default_value("uniform")
            .help("run benchmark with the given article id distribution [uniform|zipf:exponent]"))
        .arg(Arg::with_name("ngetters")
            .short("g")
            .long("getters")
            .value_name("N")
            .default_value("1")
            .help("Number of GET clients to start"))
        .arg(Arg::with_name("narticles")
            .short("a")
            .long("articles")
            .value_name("N")
            .default_value("100000")
            .help("Number of articles to prepopulate the database with"))
        .arg(Arg::with_name("runtime")
            .short("r")
            .long("runtime")
            .value_name("N")
            .default_value("60")
            .help("Benchmark runtime in seconds"))
        .arg(Arg::with_name("quiet")
            .short("q")
            .long("quiet")
            .help("No noisy output while running"))
        .arg(Arg::with_name("batch_size")
            .short("b")
            .long("batch-size")
            .takes_value(true)
            .default_value("1000")
            .help("Input batch size to use."))
        .arg(Arg::with_name("read_mix")
            .long("read-mix")
            .takes_value(true)
            .default_value("19")
            .help("Number of reads to perform for each write"))
        .arg(Arg::with_name("workers")
            .short("w")
            .long("workers")
            .value_name("N")
            .default_value("1")
            .help("Number of worker threads"))
        .arg(Arg::with_name("timely_cluster_cfg")
            .long("timely-cluster")
            .takes_value(true)
            .help("Cluster config string to pass to timely."))
        .get_matches();

    let narticles = value_t_or_exit!(args, "narticles", usize);
    let bsize = value_t_or_exit!(args, "batch_size", usize);
    let reads = value_t_or_exit!(args, "read_mix", usize);
    let runtime = value_t_or_exit!(args, "runtime", u64);
    let workers = value_t_or_exit!(args, "workers", usize);
    let cluster_cfg = args.value_of("timely_cluster_cfg");

    run_dataflow(narticles,
                 bsize,
                 reads,
                 runtime,
                 workers,
                 cluster_cfg);
}

fn run_dataflow(articles: usize,
                batch: usize,
                read_mix: usize,
                runtime: u64,
                workers: usize,
                cluster_cfg: Option<&str>) {

    println!("Batching: {:?}", batch);

    let tc = match cluster_cfg {
        None => timely::Configuration::Process(workers),
        Some(ref cc) => {
            timely::Configuration::from_args(cc.split(" ")
                                                 .map(String::from)
                                                 .collect::<Vec<_>>()
                                                 .into_iter())
                    .unwrap()
        }
    };

    // set up the dataflow
    timely::execute(tc, move |worker| {

        let index = worker.index();
        let peers = worker.peers();

        // create a a degree counting differential dataflow
        let (mut articles_in, mut votes_in, mut reads_in, probe) = worker.dataflow(|scope| {

            // create input for read request.
            let (reads_in, reads) = scope.new_collection();
            let (articles_in, articles) = scope.new_collection();
            let (votes_in, votes) = scope.new_collection();

            // merge votes with articles, to ensure counts for un-voted articles.
            let votes = votes.map(|(aid, _uid)| aid)
                             .concat(&articles.map(|(aid, _title)| aid));

            // capture artices and votes, restrict by query article ids.
            let probe = articles.semijoin_u(&votes)
                                .semijoin_u(&reads)
                                .probe();

            (articles_in, votes_in, reads_in, probe)
        });

        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions

        let mut writes: Vec<_> = (0 .. articles).collect();
        rng.shuffle(&mut writes);

        let mut reads: Vec<_> = (0 .. articles).collect();
        rng.shuffle(&mut reads);

        let timer = time::Instant::now();

        // prepopulate articles
        if index == 0 {
            for aid in 0..articles {
                articles_in.insert((aid, format!("Article #{}", aid)));
            }
        }

        // we're done adding articles
        articles_in.close();
        votes_in.advance_to(1); votes_in.flush();
        reads_in.advance_to(1); reads_in.flush();
        worker.step_while(|| probe.less_than(votes_in.time()));

        if index == 0 {
            println!("Loading finished after {:?}", timer.elapsed());
        }

        // now run the throughput measurement
        let start = time::Instant::now();
        let mut round = 1;

        while start.elapsed() < time::Duration::from_millis(runtime * 1000) {

            for count in 0 .. batch {

                let local_step = round * batch + count;
                let logical_time = local_step * peers + index;

                // either write a vote, or read an article.
                if local_step % (read_mix + 1) == 0 {
                    votes_in.advance_to(logical_time);
                    votes_in.insert((writes[local_step % writes.len()], 0))
                } 
                else {
                    reads_in.advance_to(logical_time);
                    reads_in.insert(reads[local_step % writes.len()]);
                    reads_in.advance_to(logical_time + 1);
                    reads_in.remove(reads[local_step % writes.len()]);
                }   

            }

            round += 1;

            votes_in.advance_to(round * batch * peers); votes_in.flush();
            reads_in.advance_to(round * batch * peers); reads_in.flush();
            worker.step_while(|| probe.less_than(votes_in.time()));
        }

        // remove the first round, in which we loaded the data.
        round -= 1;

        if index == 0 {
            println!("processed {} events in {}s => {}",
                     round * batch,
                     dur_to_ns!(start.elapsed()) as f64 / NANOS_PER_SEC as f64,
                     (round * batch) as f64 / (dur_to_ns!(start.elapsed()) / NANOS_PER_SEC as f64));
        }
    }).unwrap();

    println!("Done");
}
