#[macro_use]
extern crate clap;
extern crate differential_dataflow;
extern crate slog;
extern crate slog_term;
extern crate timely;

use std::time;

use timely::dataflow::operators::*;

use differential_dataflow::AsCollection;
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
        .get_matches();

    let narticles = value_t_or_exit!(args, "narticles", usize);

    run_dataflow(narticles);
}

fn run_dataflow(articles: usize) {
    //let batch: usize = 100;

    timely::execute(timely::Configuration::Process(1), move |worker| {
        let index = worker.index();
        let peers = worker.peers();

        // create a a degree counting differential dataflow
        let (mut art_in, mut vt_in, probe) = worker.dataflow(|scope| {
            // create articles input
            let (art_in, articles) = scope.new_input();
            // create votes input
            let (vt_in, votes) = scope.new_input();

            let articles = articles.as_collection();
            let votes = votes.as_collection();

            // simple vote aggregation
            let vc = votes.map(|(aid, _uid)| aid).count_u();

            // compute ArticleWithVoteCount view
            let awvc = articles.join_u(&vc);

            let probe = awvc.probe();

            (art_in, vt_in, probe)
        });

        let timer = time::Instant::now();

        // prepopulate
        if index == 0 {
            let &art_time = art_in.time();
            let &vt_time = vt_in.time();
            for aid in 0..articles {
                // add article
                art_in.send(((aid, format!("Article #{}", aid)), art_time, 1));

                // vote once for each article as we don't have a convenient left join; this ensures
                // that all articles are present in awvc
                vt_in.send(((aid, 0), vt_time, 1));
            }

            // wait for things to propagate
            art_in.advance_to(1);
            vt_in.advance_to(1);
            worker.step_while(|| probe.less_than(art_in.time()));

            println!("Loading finished after {:?}", timer.elapsed());

            // now run the throughput measurement
            let start = time::Instant::now();
            let mut count = 0;

            while start.elapsed() < time::Duration::from_millis(60000) {
                let &t = vt_in.time();
                vt_in.send(((count % articles, 0u32), t, 1));
                worker.step();
                count += 1;
            }
            println!("worker {}: {} in {}s => {}",
                     index,
                     count,
                     dur_to_ns!(start.elapsed()) as f64 / NANOS_PER_SEC as f64,
                     count as f64 / (dur_to_ns!(start.elapsed()) / NANOS_PER_SEC as f64));
        }
    })
            .unwrap();

    println!("Done");
}
