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
use differential_dataflow::operators::arrange::Arrange;
use differential_dataflow::operators::group::GroupArranged;
use differential_dataflow::trace::implementations::ord::OrdValSpine as DefaultValTrace;
use differential_dataflow::trace::implementations::ord::OrdKeySpine as DefaultKeyTrace;
use differential_dataflow::hashable::UnsignedWrapper;
use differential_dataflow::trace::Trace;

const NANOS_PER_SEC: u64 = 1_000_000_000;
macro_rules! dur_to_ns {
    ($d:expr) => {{
        let d = $d;
        (d.as_secs() * NANOS_PER_SEC + d.subsec_nanos() as u64) as f64
    }}
}

#[derive(Debug)]
enum Batch {
    None,
    Logical(usize),
    Physical(usize),
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
        .arg(Arg::with_name("batch_kind")
            .short("b")
            .long("batch_kind")
            .takes_value(true)
            .requires("batch_size")
            .help("Kind of input batching to use [logical|physical]"))
        .arg(Arg::with_name("batch_size")
            .long("batch-size")
            .takes_value(true)
            .default_value("1000")
            .help("Input batch size to use [if --batch_kind is set]."))
        .arg(Arg::with_name("workers")
            .short("w")
            .long("workers")
            .value_name("N")
            .default_value("1")
            .help("Number of worker threads"))
        .arg(Arg::with_name("merge_ops")
            .long("merge-operators")
            .help("Merge operators inside the data-flow."))
        .get_matches();

    let narticles = value_t_or_exit!(args, "narticles", usize);
    let bsize = value_t_or_exit!(args, "batch_size", usize);
    let batch = match args.value_of("batch_kind") {
        None => Batch::None,
        Some(v) => {
            match v {
                "none" => Batch::None,
                "logical" => Batch::Logical(bsize),
                "physical" => Batch::Physical(bsize),
                _ => panic!("unexpected batch kind {}", v),
            }
        }
    };
    let merge = args.is_present("merge_ops");
    let runtime = value_t_or_exit!(args, "runtime", u64);
    let workers = value_t_or_exit!(args, "workers", usize);

    run_dataflow(narticles, batch, merge, runtime, workers);
}

fn run_dataflow(articles: usize, batch: Batch, merge: bool, runtime: u64, workers: usize) {

    println!("Batching: {:?}, merging: {}", batch, merge);

    // set up the dataflow
    timely::execute(timely::Configuration::Process(workers), move |worker| {
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

            let awvc = if merge {
                let vc =
                    votes
                        .map(|(aid, _uid)| (UnsignedWrapper::from(aid), ()))
                        .arrange(DefaultKeyTrace::new())
                        .group_arranged(|_k, s, t| t.push((s[0].1, 1)), DefaultValTrace::new());

                // compute ArticleWithVoteCount view
                articles
                    .map(|(aid, text)| (UnsignedWrapper::from(aid), text))
                    .arrange(DefaultValTrace::new())
                    .join_core(&vc, |k: &UnsignedWrapper<usize>, text: &String, &count| {
                        Some((k.item, text.clone(), count))
                    })
            } else {
                // simple vote aggregation
                let vc = votes.map(|(aid, _uid)| aid).count_u();

                // compute ArticleWithVoteCount view
                articles.join_u(&vc)
            };

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
        }

        // we're done adding articles
        art_in.close();
        vt_in.advance_to(1);
        worker.step_while(|| probe.less_than(vt_in.time()));

        if index == 0 {
            println!("Loading finished after {:?}", timer.elapsed());
        }

        // now run the throughput measurement
        let start = time::Instant::now();
        let mut count = 1;
        let mut session = differential_dataflow::input::InputSession::from(&mut vt_in);

        while start.elapsed() < time::Duration::from_millis(runtime * 1000) {
            if count % peers == index {
                match batch {
                    Batch::Logical(_) => (),
                    Batch::None |
                    Batch::Physical(_) => {
                        session.advance_to(count);
                    }
                }

                session.insert((count % articles, 0));
            }

            match batch {
                Batch::None => {
                    session.advance_to(count + 1);
                    session.flush();
                    worker.step_while(|| probe.less_than(session.time()));
                }
                Batch::Logical(bs) => {
                    if count % bs == 0 {
                        let &t = session.time();
                        session.advance_to(t.inner + 1);
                        session.flush();
                        worker.step_while(|| probe.less_than(session.time()));
                    }
                }
                Batch::Physical(bs) => {
                    if count % bs == 0 {
                        session.advance_to(count + 1);
                        session.flush();
                        worker.step_while(|| probe.less_than(session.time()));
                    }
                }
            }

            count += 1;
        }

        if index == 0 {
            println!("wrote {} votes in {}s => {}",
                     count,
                     dur_to_ns!(start.elapsed()) as f64 / NANOS_PER_SEC as f64,
                     count as f64 / (dur_to_ns!(start.elapsed()) / NANOS_PER_SEC as f64));
        }
    })
            .unwrap();

    println!("Done");
}
