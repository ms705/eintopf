#[macro_use]
extern crate clap;
extern crate differential_dataflow;
extern crate slog;
extern crate slog_term;
extern crate time;
extern crate timely;

use timely::dataflow::operators::*;

use differential_dataflow::AsCollection;
use differential_dataflow::operators::*;

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
        let _peers = worker.peers();

        // create a a degree counting differential dataflow
        let (mut art_in, mut vt_in, _probe) = worker.dataflow(|scope| {
            // create articles input
            let (art_in, articles) = scope.new_input();
            // create votes input
            let (vt_in, votes) = scope.new_input();

            let articles = articles.as_collection();
            let votes = votes.as_collection();

            let vc = votes.map(|(aid, _uid)| aid).count_u();
            let awvc = articles.join_u(&vc);

            let probe = awvc.inspect(|x| println!("{:?}", x.0)).probe();

            (art_in, vt_in, probe)
        });

        let timer = ::std::time::Instant::now();

        // prepopulate
        let &art_time = art_in.time();
        //let &vt_time = vt_in.time();
        for aid in 0..articles {
            art_in.send(((aid, format!("Article #{}", aid)), art_time, 1));
            //vt_in.send(((aid, 0), vt_time, 1));

            worker.step();
        }

        art_in.advance_to(1);
        //worker.step_while(|| probe.less_than(art_in.time()));

        if index == 0 {
            println!("Loading finished after {:?}", timer.elapsed());
        }

        let &t = vt_in.time();
        vt_in.send(((0, 0u32), t, 1));
        vt_in.send(((0, 0u32), t, 1));
        worker.step();

    })
            .unwrap();

    println!("Wheeeey, done");
}
