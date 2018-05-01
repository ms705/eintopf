extern crate abomonation;
#[macro_use]
extern crate clap;
extern crate differential_dataflow;
extern crate nix;
extern crate rand;
extern crate slog;
extern crate slog_term;
extern crate timely;
extern crate zipf;
extern crate istring;

use std::{fs, thread, time};

use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::operators::Probe;
use timely::dataflow::operators::Input as TimelyInput;
use timely::progress::timestamp::RootTimestamp;

use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::input::Input;
use differential_dataflow::operators::*;

use abomonation::Abomonation;
use zipf::ZipfDistribution;

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Debug)]
struct StringWrapper {
    pub string: istring::IString,
}

#[derive(Clone, Copy)]
pub enum Distribution {
    Uniform,
    Zipf(f64),
}

use std::str::FromStr;
impl FromStr for Distribution {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "uniform" {
            Ok(Distribution::Uniform)
        } else if s.starts_with("zipf:") {
            let s = s.trim_left_matches("zipf:");
            str::parse::<f64>(s)
                .map(|exp| Distribution::Zipf(exp))
                .map_err(|e| {
                    use std::error::Error;
                    e.description().to_string()
                })
        } else {
            Err(format!("unknown distribution '{}'", s))
        }
    }
}

use std::io::Write;
use std::io::Result as IOResult;

impl Abomonation for StringWrapper {
    #[inline]
    unsafe fn entomb<W: Write>(&self, write: &mut W) -> IOResult<()>{
        if !self.string.is_inline() {
            write.write_all(self.string.as_bytes())?;
        }
        Ok(())
    }
    #[inline]
    unsafe fn exhume<'a, 'b>(&'a mut self, bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        if !self.string.is_inline() {
            if self.string.len() > bytes.len() {
                None
            } else {
                let (mine, rest) = bytes.split_at_mut(self.string.len());
                std::ptr::write(
                    ::std::mem::transmute(self.string.as_heap().ptr),
                    mine.as_ptr(),
                );
                Some(rest)
            }
        } else {
            Some(bytes)
        }
    }
}

const NANOS_PER_SEC: u64 = 1_000_000_000;
macro_rules! dur_to_ns {
    ($d:expr) => {{
        let d = $d;
        (d.as_secs() * NANOS_PER_SEC + d.subsec_nanos() as u64) as f64
    }}
}

fn main() {
    use clap::{App, Arg};

    let args = App::new("vote")
        .version("0.1")
        .about("Benchmarks user-curated news aggregator throughput for in-memory Soup")
        .arg(
            Arg::with_name("open-loop")
                .long("open-loop")
                .help("Run open-loop (vs closed-loop)"),
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
                .help("produce a CDF of recorded latencies for each client at the end"),
        )
        .arg(
            Arg::with_name("stage")
                .short("s")
                .long("stage")
                .takes_value(false)
                .help("stage execution such that all writes are performed before all reads"),
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
                .short("z")
                .long("workers")
                .value_name("N")
                .default_value("1")
                .help("Number of worker threads"),
        )
        .arg(
            Arg::with_name("host_file")
                .short("h")
                .long("hosts")
                .takes_value(true)
                .requires("process_id")
                .help("File with IP:PORT of processes in timely cluster."),
        )
        .arg(
            Arg::with_name("process_id")
                .short("p")
                .long("process")
                .takes_value(true)
                .requires("host_file")
                .help("Process ID in timely cluster."),
        )
        .get_matches();

    let narticles = value_t_or_exit!(args, "narticles", usize);
    let bsize = value_t_or_exit!(args, "batch_size", usize);
    let dist = value_t_or_exit!(args, "distribution", Distribution);
    let reads = value_t_or_exit!(args, "read_mix", usize);
    let runtime = value_t_or_exit!(args, "runtime", u64);
    let workers = value_t_or_exit!(args, "workers", usize);
    let host_file = args.value_of("host_file");
    let process_id = args.value_of("process_id")
        .map(|s| usize::from_str(s).unwrap());
    let pinning = args.is_present("core-pin");
    let open_loop = args.is_present("open-loop");

    run_dataflow(
        narticles,
        bsize,
        reads,
        runtime,
        workers,
        host_file,
        process_id,
        pinning,
        dist,
        open_loop,
    );
}

#[cfg(target_os = "linux")]
fn pin_to_core(index: usize, stride: usize) {
    let mut cpu_set = ::nix::sched::CpuSet::new();
    let tgt_cpu = index * stride;
    cpu_set.set(tgt_cpu).unwrap();
    ::nix::sched::sched_setaffinity(::nix::unistd::Pid::from_raw(0), &cpu_set).unwrap()
}
#[cfg(not(target_os = "linux"))]
fn pin_to_core(_index: usize, _stride: usize) {
    println!("core pinning: not linux");
}

fn run_dataflow(
    articles: usize,
    batch: usize,
    read_mix: usize,
    runtime: u64,
    workers: usize,
    host_file: Option<&str>,
    process_id: Option<usize>,
    pinning: bool,
    distribution: Distribution,
    open_loop: bool,
) {
    let tc = match host_file {
        None => timely::Configuration::Process(workers),
        Some(ref hf) => {
            let host_list = String::from_utf8_lossy(&fs::read(hf).unwrap())
                .lines()
                .map(String::from)
                .collect();
            timely::Configuration::Cluster(workers, process_id.unwrap(), host_list, false)
        }
    };

    // set up the dataflow
    timely::execute(tc, move |worker| {
        let index = worker.index();
        let peers = worker.peers();

        if pinning {
            pin_to_core(index, 1);
        }

        // create a a degree counting differential dataflow
        let (mut articles_in, mut votes_in, mut reads_in, probe) = worker.dataflow(|scope| {
            // create input for read request.
            // let (reads_in, reads) = scope.new_collection();
            let (reads_in, reads) = scope.new_input();
            let (articles_in, articles) = scope.new_collection();
            let (votes_in, votes) = scope.new_collection();

            // merge votes with articles, to ensure counts for un-voted articles.
            let votes = votes
                .map(|(aid, _uid)| aid)
                .concat(&articles.map(|(aid, _title): (usize, StringWrapper)| aid));

            // capture artices and votes, restrict by query article ids.
            // let probe = articles.semijoin(&votes).semijoin(&reads).probe();
            let probe = differential_dataflow::operators::arrange::query(&reads, articles.semijoin(&votes).arrange_by_key().trace).probe();

            (articles_in, votes_in, reads_in, probe)
        });

        let seed: &[_] = &[1, 2, 3, index];

        let n = articles;
        let get_random = || match distribution {
            Distribution::Uniform => {
                let mut u: StdRng = SeedableRng::from_seed(seed);
                (0..n).map(|_| u.gen_range(0, articles) as usize).collect()
            }
            Distribution::Zipf(e) => {
                let mut z =
                    ZipfDistribution::new(rand::thread_rng(), articles as usize, e).unwrap();
                (0..n).map(|_| z.gen_range(0, articles) as usize).collect()
            }
        };

        let writes: Vec<_> = get_random();
        let reads: Vec<_> = get_random();

        let timer = time::Instant::now();

        // prepopulate articles
        if index == 0 {
            for aid in 0..articles {
                // articles_in.insert((aid, format!("Article #{}", aid)));
                let source = format!("Article #{}", aid);
                let string = StringWrapper { string: istring::IString::from(&source[..]) };
                articles_in.insert((aid, string));
                // articles_in.insert((aid, String::new()));
                // articles_in.insert((aid, aid));
                // articles_in.insert((aid, ()));
            }
        }

        // we're done adding articles
        articles_in.close();
        votes_in.advance_to(1);
        votes_in.flush();
        reads_in.advance_to(1);
        // reads_in.flush();
        worker.step_while(|| probe.less_than(votes_in.time()));

        if index == 0 {
            println!("Loading finished after {:?}", timer.elapsed());
        }

        let start = time::Instant::now();
        if !open_loop {

            println!("ClosedLoop; Batching: {:?}", batch);

            // now run the throughput measurement
            let mut round = 1;

            while start.elapsed() < time::Duration::from_millis(runtime * 1000) {
                for count in 0..batch {
                    let local_step = round * batch + count;
                    let logical_time = local_step * peers + index;

                    // either write a vote, or read an article.
                    if local_step % (read_mix + 1) == 0 {
                        votes_in.advance_to(logical_time);
                        votes_in.insert((writes[local_step % writes.len()], 0))
                    } else {
                        // reads_in.advance_to(logical_time);
                        reads_in.send((reads[local_step % reads.len()], RootTimestamp::new(logical_time)));
                        // reads_in.insert(reads[local_step % reads.len()]);
                        // reads_in.advance_to(logical_time + 1);
                        // reads_in.remove(reads[local_step % reads.len()]);
                    }
                }

                round += 1;

                votes_in.advance_to(round * batch * peers);
                votes_in.flush();
                reads_in.advance_to(round * batch * peers);
                // reads_in.flush();
                worker.step_while(|| probe.less_than(votes_in.time()));
            }

            // remove the first round, in which we loaded the data.
            round -= 1;

            if index == 0 {
                println!(
                    "processed {} events in {}s over {} peers => {}",
                    round * batch * peers,
                    dur_to_ns!(start.elapsed()) as f64 / NANOS_PER_SEC as f64,
                    peers,
                    (round * batch * peers) as f64
                        / (dur_to_ns!(start.elapsed()) / NANOS_PER_SEC as f64)
                );
            }
        }
        else {

            println!("OpenLoop; OfferedLoad: {:?}", batch);

            let timer = ::std::time::Instant::now();
            let mut counts = vec![[0u64; 16]; 64];


            let requests_per_sec = batch;
            let ns_per_request = 1_000_000_000 / requests_per_sec;
            let mut request_counter = peers + index;    // skip first request for each.
            let mut ack_counter = peers + index;

            let mut inserted_ns = 1;

            let ack_target = (runtime as usize) * requests_per_sec;

            while ack_counter < ack_target {
            // while ((timer.elapsed().as_secs() as usize) * rate) < (10 * keys) {

                // Open-loop latency-throughput test, parameterized by offered rate `ns_per_request`.
                let elapsed = timer.elapsed();
                let elapsed_ns = elapsed.as_secs() * 1_000_000_000 + (elapsed.subsec_nanos() as u64);
                let elapsed_ns = elapsed_ns as usize;

                // Determine completed ns.
                let acknowledged_ns: usize = probe.with_frontier(|frontier| frontier[0].inner);

                // any un-recorded measurements that are complete should be recorded.
                while ((ack_counter * ns_per_request)) < acknowledged_ns && ack_counter < ack_target {
                    let requested_at = ack_counter * ns_per_request;
                    let count_index = (elapsed_ns - requested_at).next_power_of_two().trailing_zeros() as usize;
                    if ack_counter > ack_target / 2 {
                        // counts[count_index] += 1;
                        let low_bits = ((elapsed_ns - requested_at) >> (count_index - 5)) & 0xF;
                        counts[count_index][low_bits as usize] += 1;
                    }
                    ack_counter += peers;
                }

                // let target_ns = if acknowledged_ns >= inserted_ns { elapsed_ns } else { inserted_ns };

                let target_ns = elapsed_ns & !((1 << 16) - 1);

                if inserted_ns < target_ns {

                    while (request_counter * ns_per_request) < target_ns {
                        let logical_time = request_counter * ns_per_request;
                        let local_step = request_counter / peers;
                        if local_step % (read_mix + 1) == 0 {
                            votes_in.advance_to(logical_time);
                            votes_in.insert((writes[local_step % writes.len()], 0))
                        } else {
                            // reads_in.advance_to(logical_time);
                            reads_in.send((reads[local_step % reads.len()], RootTimestamp::new(logical_time)));
                            // reads_in.insert(reads[local_step % reads.len()]);
                            // reads_in.advance_to(logical_time + 1);
                            // reads_in.remove(reads[local_step % reads.len()]);
                        }

                        // }
                        // input.advance_to((request_counter * ns_per_request) as u64);
                        // input.insert((rng1.gen_range(0, keys),()));
                        // input.remove((rng2.gen_range(0, keys),()));
                        request_counter += peers;
                    }
                    votes_in.advance_to(target_ns);
                    votes_in.flush();
                    reads_in.advance_to(target_ns);
                    inserted_ns = target_ns;
                }

                worker.step();
            }

            if index == 0 {

                let mut results = Vec::new();
                let total = counts.iter().map(|x| x.iter().sum::<u64>()).sum();
                let mut sum = 0;
                for index in (10 .. counts.len()).rev() {
                    for sub in (0 .. 16).rev() {
                        if sum > 0 && sum < total {
                            let latency = (1 << (index-1)) + (sub << (index-5));
                            let fraction = (sum as f64) / (total as f64);
                            results.push((latency, fraction));
                        }
                        sum += counts[index][sub];
                    }
                }

                println!(
                    "processed {} events in {}s over {} peers => {}",
                    total * peers as u64,
                    dur_to_ns!(start.elapsed()) as f64 / NANOS_PER_SEC as f64,
                    peers,
                    (total * peers as u64) as f64
                        / (dur_to_ns!(start.elapsed()) / NANOS_PER_SEC as f64)
                );

                for (latency, fraction) in results.drain(..).rev() {
                    println!("\t{}\t{}", latency, fraction);
                }
            }
        }
    }).unwrap();

    thread::sleep(time::Duration::from_secs(15));

    println!("Done");
}
