extern crate abomonation;
extern crate bincode;
extern crate nix;
extern crate differential_dataflow;
extern crate slog;
extern crate slog_term;
extern crate timely;
extern crate timely_communication;
extern crate rand;

mod types;

use std::net::TcpListener;

use differential_dataflow::collection::AsCollection;
use differential_dataflow::operators::*;

use timely::dataflow::operators::{Inspect, Map, Partition};
use timely::dataflow::operators::capture::{EventReader, Replay};
use timely::progress::timestamp::RootTimestamp;

use types::Rpc;

pub struct Config {
    pub articles: usize,
    pub batch: usize,
    pub read_mix: usize,
    pub runtime: Option<u64>,
    pub workers: usize,
    pub cluster_cfg: Option<String>,
    pub pinning: bool,
}

pub struct Eintopf {
    config: Config,
}

#[cfg(target_os = "linux")]
fn pin_to_core(index: usize, stride: usize) {
    let mut cpu_set = ::nix::sched::CpuSet::new();
    let tgt_cpu = index * stride;
    cpu_set.set(tgt_cpu);
    let result = ::nix::sched::sched_setaffinity(::nix::unistd::Pid::this(), &cpu_set);
}
#[cfg(not(target_os = "linux"))]
fn pin_to_core(_index: usize, _stride: usize) {
    println!("core pinning: not linux");
}

impl Eintopf {
    pub fn new(c: Config) -> Self {
        Eintopf { config: c }
    }

    pub fn run(self) {

        println!("Batching: {:?}", self.config.batch);

        let tc = match self.config.cluster_cfg {
            None => timely::Configuration::Process(self.config.workers),
            Some(ref cc) => {
                timely::Configuration::from_args(
                    cc.split(" ")
                        .map(String::from)
                        .collect::<Vec<_>>()
                        .into_iter(),
                ).unwrap()
            }
        };

        // set up the dataflow
        timely::execute(tc, move |worker| {

            let index = worker.index();
            let peers = worker.peers();

            let listener = TcpListener::bind("0.0.0.0:9999").unwrap();

            // yuck. only accepts one client per worker!
            let recv = listener.incoming().next().unwrap().unwrap();

            if self.config.pinning {
                pin_to_core(index, 1);
            }

            // create a a degree counting differential dataflow
            let probe = worker.dataflow::<u64, _, _>(|scope| {

                // demultiplex RPCs
                let streams = EventReader::new(recv).replay_into(scope).partition(4, |x: Rpc| {
                    match x {
                        q @ Rpc::Query { .. } => {
                            println!("got query!");
                            (0, (q, RootTimestamp::new(0), 1 as isize))
                        }
                        i @ Rpc::InsertArticle { .. } => {
                            println!("got insert article!");
                            (1, (i, RootTimestamp::new(0), 1 as isize))
                        }
                        v @ Rpc::InsertVote { .. } => {
                            println!("got insert vote!");
                            (2, (v, RootTimestamp::new(0), 1 as isize))
                        }
                        f @ Rpc::Flush => {
                            println!("got flush!");
                            (3, (f, RootTimestamp::new(0), 1 as isize))
                        }
                    }
                });

                // unwrap elements in streams and build DD collections to work with; unfortunately
                // this is a bit unwieldy at the moment.
                let reads = streams[0].map(|m| match m {
                    (Rpc::Query { ref view, ref key }, ts, d) => (*key as u64, ts, d),
                    _ => unreachable!(),
                }).as_collection();
                let articles = streams[1].map(|a| match a {
                    (Rpc::InsertArticle { ref aid, ref title }, ts, d) => ((*aid as u64, title.clone()), ts, d),
                    _ => unreachable!(),
                }).as_collection();
                let votes = streams[2].map(|a| match a {
                    (Rpc::InsertVote { ref aid, ref uid }, ts, d) => ((*aid, *uid), ts, d),
                    _ => unreachable!(),
                }).as_collection();
                let flushes = streams[3].as_collection();

                // merge votes with articles, to ensure counts for un-voted articles.
                let votes = votes
                    .map(|(aid, _uid): (i64, i64)| aid as u64)
                    .concat(&articles.map(|(aid, _title)| aid as u64));

                // capture artices and votes, restrict by query article ids.
                let probe = articles.semijoin_u(&votes).semijoin_u(&reads).probe();

                probe
            });

        }).unwrap();

        println!("Done");
    }
}
