extern crate ansi_term;
extern crate api_soup;
extern crate bson;
extern crate criterion;
extern crate docopt;
#[macro_use]
extern crate log;
extern crate log4rs;
extern crate test;
extern crate time;
extern crate rand;

use ansi_term::Style;
use docopt::Docopt;
use std::collections::HashMap;
use std::process;
use std::thread;
use std::fs::File;
use std::time::Duration;

const USAGE: &'static str = "
Serves soup.

Usage:
  gulaschkanone [-v | --verbose]
  \
                             gulaschkanone (-h | --help)
  gulaschkanone --version

Options:
  -h \
                             --help        Show this screen.
  -v --verbose     Verbose output.
  \
                             --version        Show version.
";

fn main() {
    let args = Docopt::new(USAGE)
                   .and_then(|dopt| dopt.parse())
                   .unwrap_or_else(|e| e.exit());

    println!("Wheeeey");
}
