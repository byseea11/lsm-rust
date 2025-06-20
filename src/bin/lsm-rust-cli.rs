mod wrapper;

use lsmxzy::compact::{CompactionOptions, SimpleLeveledCompactionOptions};
use rustyline::DefaultEditor;
use wrapper::mini_lsm_wrapper;

use anyhow::Result;
use bytes::Bytes;
use clap::{Parser, ValueEnum};
use mini_lsm_wrapper::iterators::StorageIterator;
use mini_lsm_wrapper::lsm_storage::{LsmStorageOptions, MiniLsm};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Clone, ValueEnum)]
enum CompactionStrategy {
    Simple,
    None,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "lsm.db")]
    path: PathBuf,
    #[arg(long, default_value = "None")]
    compaction: CompactionStrategy,
    #[arg(long)]
    enable_wal: bool,
}

struct ReplHandler {
    epoch: u64,
    lsm: Arc<MiniLsm>,
}

impl ReplHandler {
    fn handle(&mut self, command: &Command) -> Result<()> {
        match command {
            Command::Fill { begin, end } => {
                for i in *begin..=*end {
                    self.lsm.put(
                        format!("{}", i).as_bytes(),
                        format!("value{}@{}", i, self.epoch).as_bytes(),
                    )?;
                }

                println!(
                    "{} values filled with epoch {}",
                    end - begin + 1,
                    self.epoch
                );
            }
            Command::Del { key } => {
                self.lsm.delete(key.as_bytes())?;
                println!("{} deleted", key);
            }
            Command::Update { key, value } => {
                if let Some(old_value) = self.lsm.get(key.as_bytes())? {
                    self.lsm.put(key.as_bytes(), value.as_bytes())?;
                    println!(
                        "key: {}, before update: {:?}, update: {:?}",
                        key, old_value, value
                    );
                } else {
                    println!("{} not exist", key);
                }
            }
            Command::Get { key } => {
                if let Some(value) = self.lsm.get(key.as_bytes())? {
                    println!("{}={:?}", key, value);
                } else {
                    println!("{} not exist", key);
                }
            }
            Command::Scan { begin, end } => match (begin, end) {
                (None, None) => {
                    let mut iter = self
                        .lsm
                        .scan(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)?;
                    let mut cnt = 0;
                    while iter.is_valid() {
                        println!(
                            "{:?}={:?}",
                            Bytes::copy_from_slice(iter.key()),
                            Bytes::copy_from_slice(iter.value()),
                        );
                        iter.next()?;
                        cnt += 1;
                    }
                    println!();
                    println!("{} keys scanned", cnt);
                }
                (Some(begin), Some(end)) => {
                    let mut iter = self.lsm.scan(
                        std::ops::Bound::Included(begin.as_bytes()),
                        std::ops::Bound::Included(end.as_bytes()),
                    )?;
                    let mut cnt = 0;
                    while iter.is_valid() {
                        println!(
                            "{:?}={:?}",
                            Bytes::copy_from_slice(iter.key()),
                            Bytes::copy_from_slice(iter.value()),
                        );
                        iter.next()?;
                        cnt += 1;
                    }
                    println!();
                    println!("{} keys scanned", cnt);
                }
                _ => {
                    println!("invalid command");
                }
            },
            Command::Flush => {
                self.lsm.force_flush()?;
                println!("flush success");
            }
            Command::Quit | Command::Close => {
                self.lsm.close()?;
                std::process::exit(0);
            }
        };

        self.epoch += 1;

        Ok(())
    }
}

#[derive(Debug)]
enum Command {
    Fill {
        begin: u64,
        end: u64,
    },
    Del {
        key: String,
    },
    Get {
        key: String,
    },
    Scan {
        begin: Option<String>,
        end: Option<String>,
    },
    Update {
        key: String,
        value: String,
    },
    Flush,
    Quit,
    Close,
}

impl Command {
    pub fn parse(input: &str) -> Result<Self> {
        use nom::bytes::complete::*;
        use nom::character::complete::*;

        use nom::branch::*;
        use nom::combinator::*;
        use nom::sequence::*;

        let uint = |i| {
            map_res(digit1::<&str, nom::error::Error<_>>, |s: &str| {
                s.parse()
                    .map_err(|_| nom::error::Error::new(s, nom::error::ErrorKind::Digit))
            })(i)
        };

        let string = |i| {
            map(take_till1(|c: char| c.is_whitespace()), |s: &str| {
                s.to_string()
            })(i)
        };

        let fill = |i| {
            map(
                tuple((tag_no_case("fill"), space1, uint, space1, uint)),
                |(_, _, key, _, value)| Command::Fill {
                    begin: key,
                    end: value,
                },
            )(i)
        };

        let del = |i| {
            map(
                tuple((tag_no_case("del"), space1, string)),
                |(_, _, key)| Command::Del { key },
            )(i)
        };

        let update = |i| {
            map(
                tuple((tag_no_case("update"), space1, string, space1, string)),
                |(_, _, key, _, value)| Command::Update { key, value },
            )(i)
        };

        let get = |i| {
            map(
                tuple((tag_no_case("get"), space1, string)),
                |(_, _, key)| Command::Get { key },
            )(i)
        };

        let scan = |i| {
            map(
                tuple((
                    tag_no_case("scan"),
                    opt(tuple((space1, string, space1, string))),
                )),
                |(_, opt_args)| {
                    let (begin, end) = opt_args
                        .map_or((None, None), |(_, begin, _, end)| (Some(begin), Some(end)));
                    Command::Scan { begin, end }
                },
            )(i)
        };

        let command = |i| {
            alt((
                fill,
                del,
                get,
                scan,
                update,
                map(tag_no_case("flush"), |_| Command::Flush),
                map(tag_no_case("quit"), |_| Command::Quit),
                map(tag_no_case("close"), |_| Command::Close),
            ))(i)
        };

        command(input)
            .map(|(_, c)| c)
            .map_err(|e| anyhow::anyhow!("{}", e))
    }
}

struct Repl {
    app_name: String,
    description: String,
    prompt: String,

    handler: ReplHandler,

    editor: DefaultEditor,
}

impl Repl {
    pub fn run(mut self) -> Result<()> {
        self.bootstrap()?;

        loop {
            let readline = self.editor.readline(&self.prompt)?;
            if readline.trim().is_empty() {
                // Skip noop
                continue;
            }
            let command = Command::parse(&readline)?;
            self.handler.handle(&command)?;
            self.editor.add_history_entry(readline)?;
        }
    }

    fn bootstrap(&mut self) -> Result<()> {
        println!("Welcome to {}!", self.app_name);
        println!("{}", self.description);
        println!();
        Ok(())
    }
}

struct ReplBuilder {
    app_name: String,
    description: String,
    prompt: String,
}

impl ReplBuilder {
    pub fn new() -> Self {
        Self {
            app_name: "LSM-RUST".to_string(),
            description: "A CLI for lsm-rust".to_string(),
            prompt: "lsm-rust-cli> ".to_string(),
        }
    }

    pub fn app_name(mut self, app_name: &str) -> Self {
        self.app_name = app_name.to_string();
        self
    }

    pub fn description(mut self, description: &str) -> Self {
        self.description = description.to_string();
        self
    }

    pub fn prompt(mut self, prompt: &str) -> Self {
        self.prompt = prompt.to_string();
        self
    }

    pub fn build(self, handler: ReplHandler) -> Result<Repl> {
        Ok(Repl {
            app_name: self.app_name,
            description: self.description,
            prompt: self.prompt,
            editor: DefaultEditor::new()?,
            handler,
        })
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let lsm = MiniLsm::open(
        args.path,
        LsmStorageOptions {
            block_size: 4096,
            target_sst_size: 2 << 20, // 2MB
            num_memtable_limit: 3,
            enable_wal: args.enable_wal,
            compaction_options: match args.compaction {
                CompactionStrategy::None => CompactionOptions::NoCompaction,
                CompactionStrategy::Simple => {
                    CompactionOptions::Simple(SimpleLeveledCompactionOptions {
                        size_ratio_percent: 200,
                        level0_file_num_compaction_trigger: 2,
                        max_levels: 4,
                    })
                }
            },
        },
    )?;

    let repl = ReplBuilder::new()
        .app_name("lsm-rust-cli")
        .description("A CLI for lsm-rust")
        .prompt("lsm-rust-cli> ")
        .build(ReplHandler { epoch: 0, lsm })?;

    repl.run()?;
    Ok(())
}
