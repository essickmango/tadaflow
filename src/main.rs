use std::{collections::BTreeMap, fs::{File, OpenOptions}, io::{Stderr, Write}, os::fd::AsFd, rc::Rc, time::SystemTime};

use clap::Parser;
use nix::{poll::{poll, PollFd, PollFlags, PollTimeout}, sys::stat::Mode};
use node::{Node, NodeConfig, NodeState};
use schedule::{Schedule, SerializeableSchedule};

mod node;
mod schedule;

const MANAGEMENT_FIFO_PATH: &str = "/var/run/tadaflow";

#[derive(Parser)]
struct Args {
    #[arg(short, long, default_value = "/etc/tadaflow")]
    config_dir: String,

    #[arg(short, long, default_value = "/run/tadaflow")]
    state_dir: String,

    #[arg(short, long)]
    logfile: Option<String>,
}

fn main() {
    let args = Args::parse();
    // if std::fs::metadata(MANAGEMENT_FIFO_PATH).map(|mtd| !mtd.is_file()).unwrap_or(true) {
    //     nix::unistd::mkfifo(MANAGEMENT_FIFO_PATH, Mode::S_IRWXU).unwrap();
    // }
    // let mut listener = std::fs::OpenOptions::new().read(true).open(MANAGEMENT_FIFO_PATH).unwrap();
    // let poll_clone = listener.try_clone().unwrap();
    // let poll_fd = PollFd::new(poll_clone.as_fd(), PollFlags::POLLIN);

    let mut log = if let Some(filepath) = &args.logfile {
        match std::fs::OpenOptions::new().create(true).write(true).truncate(true).open(filepath) {
            Ok(file) => Log::File(file),
            Err(e) => panic!("Error while opening log: {}", e) 
        }
    } else {
        Log::Stderr(std::io::stderr())
    };

    let (nodes, mut schedule) = match load(&args, &mut log) {
        Ok(x) => x,
        Err(e) => panic!("Error while loading: {}", e)
    };
    let lut = build_filename_changed_lut(&nodes);

    macro_rules! log {
        ($($arg:tt)*) => {
            write!(log, $($arg)*).unwrap()
        };
    }

    loop {
        if let Some(execution_time) = schedule.next_scheduled_event() {
            while let Ok(duration) = execution_time.duration_since(SystemTime::now()) {
                // FIXME: 28 days should be enough, but it might fail.
                let timeout = PollTimeout::try_from(duration.as_millis()).unwrap();
                match poll(&mut [], timeout) {
                    Ok(0) => {},
                    // Ok(_) => handle_input(&mut listener, &mut schedule),
                    Ok(_) => {}
                    Err(errno) => {log!("Error while polling file: {}\n", errno);},
                }
            }
            schedule = schedule.execute(&lut, &mut log);
            store_state(&nodes, &schedule, &args);
        }
        else {
            // handle_input(&mut listener, &mut schedule)
        }
        
    }
}


fn load(args: &Args, log: &mut Log) -> Result<(Vec<Rc<Node>>, Schedule), std::io::Error> {
    let configdir = std::path::PathBuf::from(&args.config_dir);
    let config_files = std::fs::read_dir(configdir)?;
    let mut node_configs = Vec::new();
    for entry in config_files {
        let entry = entry?;
        if entry.metadata().map(|mtd| mtd.is_file()).unwrap_or(false) {
            let file = OpenOptions::new().read(true).open(entry.path())?;
            let file_configs: Vec<NodeConfig> = match serde_yaml::from_reader(file) {
                Ok(configs) => configs,
                Err(e) => {
                    write!(log, "Error while loading config file {}: {}\n", entry.file_name().to_str().unwrap_or("???"), e).unwrap();
                    vec![]
                }
            };
            node_configs.extend(file_configs);
        }
    }

    let statedir = std::path::PathBuf::from(&args.state_dir);

    let nodestate_path = statedir.join("nodes.yaml");
    let node_file = OpenOptions::new().read(true).open(nodestate_path);
    let state_map: BTreeMap<String, NodeState> = match node_file {
        Ok(file) => {
            match serde_yaml::from_reader(file) {
                Ok(treemap) => treemap,
                Err(_) => {
                    write!(log, "Deserializing node state failed, creating new.\n").unwrap();
                    BTreeMap::new()
                }
            }
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            write!(log, "Node state not found, creating new.\n").unwrap();
            BTreeMap::new()
        }
        Err(e) => return Err(e)
    };

    let nodes: Vec<Rc<Node>> = node_configs
        .into_iter()
        .map(|config| {
            let id = config.node_id.clone();
            Rc::new(Node::from_config_and_maybe_state(config, state_map.get(&id)))
        }).collect();

    let schedule_path = statedir.join("schedule.yaml");
    let schedule_file = OpenOptions::new().read(true).open(schedule_path);
    let schedule = match schedule_file {
        Ok(file) => {
            match serde_yaml::from_reader::<File, SerializeableSchedule>(file) {
                Ok(schedule) => schedule
                                    .to_schedule(&nodes.iter().map(|node| (node.config.node_id.clone(), node.clone())).collect())
                                    .merge(Schedule::for_nodes(&nodes, true)),
                Err(e) => {
                    write!(log, "Deserializing schedule failed, creating new: {}\n", e).unwrap();
                    Schedule::for_nodes(&nodes, true)
                }
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            write!(log, "Schedule state not found, creating new schedule.\n").unwrap();
            Schedule::for_nodes(&nodes, true)
        },
        Err(e) => {
            write!(log, "Error while opening state file: {}\n", e).unwrap();
            return Err(e)
        }
    };

    Ok((nodes, schedule))
}

fn build_filename_changed_lut(nodes: &[Rc<Node>]) -> BTreeMap<String, Vec<Rc<Node>>> {
    let mut map: BTreeMap<String, Vec<Rc<Node>>> = BTreeMap::new();
    for node in nodes {
        for filename in node.config.get_input_files() {
            if let Some(listening_nodes) = map.get_mut(filename) {
                listening_nodes.push(node.clone());
            } else {
                map.insert(filename.to_owned(), vec![node.clone()]);
            }
        }
    }
    map
}

fn store_state(nodes: &Vec<Rc<Node>>, schedule: &Schedule, args: &Args) {
    let statedir = std::path::PathBuf::from(&args.state_dir);
    std::fs::create_dir_all(&statedir).unwrap();
    
    let schedule_file = {
        let path = statedir.join("schedule.yaml");
        OpenOptions::new().write(true).create(true).truncate(true).open(path).unwrap()
    };
    serde_yaml::to_writer(schedule_file, &schedule.serializable()).unwrap();

    let node_file = {
        let path = statedir.join("nodes.yaml");
        OpenOptions::new().write(true).create(true).truncate(true).open(path).unwrap()
    };
    serde_yaml::to_writer(node_file, &nodes.iter().map(|node| node.serializable_state()).collect::<BTreeMap<String, NodeState>>()).unwrap();
}


fn handle_input(listener: &mut File, schedule: &mut Schedule) {
    // TODO: allow management through pipe.
}


pub enum Log {
    Stderr(Stderr),
    File(File)
}

impl Write for Log {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Log::File(file) => file.write(buf),
            Log::Stderr(stderr) => stderr.write(buf)
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Log::File(file) => file.flush(),
            Log::Stderr(stderr) => stderr.flush()
        }
    }
}

