use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{Stderr, Write},
    path::PathBuf,
    rc::Rc,
};

use clap::Parser;
use node::{Node, NodeConfig, NodeId, NodeState};
use resource::{load_resources, Resource};
use schedule::{Schedule, SerializeableSchedule};
use serde::{Deserialize, Serialize};

use crate::resource::ResourceBuilder;

mod node;
mod resource;
mod schedule;
mod task;

#[derive(Parser)]
struct Args {
    #[arg(short, long, default_value = "/etc/tadaflow")]
    config_dir: String,

    #[arg(short, long, default_value = "/run/tadaflow")]
    state_dir: String,

    #[arg(short, long)]
    logfile: Option<String>,

    #[cfg(feature = "management-interface")]
    #[arg(short, long, default_value = "/var/run/tadaflow")]
    management_socket: String,
}

fn main() {
    let args = Args::parse();

    let mut log = if let Some(filepath) = &args.logfile {
        match std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(filepath)
        {
            Ok(file) => Log::File(file),
            Err(e) => panic!("Error while opening log: {}", e),
        }
    } else {
        Log::Stderr(std::io::stderr())
    };
    let logref = &mut log;

    let (nodes, schedule, connections) = match load(&args, logref) {
        Ok(x) => x,
        Err(e) => panic!("Error while loading: {}", e),
    };
    let lut = build_update_lut(
        &nodes
            .iter()
            .map(|node| (node.config.node_id.clone(), node.clone()))
            .collect(),
        connections,
        logref,
    );

    let resource_builder = ResourceBuilder::new(PathBuf::from(&args.state_dir).join("resources"));

    runtime::run(&args, schedule, lut, resource_builder, nodes, logref);
}

fn load(
    args: &Args,
    log: &mut Log,
) -> Result<(Vec<Rc<Node>>, Schedule, Vec<Connection>), std::io::Error> {
    let configdir = std::path::PathBuf::from(&args.config_dir);
    let config_files = std::fs::read_dir(configdir)?;
    let mut node_configs = Vec::new();
    let mut connections = Vec::new();
    for entry in config_files {
        let entry = entry?;
        if entry.metadata().map(|mtd| mtd.is_file()).unwrap_or(false) && entry.path().extension().map(|ext| ext == "yaml").unwrap_or(false) {
            let file = OpenOptions::new().read(true).open(entry.path())?;
            let file_config: ConfigFile = match serde_yaml::from_reader(file) {
                Ok(configs) => configs,
                Err(e) => {
                    write!(
                        log,
                        "Error while loading config file {}: {}\n",
                        entry.file_name().to_str().unwrap_or("???"),
                        e
                    )
                    .unwrap();

                    ConfigFile {
                        nodes: vec![],
                        connections: vec![],
                    }
                }
            };
            node_configs.extend(file_config.nodes);
            connections.extend(file_config.connections)
        }
    }

    let statedir = std::path::PathBuf::from(&args.state_dir);

    let nodestate_path = statedir.join("nodes.yaml");
    let node_file = OpenOptions::new().read(true).open(nodestate_path);
    let state_map: BTreeMap<String, NodeState> = match node_file {
        Ok(file) => match serde_yaml::from_reader(file) {
            Ok(treemap) => treemap,
            Err(_) => {
                write!(log, "Deserializing node state failed, creating new.\n").unwrap();
                BTreeMap::new()
            }
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            write!(log, "Node state not found, creating new.\n").unwrap();
            BTreeMap::new()
        }
        Err(e) => return Err(e),
    };

    let nodes: Vec<Rc<Node>> = node_configs
        .into_iter()
        .map(|config| {
            let id = config.node_id.clone();
            Rc::new(Node::from_config_and_maybe_state(
                config,
                state_map.get(&id),
            ))
        })
        .collect();

    let resource_dir = statedir.join("resources");
    if !resource_dir.exists() {
        std::fs::create_dir(&resource_dir)?;
    }
    let resources = load_resources(resource_dir)?;
    let mut latest_resource = BTreeMap::<(NodeId, String), Rc<Resource>>::new();
    for resource in &resources {
        let key = (
            resource.metadata.created_by.clone(),
            resource.metadata.name.clone(),
        );
        match latest_resource.get(&key) {
            Some(other) if other.metadata.created_at < resource.metadata.created_at => {
                latest_resource.insert(key, resource.clone());
            }
            None => {
                latest_resource.insert(key, resource.clone());
            }
            _ => {}
        }
    }

    let schedule_path = statedir.join("schedule.yaml");
    let schedule_file = OpenOptions::new().read(true).open(schedule_path);
    let schedule = match schedule_file {
        Ok(file) => match serde_yaml::from_reader::<File, SerializeableSchedule>(file) {
            Ok(schedule) => schedule
                .to_schedule(
                    &nodes
                        .iter()
                        .map(|node| (node.config.node_id.clone(), node.clone()))
                        .collect(),
                    &resources
                        .iter()
                        .map(|resource| (resource.id.clone(), resource.clone()))
                        .collect(),
                    log,
                )
                .schedule_nodes(&nodes, &latest_resource),
            Err(e) => {
                write!(log, "Deserializing schedule failed, creating new: {}\n", e).unwrap();
                Schedule::new().schedule_nodes(&nodes, &latest_resource)
            }
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            write!(log, "Schedule state not found, creating new schedule.\n").unwrap();
            Schedule::new().schedule_nodes(&nodes, &latest_resource)
        }
        Err(e) => {
            write!(log, "Error while opening state file: {}\n", e).unwrap();
            return Err(e);
        }
    };

    Ok((nodes, schedule, connections))
}

fn build_update_lut(
    nodes: &BTreeMap<NodeId, Rc<Node>>,
    connections: Vec<Connection>,
    log: &mut Log,
) -> BTreeMap<(NodeId, String), Vec<(Rc<Node>, String)>> {
    let mut map = BTreeMap::new();
    for connection in connections {
        let key = (connection.from_id.clone(), connection.output.clone());
        let connections = match map.get_mut(&key) {
            Some(x) => x,
            None => {
                map.insert(key.clone(), vec![]);
                map.get_mut(&key).unwrap()
            }
        };
        let Some(node) = nodes.get(&connection.to_id) else {
            write!(
                log,
                "Connection destination node does not exist, skipping {:?}",
                connection
            )
            .unwrap();
            continue;
        };
        if !node.config.has_input(&connection.input) {
            write!(
                log,
                "Connection destination does not have the specified input, skipping {:?}",
                connection
            )
            .unwrap();
            continue;
        }
        connections.push((node.clone(), connection.input))
    }
    map
}

fn store_state(nodes: &Vec<Rc<Node>>, schedule: &Schedule, args: &Args) {
    let statedir = std::path::PathBuf::from(&args.state_dir);
    std::fs::create_dir_all(&statedir).unwrap();

    let schedule_file = {
        let path = statedir.join("schedule.yaml");
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .unwrap()
    };
    serde_yaml::to_writer(schedule_file, &schedule.to_serializeable()).unwrap();

    let node_file = {
        let path = statedir.join("nodes.yaml");
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .unwrap()
    };
    serde_yaml::to_writer(
        node_file,
        &nodes
            .iter()
            .map(|node| node.serializable_state())
            .collect::<BTreeMap<String, NodeState>>(),
    )
    .unwrap();
}

pub enum Log {
    Stderr(Stderr),
    File(File),
}

impl Write for Log {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Log::File(file) => file.write(buf),
            Log::Stderr(stderr) => stderr.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Log::File(file) => file.flush(),
            Log::Stderr(stderr) => stderr.flush(),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct ConfigFile {
    pub nodes: Vec<NodeConfig>,
    pub connections: Vec<Connection>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Connection {
    pub from_id: String,
    pub output: String,
    pub to_id: String,
    pub input: String,
}

#[cfg(not(feature = "management-interface"))]
mod runtime {
    use std::{collections::BTreeMap, rc::Rc, thread, time::SystemTime};

    use crate::{
        node::Node, resource::ResourceBuilder, schedule::Schedule, store_state, Args, Log,
    };

    pub fn run(
        args: &Args,
        mut schedule: Schedule,
        lut: BTreeMap<(String, String), Vec<(Rc<Node>, String)>>,
        resource_builder: ResourceBuilder,
        nodes: Vec<Rc<Node>>,
        logref: &mut Log,
    ) {
        writeln!("\n[INFO] Running schedule.").unwrap();
        loop {
            if let Some(execution_time) = schedule.next_scheduled_task() {
                while let Ok(duration) = execution_time.duration_since(SystemTime::now()) {
                    thread::sleep(duration);
                }
                schedule = schedule.execute(&lut, &resource_builder, logref);
                store_state(&nodes, &schedule, &args);
            } else {
                break;
            }
        }
    }
}

#[cfg(feature = "management-interface")]
mod runtime {

    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::net::UnixStream;
    use std::rc::Rc;
    use std::sync::mpsc::Receiver;
    use std::thread;
    use std::{
        collections::BTreeMap,
        os::fd::AsFd,
        path::PathBuf,
        sync::{
            mpsc::{channel, Sender},
            Arc, Mutex,
        },
        thread::JoinHandle,
        time::SystemTime,
    };

    use nix::poll::{poll, PollFd, PollFlags, PollTimeout};

    use crate::node::Node;
    use crate::resource::ResourceBuilder;
    use crate::store_state;
    use crate::{schedule::Schedule, Args, Log};

    pub fn run(
        args: &Args,
        mut schedule: Schedule,
        lut: BTreeMap<(String, String), Vec<(Rc<Node>, String)>>,
        resource_builder: ResourceBuilder,
        nodes: Vec<Rc<Node>>,
        logref: &mut Log,
    ) {
        let management_socket_path = PathBuf::from(&args.management_socket);
        let _ = std::fs::remove_file(&management_socket_path);
        let socket = match std::os::unix::net::UnixListener::bind(management_socket_path) {
            Ok(s) => s,
            Err(e) => panic!("Unable to bind to management socket: {}", e.to_string()),
        };
        let mut connections = BTreeMap::<u64, (JoinHandle<()>, Sender<String>)>::new();
        let (tx, rx) = channel::<ManagementMessage>();
        let sender = Arc::new(Mutex::new(tx));
        let mut current_conn_id = 0;

        macro_rules! log {
            ($($arg:tt)*) => {
                writeln!(logref, $($arg)*).unwrap()
            };
        }

        log!("\n[INFO] Running with management console.");

        loop {
            if let Some(execution_time) = schedule.next_scheduled_task() {
                while let Ok(duration) = execution_time.duration_since(SystemTime::now()) {
                    while let Ok(message) = rx.try_recv() {
                        handle_input(message, &mut schedule, &mut connections);
                    }

                    // FIXME: 28 days should be enough, but it might fail.
                    let timeout = PollTimeout::try_from(duration.as_millis()).unwrap();
                    let poll_fd = PollFd::new(socket.as_fd(), PollFlags::POLLIN);
                    match poll(&mut [poll_fd], timeout) {
                        Ok(0) => {}
                        Ok(_) => {
                            let Ok(connection) = socket.accept() else {
                                log!("Management connection failed.");
                                continue;
                            };
                            let sender_clone = sender.clone();
                            let (tx, rx) = channel();
                            connections.insert(
                                current_conn_id,
                                (
                                    thread::spawn(move || {
                                        listen(sender_clone, current_conn_id, connection.0, rx)
                                    }),
                                    tx,
                                ),
                            );
                            current_conn_id += 1;
                        }
                        Err(errno) => {
                            log!("Error while polling file: {}", errno);
                        }
                    }
                }
                schedule = schedule.execute(&lut, &resource_builder, logref);
                store_state(&nodes, &schedule, &args);
            }
            while let Ok(message) = rx.try_recv() {
                handle_input(message, &mut schedule, &mut connections)
            }
        }
    }

    struct ManagementMessage {
        id: u64,
        request: ManagementRequest,
    }

    enum ManagementRequest {
        Close,
        Trigger(String),
    }

    fn handle_input(
        message: ManagementMessage,
        schedule: &mut Schedule,
        connections: &mut BTreeMap<u64, (JoinHandle<()>, Sender<String>)>,
    ) {
        match message.request {
            ManagementRequest::Close => {
                if let Some(connection) = connections.remove(&message.id) {
                    let _ = connection.0.join();
                }
            }
            ManagementRequest::Trigger(nodeid) => {
                if let Err(e) = schedule.trigger(&nodeid) {
                    let _ = connections
                        .get_mut(&message.id)
                        .map(|(_, sender)| sender.send(e));
                } else {
                    let _ = connections.get_mut(&message.id).map(|(_, sender)| {
                        sender.send(format!("Succesfully scheduled {}", nodeid))
                    });
                }
            }
        }
    }

    fn listen(
        sender: Arc<Mutex<Sender<ManagementMessage>>>,
        id: u64,
        mut stream: UnixStream,
        response: Receiver<String>,
    ) {
        let mut reader = BufReader::new(stream.try_clone().expect("Unable to clone handle..."));
        let mut line = String::new();
        while let Ok(_) = reader.read_line(&mut line) {
            if line.starts_with("trigger") {
                let Some(node_id) = line.split_once(' ').map(|(_, node)| node.trim().to_owned())
                else {
                    continue;
                };
                let Ok(_) = sender.lock().unwrap().send(ManagementMessage {
                    id,
                    request: ManagementRequest::Trigger(node_id),
                }) else {
                    break;
                };

                let _ = stream.write_all(
                    response
                        .recv()
                        .unwrap_or("No response?".to_owned())
                        .as_bytes(),
                );
            }
        }

        sender
            .lock()
            .unwrap()
            .send(ManagementMessage {
                id,
                request: ManagementRequest::Close,
            })
            .unwrap();
    }
}
