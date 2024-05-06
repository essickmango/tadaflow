use std::{
    cell::RefCell,
    fs::OpenOptions,
    hash::Hash,
    io::{Read, Seek},
    process::Stdio,
    time::SystemTime,
};

use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};

pub(crate) type Filename = String;

#[derive(Serialize, Deserialize)]
pub struct NodeConfig {
    pub node_id: String,
    program: String,
    arguments: Vec<String>,
    outputs: Vec<NodeOutputConfig>,
    stdin: Option<Filename>,
    pub typ: NodeType,
    pub error_handling: NodeErrorConfig,
}

impl NodeConfig {
    pub fn get_input_files(&self) -> &[Filename] {
        match &self.typ {
            NodeType::OnFileUpdated(files) => files,
            NodeType::Scheduled { .. } => &[],
        }
    }
}

#[derive(Serialize, Deserialize)]
pub enum NodeType {
    Scheduled { schedule: NodeSchedule },
    OnFileUpdated(Vec<Filename>),
}

#[derive(Serialize, Deserialize)]
pub enum NodeSchedule {
    Startup { delay_ms: u64 },
    Repeated { interval_ms: u64 },
    Daily,
    Manual,
}

#[derive(Serialize, Deserialize)]
pub enum NodeTrigger {
    /// Trigger only when inputs changed.
    OnChange,
    /// Trigger whenever inputs updated.
    OnUpdate,
}

#[derive(Serialize, Deserialize)]
enum NodeOutputConfig {
    File { name: Filename },
    Stdout { destination: Filename },
    Stderr { destination: Filename },
}

impl NodeOutputConfig {
    fn output_filename(&self) -> Filename {
        match self {
            NodeOutputConfig::File { name } => name.clone(),
            NodeOutputConfig::Stderr { destination } => destination.clone(),
            NodeOutputConfig::Stdout { destination } => destination.clone(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct NodeErrorConfig {
    handling: NodeErrorHandling,
    error_source: NodeErrorSource,
}

#[derive(Serialize, Deserialize, Clone, Copy)]
pub enum NodeErrorHandling {
    Fail,
    Retry { delay_ms: u64 },
}

#[derive(Serialize, Deserialize)]
pub enum NodeErrorSource {
    Silent,
    Stderr,
    Logfile(Filename),
}

pub struct Node {
    pub config: NodeConfig,
    pub state: RefCell<NodeState>,
}

impl Node {
    pub fn execute(&self) -> Result<Vec<Filename>, NodeError> {
        let error_handling = self.config.error_handling.handling;
        self.state.borrow_mut().last_executed = SystemTime::now().into();

        let stdin = if let Some(path) = &self.config.stdin {
            match OpenOptions::new().read(true).open(path) {
                Ok(file) => Stdio::from(file),
                Err(e) => {
                    return Err(NodeError {
                        handling: error_handling,
                        error: Some(e.to_string()),
                    })
                }
            }
        } else {
            Stdio::null()
        };

        let stdout_path = self.config.outputs.iter().find_map(|output| {
            if let NodeOutputConfig::Stdout { destination } = output {
                Some(destination)
            } else {
                None
            }
        });
        let stdout = if let Some(path) = stdout_path {
            match OpenOptions::new().write(true).create(true).truncate(true).open(path) {
                Ok(file) => Stdio::from(file),
                Err(e) => {
                    return Err(NodeError {
                        handling: error_handling,
                        error: Some(e.to_string()),
                    })
                }
            }
        } else {
            Stdio::null()
        };

        let (stderr, stderr_file) =
            if let NodeErrorSource::Stderr = self.config.error_handling.error_source {
                let tmpdir = std::env::temp_dir();
                match OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .read(true)
                    .open(tmpdir.join("tadaflow_node_stderr"))
                {
                    Ok(file) => {
                        let clone = file.try_clone().ok();
                        (Stdio::from(file), clone)
                    }
                    Err(e) => {
                        return Err(NodeError {
                            handling: error_handling,
                            error: Some(e.to_string()),
                        })
                    }
                }
            } else {
                (Stdio::null(), None)
            };

        let mut child = std::process::Command::new(self.config.program.clone())
            .args(self.config.arguments.clone())
            .stdin(stdin)
            .stdout(stdout)
            .stderr(stderr)
            .spawn()
            .map_err(|e| NodeError {
                handling: error_handling,
                error: Some(e.to_string()),
            })?;

        let status = child.wait().map_err(|e| NodeError {
            handling: error_handling,
            error: Some(e.to_string()),
        })?;
        if status.success() {
            Ok(self
                .config
                .outputs
                .iter()
                .map(|o| o.output_filename())
                .collect())
        } else {
            Err(NodeError {
                handling: error_handling,
                error: match &self.config.error_handling.error_source {
                    NodeErrorSource::Silent => None,
                    NodeErrorSource::Stderr => Some(
                        stderr_file
                            .map(|mut file| {
                                file.seek(std::io::SeekFrom::Start(0)).unwrap();
                                let mut res = String::new();
                                file.read_to_string(&mut res).unwrap();
                                res
                            })
                            .unwrap_or("Unable to get error - stderr not found.".into()),
                    ),
                    NodeErrorSource::Logfile(filename) => {
                        match OpenOptions::new().read(true).open(filename) {
                            Ok(mut file) => {
                                let mut res = String::new();
                                file.read_to_string(&mut res).unwrap();
                                Some(res)
                            }
                            Err(e) => Some(format!(
                                "Unable to get error from log file: {}",
                                e.to_string()
                            )),
                        }
                    }
                },
            })
        }
    }

    pub fn serializable_state(&self) -> (String, NodeState) {
        (self.config.node_id.clone(), self.state.borrow().clone())
    }

    pub fn from_config_and_maybe_state(config: NodeConfig, state: Option<&NodeState>) -> Node {
        Node {
            config,
            state: RefCell::new(NodeState {
                last_executed: state
                    .map(|s| s.last_executed.into())
                    .unwrap_or(SystemTime::UNIX_EPOCH.into()),
            }),
        }
    }

    pub fn should_be_scheduled(&self, is_startup: bool) -> Option<SystemTime> {
        match self.config.typ {
            NodeType::OnFileUpdated(_) => None,
            NodeType::Scheduled {
                schedule: NodeSchedule::Manual,
            } => None,
            NodeType::Scheduled {
                schedule: NodeSchedule::Daily,
            } => {
                if self.state.borrow().last_executed.date_naive()
                    < chrono::Local::now().date_naive()
                {
                    Some(SystemTime::now())
                } else {
                    Some((self.state.borrow().last_executed + chrono::Duration::days(1)).with_time(chrono::NaiveTime::MIN).unwrap().into())
                }
            }
            NodeType::Scheduled {
                schedule: NodeSchedule::Startup { delay_ms },
            } => {
                if is_startup {
                    Some(SystemTime::now() + std::time::Duration::from_millis(delay_ms))
                } else {
                    None
                }
            }
            NodeType::Scheduled {
                schedule: NodeSchedule::Repeated { interval_ms },
            } => Some(SystemTime::now() + std::time::Duration::from_millis(interval_ms)),
        }
    }
}

impl Hash for Node {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.config.node_id.hash(state);
        self.config.program.hash(state);
        self.config.arguments.hash(state);
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.config.node_id == other.config.node_id
            && self.config.program == other.config.program
            && self.config.arguments == other.config.arguments
    }
}

impl Eq for Node {}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct NodeState {
    pub last_executed: DateTime<Local>,
}

pub struct NodeError {
    pub handling: NodeErrorHandling,
    pub error: Option<String>,
}

#[test]
fn write_test_config() {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open("test/config/test.yaml")
        .unwrap();
    serde_yaml::to_writer(
        file,
        &vec![
            NodeConfig {
                node_id: "testnode".into(),
                program: "echo".into(),
                arguments: vec!["world".into()],
                outputs: vec![NodeOutputConfig::Stdout {
                    destination: "testnode.out".into(),
                }],
                stdin: None,
                typ: NodeType::Scheduled {
                    schedule: NodeSchedule::Startup { delay_ms: 0 },
                },
                error_handling: NodeErrorConfig {
                    handling: NodeErrorHandling::Fail,
                    error_source: NodeErrorSource::Stderr,
                },
            },
            NodeConfig {
                node_id: "deleteout".into(),
                program: "rm".into(),
                arguments: vec!["testnode.out".into()],
                outputs: vec![],
                stdin: None,
                typ: NodeType::OnFileUpdated(vec!["testnode.out".into()]),
                error_handling: NodeErrorConfig {
                    handling: NodeErrorHandling::Retry { delay_ms: 1000 },
                    error_source: NodeErrorSource::Stderr
                },
            },
            NodeConfig {
                node_id: "fail".into(),
                program: "test/fail.sh".into(),
                arguments: vec![],
                outputs: vec![],
                stdin: None,
                typ: NodeType::Scheduled { schedule: NodeSchedule::Repeated { interval_ms: 60000 } },
                error_handling: NodeErrorConfig {
                    handling: NodeErrorHandling::Retry { delay_ms: 1000 },
                    error_source: NodeErrorSource::Stderr
                },
            },
        ],
    )
    .unwrap();
}
