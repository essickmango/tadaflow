use std::{
    cell::RefCell,
    collections::BTreeMap,
    fs::OpenOptions,
    hash::Hash,
    path::PathBuf,
    process::{Command, Stdio},
    rc::Rc,
    time::SystemTime,
};

use chrono::{DateTime, Duration, Local, NaiveTime};
use serde::{Deserialize, Serialize};

use crate::resource::{Resource, ResourceBuilder};

pub(crate) type Filename = String;
pub type NodeId = String;

#[derive(Serialize, Deserialize)]
pub struct NodeConfig {
    pub node_id: NodeId,
    program: String,
    arguments: Vec<String>,
    inputs: BTreeMap<String, NodeInputConfig>,
    outputs: BTreeMap<String, NodeOutputConfig>,
    pub typ: NodeType,
    pub error_handling: NodeErrorConfig,
}

impl NodeConfig {
    pub fn get_input_names(&self) -> impl Iterator<Item = &String> {
        self.inputs.keys()
    }

    pub fn has_input(&self, input: &str) -> bool {
        self.inputs.contains_key(input)
    }
}

#[derive(Serialize, Deserialize)]
pub enum NodeType {
    Scheduled { schedule: NodeSchedule },
    OnInput,
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

#[derive(Serialize, Deserialize, PartialEq, Eq)]
enum NodeOutputConfig {
    File { path: Filename },
    Stdout,
    Stderr,
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
enum NodeInputConfig {
    Stdin,
    FilenameArg { placeholder: String },
}

#[derive(Serialize, Deserialize)]
pub struct NodeErrorConfig {
    handling: NodeErrorHandling,
    error_log: NodeErrorLog,
}

#[derive(Serialize, Deserialize, Clone, Copy)]
pub enum NodeErrorHandling {
    Fail,
    Retry { delay_ms: u64 },
}

#[derive(Serialize, Deserialize)]
pub enum NodeErrorLog {
    Silent,
    Stderr,
    Logfile { path: Filename },
}

pub struct Node {
    pub config: NodeConfig,
    pub state: RefCell<NodeState>,
}

impl Node {
    pub fn execute(
        &self,
        inputs: BTreeMap<String, Rc<Resource>>,
        resource_builder: &ResourceBuilder,
    ) -> Result<BTreeMap<(NodeId, String), Rc<Resource>>, NodeError> {
        self.state.borrow_mut().last_executed = Local::now();

        let new_args: Vec<String> = self
            .config
            .arguments
            .iter()
            .cloned()
            .map(|mut argument| {
                for (input, config) in &self.config.inputs {
                    if let NodeInputConfig::FilenameArg { placeholder } = config {
                        argument = argument.replace(
                            placeholder,
                            inputs.get(input).unwrap().path.to_str().unwrap(),
                        );
                    }
                }
                argument
            })
            .collect();

        let stdin = if let Some((input, _)) = self
            .config
            .inputs
            .iter()
            .find(|(_, config)| **config == NodeInputConfig::Stdin)
        {
            let file = OpenOptions::new()
                .read(true)
                .open(&inputs.get(input.as_str()).unwrap().path)
                .map_err(|e| {
                    NodeError::new_unconditional(
                        self,
                        format!(
                            "Error opening resource for stdin while running {}: {}",
                            self.config.node_id,
                            e.to_string()
                        ),
                    )
                })?;
            Stdio::from(file)
        } else {
            Stdio::null()
        };

        let (stdout, stdout_resource) = {
            let name = self.config.outputs.iter().find_map(|(output, config)| if *config == NodeOutputConfig::Stdout { Some(output.clone()) } else { None }).unwrap_or("__temp_stdout".into());
            let resource = resource_builder.new_resource(&self.config.node_id, &name).map_err(|e| NodeError::new_unconditional(self, format!("Error while creating (temporary) stdout resource for {}: {}", self.config.node_id, e)))?;
            let file = OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&resource.path)
                .map_err(|e| {
                    NodeError::new_unconditional(
                        self,
                        format!(
                            "Error opening tempfile for stdout while running {}: {}",
                            self.config.node_id,
                            e.to_string()
                        ),
                    )
                })?;
            (Stdio::from(file), resource)
        };

        let (stderr, stderr_resource) = {
            let name = self.config.outputs.iter().find_map(|(output, config)| if *config == NodeOutputConfig::Stdout { Some(output.clone()) } else { None }).unwrap_or("__temp_stderr".into());
            let resource = resource_builder.new_resource(&self.config.node_id, &name).map_err(|e| NodeError::new_unconditional(self, format!("Error while creating (temporary) stderr resource for {}: {}", self.config.node_id, e)))?;
            let file = OpenOptions::new()
                .write(true)
                .truncate(true)
                .create(true)
                .open(&resource.path)
                .map_err(|e| {
                    NodeError::new_unconditional(
                        self,
                        format!(
                            "Error opening tempfile for stderr while running {}: {}",
                            self.config.node_id,
                            e.to_string()
                        ),
                    )
                })?;
            (Stdio::from(file), resource)
        };

        let status = Command::new(&self.config.program)
            .args(&new_args)
            .stdin(stdin)
            .stdout(stdout)
            .stderr(stderr)
            .status()
            .map_err(|e| {
                NodeError::new_unconditional(
                    self,
                    format!(
                        "Error while executing task for {}: {}",
                        self.config.node_id,
                        e.to_string()
                    ),
                )
            })?;

        if !status.success() {
            let error = match self.config.error_handling.error_log {
                NodeErrorLog::Silent => None,
                NodeErrorLog::Stderr => {
                    Some(std::fs::read_to_string(&stderr_resource.path).map_err(|e| {
                        NodeError::new_unconditional(
                            self,
                            format!(
                                "Error while reading error from stderr for task {}: {}",
                                self.config.node_id,
                                e.to_string()
                            ),
                        )
                    })?)
                }
                NodeErrorLog::Logfile { ref path } => {
                    Some(std::fs::read_to_string(path).map_err(|e| {
                        NodeError::new_unconditional(
                            self,
                            format!(
                                "Error while reading error from log file {} for task {}: {}",
                                path,
                                self.config.node_id,
                                e.to_string()
                            ),
                        )
                    })?)
                }
            };
            return Err(NodeError {
                handling: self.config.error_handling.handling,
                error,
            });
        }

        self.config
            .outputs
            .iter()
            .fold(
                Ok(BTreeMap::new()),
                |state: Result<BTreeMap<(NodeId, String), Rc<Resource>>, String>,
                 (output, config)| {
                    let mut state = state?;
                    let resource = match config {
                        NodeOutputConfig::Stdout => stdout_resource.clone(),
                        NodeOutputConfig::Stderr => stderr_resource.clone(),
                        NodeOutputConfig::File { path: name } => resource_builder
                            .new_resource_from_file(
                                &PathBuf::from(name),
                                &self.config.node_id,
                                output,
                            )?,
                    };
                    state.insert((self.config.node_id.clone(), output.clone()), resource);
                    Ok(state)
                },
            )
            .map_err(|e| NodeError::new_unconditional(self, e))
    }

    pub fn next_execution_time(&self) -> Option<DateTime<Local>> {
        let last_execution = self.state.borrow().last_executed;
        match self.config.typ {
            NodeType::Scheduled {
                schedule: NodeSchedule::Daily,
            } => {
                Some(last_execution.with_time(NaiveTime::MIN).unwrap() + Duration::days(1))
            }
            NodeType::Scheduled {
                schedule: NodeSchedule::Repeated { interval_ms },
            } => {
                Some(last_execution + Duration::milliseconds(interval_ms as i64))
            }
            _ => { None }
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

impl NodeError {
    fn new_unconditional(node: &Node, message: String) -> NodeError {
        NodeError {
            handling: node.config.error_handling.handling,
            error: Some(message),
        }
    }
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
        &crate::ConfigFile {
            nodes: vec![
                NodeConfig {
                    node_id: "testnode".into(),
                    program: "echo".into(),
                    arguments: vec!["world".into()],
                    outputs: BTreeMap::from([
                        (
                            "stdout".to_owned(),
                            NodeOutputConfig::Stdout
                        )
                    ]),
                    inputs: BTreeMap::new(),
                    typ: NodeType::Scheduled {
                        schedule: NodeSchedule::Startup { delay_ms: 0 },
                    },
                    error_handling: NodeErrorConfig {
                        handling: NodeErrorHandling::Fail,
                        error_log: NodeErrorLog::Stderr,
                    },
                },
                NodeConfig {
                    node_id: "consume".into(),
                    program: "cp".into(),
                    arguments: vec!["%input".into(), "/dev/null".into()],
                    outputs: BTreeMap::new(),
                    inputs: BTreeMap::from([
                        (
                            "input".to_owned(),
                            NodeInputConfig::FilenameArg { placeholder: "%input".to_owned() }
                        )
                    ]),
                    typ: NodeType::OnInput,
                    error_handling: NodeErrorConfig {
                        handling: NodeErrorHandling::Retry { delay_ms: 1000 },
                        error_log: NodeErrorLog::Logfile { path: "dummy/path".into() }
                    },
                },
                NodeConfig {
                    node_id: "fail".into(),
                    program: "test/fail.sh".into(),
                    arguments: vec![],
                    outputs: BTreeMap::new(),
                    inputs: BTreeMap::new(),
                    typ: NodeType::Scheduled { schedule: NodeSchedule::Repeated { interval_ms: 60000 } },
                    error_handling: NodeErrorConfig {
                        handling: NodeErrorHandling::Retry { delay_ms: 1000 },
                        error_log: NodeErrorLog::Stderr
                    },
                },
            ],
            connections: vec![
                crate::Connection { from_id: "testnode".into(), output: "stdout".into(), to_id: "consume".into(), input: "input".to_owned() }
            ]
        }
    )
    .unwrap();
}
