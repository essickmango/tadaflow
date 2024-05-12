use std::{collections::BTreeMap, fmt::Debug, rc::Rc, time::SystemTime};

use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};

use crate::{
    node::{Node, NodeError, NodeId},
    resource::{Resource, ResourceBuilder, ResourceId},
};

#[derive(Debug)]
pub struct ScheduledTask {
    task: Task,
    time: DateTime<Local>,
}

impl ScheduledTask {
    pub fn to_serializeable(&self) -> SerializeableScheduledTask {
        SerializeableScheduledTask {
            task: self.task.to_serializeable(),
            time: self.time,
        }
    }

    pub fn get_time(&self) -> SystemTime {
        self.time.into()
    }

    pub fn execute(
        &self,
        resource_builder: &ResourceBuilder,
    ) -> Result<BTreeMap<(NodeId, String), Rc<Resource>>, NodeError> {
        self.task.execute(resource_builder)
    }

    pub fn reschedule(&self, time: DateTime<Local>) -> ScheduledTask {
        self.task.schedule(time).unwrap()
    }

    pub fn get_node(&self) -> &Rc<Node> {
        &self.task.node
    }

    pub fn replace_input(&mut self, input: String, resource: Rc<Resource>) {
        self.task.replace_input(input, resource)
    }
}

pub struct Task {
    node: Rc<Node>,
    inputs: BTreeMap<String, Rc<Resource>>,
}

impl Task {
    pub fn new(node: Rc<Node>) -> Task {
        Task {
            node,
            inputs: BTreeMap::new(),
        }
    }

    pub fn to_serializeable(&self) -> SerializeableTask {
        SerializeableTask {
            node: self.node.config.node_id.clone(),
            inputs: self
                .inputs
                .iter()
                .map(|(key, value)| (key.clone(), value.id.clone()))
                .collect(),
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node.config.node_id.clone()
    }

    pub fn execute(
        &self,
        resource_builder: &ResourceBuilder,
    ) -> Result<BTreeMap<(NodeId, String), Rc<Resource>>, NodeError> {
        self.node.execute(self.inputs.clone(), resource_builder)
    }

    pub fn schedule(&self, time: DateTime<Local>) -> Option<ScheduledTask> {
        if self
            .node
            .config
            .get_input_names()
            .all(|input| self.inputs.contains_key(input))
        {
            Some(ScheduledTask {
                task: Task {
                    node: self.node.clone(),
                    inputs: self.inputs.clone(),
                },
                time,
            })
        } else {
            None
        }
    }

    pub fn replace_input(&mut self, input: String, resource: Rc<Resource>) {
        if self.node.config.has_input(&input) {
            self.inputs.insert(input, resource);
        }
    }
}

impl Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Task").field("node", &self.node.config.node_id).field("inputs", &self.inputs).finish()
    }
}

#[derive(Serialize, Deserialize)]
pub struct SerializeableScheduledTask {
    task: SerializeableTask,
    time: DateTime<Local>,
}

impl SerializeableScheduledTask {
    pub fn to_task(
        self,
        nodes: &BTreeMap<NodeId, Rc<Node>>,
        resources: &BTreeMap<ResourceId, Rc<Resource>>,
    ) -> Result<ScheduledTask, TaskDeserializationError> {
        let task = self.task.to_task(nodes, resources)?;
        Ok(ScheduledTask {
            task,
            time: self.time,
        })
    }
}

#[derive(Serialize, Deserialize)]
pub struct SerializeableTask {
    node: NodeId,
    inputs: BTreeMap<String, ResourceId>,
}

impl SerializeableTask {
    pub fn to_task(
        self,
        nodes: &BTreeMap<NodeId, Rc<Node>>,
        resources: &BTreeMap<ResourceId, Rc<Resource>>,
    ) -> Result<Task, TaskDeserializationError> {
        let Some(node) = nodes.get(&self.node) else {
            return Err(TaskDeserializationError::NodeNotFound(self.node));
        };
        let inputs =
            self.inputs
                .into_iter()
                .fold(Ok(BTreeMap::new()), |state, (input, resource_id)| {
                    let mut state = state?;
                    let Some(resource) = resources.get(&resource_id) else {
                        return Err(TaskDeserializationError::ResourceNotFound(resource_id));
                    };
                    state.insert(input, resource.clone());
                    Ok(state)
                })?;
        Ok(Task {
            node: node.clone(),
            inputs,
        })
    }
}

pub enum TaskDeserializationError {
    NodeNotFound(NodeId),
    ResourceNotFound(ResourceId),
}
