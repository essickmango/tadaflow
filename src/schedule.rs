use std::{collections::BTreeMap, io::Write, rc::Rc, time::SystemTime, vec};

use chrono::{Duration, Local, NaiveTime};
use serde::{Deserialize, Serialize};

use crate::{
    node::{Node, NodeErrorHandling, NodeId, NodeSchedule, NodeType},
    resource::{Resource, ResourceBuilder, ResourceId},
    task::{
        ScheduledTask, SerializeableScheduledTask, SerializeableTask, Task,
        TaskDeserializationError,
    },
    Log,
};

/// The schedule has the following invariants:
///  1. next.is_none() => following.is_empty()
///  2. repeated nodes are always scheduled (except for `Startup`, `Manual`).
#[derive(Debug)]
pub struct Schedule {
    next: Option<ScheduledTask>,
    following: Vec<ScheduledTask>,
    templates: BTreeMap<NodeId, Task>,
}

impl Schedule {
    pub fn new() -> Schedule {
        Schedule {
            next: None,
            following: vec![],
            templates: BTreeMap::new(),
        }
    }

    fn merge(self, new_tasks: Vec<ScheduledTask>) -> Schedule {
        let mut res = self
            .next
            .into_iter()
            .chain(self.following)
            .chain(new_tasks)
            .collect::<Vec<ScheduledTask>>();
        res.sort_by(|t1, t2| t1.get_time().cmp(&t2.get_time()));
        let mut iter = res.into_iter();
        Schedule {
            next: iter.next(),
            following: iter.collect(),
            templates: self.templates,
        }
    }

    fn has_task_scheduled_for_node(&self, nodeid: &NodeId) -> bool {
        self.next
            .iter()
            .chain(self.following.iter())
            .any(|task| &task.get_node().config.node_id == nodeid)
    }

    pub fn schedule_nodes(
        mut self,
        nodes: &Vec<Rc<Node>>,
        resources: &BTreeMap<(NodeId, String), Rc<Resource>>,
    ) -> Schedule {
        let mut new_tasks = vec![];
        for node in nodes {
            if !self.templates.contains_key(&node.config.node_id) {
                let mut task = Task::new(node.clone());
                for input in node.config.get_input_names() {
                    if let Some(resource) =
                        resources.get(&(node.config.node_id.clone(), input.clone()))
                    {
                        task.replace_input(input.clone(), resource.clone())
                    }
                }
                self.templates.insert(node.config.node_id.clone(), task);
            };

            match node.config.typ {
                NodeType::Scheduled {
                    schedule: NodeSchedule::Daily,
                } => {
                    if !self.has_task_scheduled_for_node(&node.config.node_id) {
                        let next_execution = if node.state.borrow().last_executed.date_naive()
                            < Local::now().date_naive()
                        {
                            Local::now()
                        } else {
                            Local::now().with_time(NaiveTime::MIN).unwrap() + Duration::days(1)
                        };
                        if let Some(task) = self
                            .templates
                            .get(&node.config.node_id)
                            .unwrap()
                            .schedule(next_execution)
                        {
                            new_tasks.push(task);
                        }
                    }
                }
                NodeType::Scheduled {
                    schedule: NodeSchedule::Startup { delay_ms },
                } => {
                    if !self.has_task_scheduled_for_node(&node.config.node_id) {
                        if let Some(task) = self
                            .templates
                            .get(&node.config.node_id)
                            .unwrap()
                            .schedule(Local::now() + Duration::milliseconds(delay_ms as i64))
                        {
                            new_tasks.push(task)
                        }
                    }
                }
                NodeType::Scheduled {
                    schedule: NodeSchedule::Repeated { interval_ms },
                } => {
                    if !self.has_task_scheduled_for_node(&node.config.node_id) {
                        if let Some(task) = self
                            .templates
                            .get(&node.config.node_id)
                            .unwrap()
                            .schedule(Local::now() + Duration::milliseconds(interval_ms as i64))
                        {
                            new_tasks.push(task)
                        }
                    }
                }
                _ => {}
            }
        }

        self.merge(new_tasks)
    }

    pub fn execute(
        mut self,
        lut: &BTreeMap<(NodeId, String), Vec<(Rc<Node>, String)>>,
        resource_builder: &ResourceBuilder,
        log: &mut Log,
    ) -> Schedule {
        let Some(mut task) = self.next.take() else {
            return self;
        };

        writeln!(log, "[INFO] Executing task for node {}", &task.get_node().config.node_id).unwrap();

        let new_tasks = match task.execute(resource_builder) {
            Ok(outputs) => {
                let mut new_tasks = vec![];
                for (output, resource) in outputs {
                    for (input_node, input) in lut.get(&output).unwrap_or(&vec![]) {
                        match input_node.config.typ {
                            NodeType::Scheduled { .. } => {
                                for scheduled_task in self.following.iter_mut() {
                                    if scheduled_task.get_node().config.node_id
                                        == input_node.config.node_id
                                    {
                                        scheduled_task
                                            .replace_input(input.clone(), resource.clone())
                                    }
                                }
                                if input_node.config.node_id == task.get_node().config.node_id {
                                    task.replace_input(input.clone(), resource.clone())
                                }
                            }
                            NodeType::OnInput => {
                                let task = match self.templates.get_mut(&input_node.config.node_id)
                                {
                                    Some(x) => x,
                                    None => {
                                        self.templates.insert(
                                            input_node.config.node_id.clone(),
                                            Task::new(input_node.clone()),
                                        );
                                        self.templates.get_mut(&input_node.config.node_id).unwrap()
                                    }
                                };
                                task.replace_input(input.clone(), resource.clone());
                                if let Some(task) = task.schedule(Local::now()) {
                                    new_tasks.push(task)
                                }
                            }
                        }
                    }
                }
                if let Some(time) = task.get_node().next_execution_time() {
                    new_tasks.push(task.reschedule(time))
                }

                new_tasks
            }
            Err(node_error) => {
                if let Some(message) = node_error.error {
                    writeln!(log, "[WARN] An error executed while executing a task for node {}: {}", task.get_node().config.node_id, message).unwrap();
                } else {
                    writeln!(log, "[INFO] Task for node {} failed silently.", task.get_node().config.node_id).unwrap();
                }
                match node_error.handling {
                    NodeErrorHandling::Fail => {
                        if let Some(time) = task.get_node().next_execution_time() {
                            vec![task.reschedule(time)]
                        } else { vec![] }
                    },
                    NodeErrorHandling::Retry { delay_ms } => {
                        vec![task.reschedule(Local::now() + Duration::milliseconds(delay_ms as i64))]
                    }
                }
            }
        };

        self.merge(new_tasks)
    }

    #[cfg(feature="management-interface")]
    pub fn trigger(&mut self, nodeid: &NodeId) -> Result<(), String> {
        match self.templates.get(nodeid) {
            Some(task) => {
                if let Some(task) = task.schedule(Local::now()) {
                    let previous_next = self.next.replace(task);

                    let mut placeholder = vec![];
                    std::mem::swap(&mut placeholder, &mut self.following);
                    self.following = previous_next.into_iter().chain(placeholder).collect();

                    Ok(())
                } else {
                    Err(format!("Node {} has not all inputs.", nodeid))
                }
            }
            None => Err(format!("Node {} not found.", nodeid)),
        }
    }

    pub fn next_scheduled_task(&self) -> Option<SystemTime> {
        self.next.as_ref().map(|task| task.get_time())
    }

    pub fn to_serializeable(&self) -> SerializeableSchedule {
        SerializeableSchedule {
            tasks: self
                .next
                .iter()
                .chain(self.following.iter())
                .map(|t| t.to_serializeable())
                .collect(),
            templates: self
                .templates
                .values()
                .map(|t| t.to_serializeable())
                .collect(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct SerializeableSchedule {
    tasks: Vec<SerializeableScheduledTask>,
    templates: Vec<SerializeableTask>,
}

impl SerializeableSchedule {
    pub fn to_schedule(
        self,
        nodes: &BTreeMap<NodeId, Rc<Node>>,
        resources: &BTreeMap<ResourceId, Rc<Resource>>,
        log: &mut Log,
    ) -> Schedule {
        let templates = self
            .templates
            .into_iter()
            .filter_map(|task| match task.to_task(nodes, resources) {
                Ok(task) => Some((task.node_id(), task)),
                Err(TaskDeserializationError::NodeNotFound(node_id)) => {
                    writeln!(log, "[WARN] Node {} for task not found, ignoring.", node_id).unwrap();
                    None
                }
                Err(TaskDeserializationError::ResourceNotFound(resource_id)) => {
                    writeln!(
                        log,
                        "[WARN] Resource {} for task not found, ignoring task.",
                        resource_id
                    )
                    .unwrap();
                    None
                }
            })
            .collect();

        let mut tasks =
            self.tasks
                .into_iter()
                .filter_map(|task| match task.to_task(nodes, resources) {
                    Ok(task) => Some(task),
                    Err(TaskDeserializationError::NodeNotFound(node_id)) => {
                        writeln!(log, "[ERROR] Node {} for task not found, ignoring.", node_id)
                            .unwrap();
                        None
                    }
                    Err(TaskDeserializationError::ResourceNotFound(resource_id)) => {
                        writeln!(
                            log,
                            "[ERROR] Resource {} for task not found, ignoring task.",
                            resource_id
                        )
                        .unwrap();
                        None
                    }
                });

        Schedule {
            next: tasks.next(),
            following: tasks.collect(),
            templates,
        }
    }
}
