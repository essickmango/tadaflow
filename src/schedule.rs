use std::{collections::{BTreeMap, HashSet}, rc::Rc, time::{Duration, SystemTime}, io::Write};

use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};

use crate::{node::{Filename, Node, NodeErrorHandling}, Log};


struct ScheduleNode {
    node: Rc<Node>,
    time: SystemTime,
    should_persist: bool,
}

fn get_time(node: &ScheduleNode) -> SystemTime { node.time }


pub struct Schedule {
    next: Option<ScheduleNode>,
    following: Vec<ScheduleNode>,
}

impl Schedule {
    fn from_two_iters(left: impl Iterator<Item=ScheduleNode>, right: impl Iterator<Item=ScheduleNode>) -> Schedule {
        let size = match (left.size_hint().1, right.size_hint().1) {
            (Some(x), Some(y)) => Some((x + y).saturating_sub(1)),
            _ => None
        };
        let mut iter1 = left.into_iter().peekable();
        let mut iter2 = right.into_iter().peekable();
        
        let next = match (iter1.peek().map(get_time), iter2.peek().map(get_time)) {
            (Some(i1), Some(i2)) if i2 < i1
                => Some(iter2.next().unwrap()),
            (Some(_), Some(_))
                => Some(iter1.next().unwrap()),
            (Some(_), None)
                => Some(iter1.next().unwrap()),
            (None, Some(_))
                => Some(iter2.next().unwrap()),
            (None, None)
                => None
        };

        let mut joined = if let Some(size) = size { Vec::with_capacity(size) } else { Vec::new() };
        while iter1.peek().is_some() || iter2.peek().is_some() {
            match (iter1.peek().map(get_time), iter2.peek().map(get_time)) {
                (Some(i1), Some(i2)) if i2 < i1
                    => joined.push(iter2.next().unwrap()),
                (Some(_), Some(_))
                    => joined.push(iter1.next().unwrap()),
                (Some(_), None)
                    => joined.push(iter1.next().unwrap()),
                (None, Some(_))
                    => joined.push(iter2.next().unwrap()),
                (None, None)
                    => {}
            };
        }

        Schedule {
            next,
            following: joined
        }
    }

    pub fn merge(self, other: Schedule) -> Schedule {
        Schedule::from_two_iters(self.next.into_iter().chain(self.following), other.next.into_iter().chain(other.following))
    }

    pub fn execute(mut self, lut: &BTreeMap<Filename, Vec<Rc<Node>>>, log: &mut Log) -> Schedule {
        match self.next.take() {
            Some(scheduled_node) => {
                if let Ok(duration) = scheduled_node.time.duration_since(SystemTime::now()) {
                    std::thread::sleep(duration);
                }
                write!(log, "Executing node {}\n", scheduled_node.node.config.node_id).unwrap();

                let nodes_to_schedule = match scheduled_node.node.execute() {
                    Ok(filenames) => {
                        let mut nodes_to_schedule = filenames
                            .into_iter()
                            .map(|filename| lut.get(&filename).unwrap_or(&vec![]).into_iter().cloned().collect::<HashSet<Rc<Node>>>())
                            .flatten()
                            .map(|node| ScheduleNode {
                                node,
                                time: SystemTime::now(),
                                should_persist: true
                            })
                            .collect::<Vec<ScheduleNode>>();

                        if let Some(time) = scheduled_node.node.should_be_scheduled(false) {
                            nodes_to_schedule.push(ScheduleNode { node: scheduled_node.node, time: time, should_persist: false })
                        }
                        nodes_to_schedule
                    },
                    Err(e) => {
                        if let Some(error) = e.error {
                            write!(log, "Error while executing {}: {}\n", &scheduled_node.node.config.node_id, error).unwrap()
                        }

                        if let NodeErrorHandling::Retry { delay_ms } = e.handling {
                            vec![ScheduleNode {
                                node: scheduled_node.node,
                                time: SystemTime::now() + Duration::from_millis(delay_ms),
                                should_persist: scheduled_node.should_persist
                            }]
                        } else { vec![] }
                    }
                };

                Schedule::from_two_iters(self.following.into_iter(), nodes_to_schedule.into_iter())
            },
            None => self
        }
    }

    pub fn next_scheduled_event(&self) -> Option<SystemTime> {
        match &self.next {
            Some(schedule_node) => Some(schedule_node.time),
            None => None,
        }
    }

    pub fn serializable(&self) -> SerializeableSchedule {
        SerializeableSchedule(
            self.next
                .iter()
                .chain(self.following.iter())
                .filter_map(|schedule_node|
                    if schedule_node.should_persist {
                        Some(SerializeableScheduleNode {
                            node_id: schedule_node.node.config.node_id.clone(),
                            time: DateTime::<Local>::from(schedule_node.time)
                        })
                    } else { None })
                .collect())
    }

    pub fn for_nodes(nodes: &[Rc<Node>], is_startup: bool) -> Schedule {
        let mut iter = nodes.iter().filter_map(|node| node.should_be_scheduled(is_startup).map(|time| ScheduleNode {
            node: node.clone(),
            time,
            should_persist: false
        }));
        Schedule {
            next: iter.next(),
            following: iter.collect(),
        }
    }
}


#[derive(Serialize, Deserialize)]
pub struct SerializeableSchedule(Vec<SerializeableScheduleNode>);

#[derive(Serialize, Deserialize)]
struct SerializeableScheduleNode {
    node_id: String,
    time: DateTime<Local>,
}

impl SerializeableSchedule {
    pub fn to_schedule(self, node_map: &BTreeMap<String, Rc<Node>>) -> Schedule {
        let to_schedule_node = |serial_node: SerializeableScheduleNode| {
            node_map.get(&serial_node.node_id).map(|node| ScheduleNode { node: node.clone(), time: serial_node.time.into(), should_persist: true })
        };

        let mut iter = self.0.into_iter();
        let next = iter.next().map(to_schedule_node).flatten();

        Schedule {
            next,
            following: iter.filter_map(to_schedule_node).collect()
        }
    }
}

