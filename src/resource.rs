use std::{path::PathBuf, rc::Rc};

use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};

use crate::node::NodeId;

pub type ResourceId = String;

#[derive(Debug)]
pub struct Resource {
    pub id: ResourceId,
    pub metadata: ResourceMetadata,
    pub path: PathBuf,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ResourceMetadata {
    pub created_at: DateTime<Local>,
    pub created_by: NodeId,
    pub name: String,
}

pub fn load_resources(resource_dir: PathBuf) -> Result<Vec<Rc<Resource>>, std::io::Error> {
    Ok(std::fs::read_dir(resource_dir)?
        .filter_map(|file| {
            let file = file.ok()?;
            let metadata = file.metadata().ok()?;
            let file_opt = file.file_name();
            let filename = file_opt.to_str()?;
            if metadata.is_file() && filename.ends_with(".meta") {
                let opened = std::fs::OpenOptions::new()
                    .read(true)
                    .open(file.path())
                    .ok()?;
                let resource_path = file.path().with_extension("");
                if !resource_path.exists() {
                    std::fs::remove_file(file.path()).ok()?;
                    return None;
                }
                let id = filename.strip_suffix(".meta").unwrap().to_owned();

                let metadata = serde_yaml::from_reader(opened).ok()?;
                Some(Rc::new(Resource {
                    id,
                    metadata,
                    path: resource_path,
                }))
            } else {
                None
            }
        })
        .collect())
}

impl Drop for Resource {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        let _ = std::fs::remove_file(self.path.with_extension("meta"));
    }
}

pub struct ResourceBuilder {
    resource_dir: PathBuf,
}

impl ResourceBuilder {
    pub fn new(resource_dir: PathBuf) -> ResourceBuilder {
        ResourceBuilder { resource_dir }
    }

    pub fn new_resource_from_file(
        &self,
        path: &PathBuf,
        creator: &NodeId,
        name: &str,
    ) -> Result<Rc<Resource>, String> {
        let id = uuid::Uuid::new_v4().to_string();
        let resource_path = self.resource_dir.join(&id);
        let metadata = ResourceMetadata {
            created_at: Local::now(),
            created_by: creator.clone(),
            name: name.to_owned(),
        };
        std::fs::copy(path, &resource_path).map_err(|e| e.to_string())?;
        let meta_file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&resource_path.with_extension("meta"))
            .map_err(|e| e.to_string())?;
        serde_yaml::to_writer(meta_file, &metadata).map_err(|e| e.to_string())?;

        Ok(Rc::new(Resource {
            id,
            metadata,
            path: resource_path,
        }))
    }

    pub fn new_resource(&self, creator: &NodeId, name: &str) -> Result<Rc<Resource>, String> {
        let id = uuid::Uuid::new_v4().to_string();
        let resource_path = self.resource_dir.join(&id);
        let metadata = ResourceMetadata {
            created_at: Local::now(),
            created_by: creator.clone(),
            name: name.to_owned(),
        };
        let meta_file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&resource_path.with_extension("meta"))
            .map_err(|e| e.to_string())?;
        serde_yaml::to_writer(meta_file, &metadata).map_err(|e| e.to_string())?;
        let _ = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&resource_path)
            .map_err(|e| e.to_string())?;

        Ok(Rc::new(Resource {
            id,
            metadata,
            path: resource_path,
        }))
    }
}
