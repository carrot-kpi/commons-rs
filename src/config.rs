use std::{fs::File, io, path::PathBuf};

use serde::de::DeserializeOwned;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum GetConfigError {
    #[error("could not get base project dir")]
    ProjectDir,
    #[error("could not open config file: {0:?}")]
    FileOpen(io::Error),
    #[error("could not deserialize config file: {0:?}")]
    Deserialization(serde_yaml::Error),
}

pub fn get_config<S: AsRef<str>, C: DeserializeOwned>(
    app_name: S,
    alt_path: Option<PathBuf>,
) -> Result<C, GetConfigError> {
    if let Some(project_dirs) =
        directories::ProjectDirs::from("xyz", "carrot-labs", app_name.as_ref())
    {
        let path = if let Some(alt_path) = alt_path {
            alt_path
        } else {
            PathBuf::from(project_dirs.config_dir())
        };

        let config_file = File::open(path).map_err(|err| GetConfigError::FileOpen(err))?;

        serde_yaml::from_reader::<File, C>(config_file)
            .map_err(|err| GetConfigError::Deserialization(err))
    } else {
        Err(GetConfigError::ProjectDir)
    }
}
