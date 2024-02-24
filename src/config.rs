use serde_derive::Deserialize;

use std::fs;
use toml;

const CONFIG_FILE: &str = "config.toml";

#[derive(Debug, Deserialize, Clone)]
pub struct TablesConfig {
    pub data_source: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DbConfig {
    pub username: String,
    pub password: String,
    pub host: String,
    pub port: String,
    pub database: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DbTechnology {
    pub category: String,
}

// Top level struct to hold the TOML data.
#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub source: DbConfig,
    pub target: DbConfig,
    pub tables: TablesConfig,
    pub technology: DbTechnology,
}

pub fn read_config() -> Config {
    println!("Reading config file: {}", CONFIG_FILE);
    let contents = fs
        ::read_to_string(CONFIG_FILE)
        .expect(format!("Could not read file `{}`", CONFIG_FILE).as_str());

    let data: Config = toml
        ::from_str(&contents)
        .expect(format!("Unable to load data from `{}`", CONFIG_FILE).as_str());
    println!("Read config file: {}", CONFIG_FILE);
    println!("{:#?}", data);

    return data;
}
