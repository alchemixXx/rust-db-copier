use std::collections::HashMap;

use crate::{ config::Config, mysql_processor::db::get_connection };
use mysql::{ from_value, prelude::Queryable, Row };

pub struct DataMigrator {
    pub config: Config,
}

impl DataMigrator {
    pub fn migrate(&self) {
        println!("Connecting to source database");
        let mut source_conn = get_connection(&self.config.source).unwrap();
        println!("Connected to source database");

        println!("Connecting to target database");
        let mut target_conn = get_connection(&self.config.target).unwrap();
        println!("Connected to target database");

        for table in &self.config.tables.data_source {
            println!("Migrating data for table: {}", table);
            let column_query = format!("SHOW COLUMNS FROM {};", table);
            let columns: Vec<String> = source_conn
                .query_map(column_query, |row: Row| {
                    let columns = row.columns_ref();
                    let mut column_index: Option<usize> = None;
                    for (index, column) in columns.iter().enumerate() {
                        if column.name_str() == "Field" {
                            column_index = Some(index);
                            break;
                        }
                    }
                    if column_index.is_none() {
                        panic!("No such column: `Field` in row {:?}", row);
                    }

                    let column: String = row.get(column_index.unwrap()).unwrap();
                    column
                })
                .unwrap();

            let data_query = format!("SELECT * FROM {}", table);
            let data: Vec<HashMap<String, mysql::Value>> = source_conn
                .query_map(data_query, |row: Row| {
                    let mut map: HashMap<String, mysql::Value> = HashMap::new();
                    for (index, column_name) in columns.iter().enumerate() {
                        map.insert(column_name.clone(), row.get(index).unwrap());
                    }
                    map
                })
                .unwrap();

            for row in data {
                let column_names: Vec<String> = row
                    .iter()
                    .map(|(key, _)| format!("`{}`", key.as_str()))
                    .collect();

                let values: Vec<mysql::Value> = row
                    .iter()
                    .map(|(_, value)| value.clone())
                    .collect();

                let values_as_strings: Vec<String> = values
                    .iter()
                    .map(|value| {
                        match value {
                            mysql::Value::NULL => "NULL".to_string(),
                            _ => {
                                let mut value = from_value::<String>(value.clone());
                                if value.contains('\'') {
                                    value = value.replace('\'', "\\'");
                                }
                                format!("'{}'", value)
                            }
                        }
                    })
                    .collect();

                let insert_query = format!(
                    "INSERT INTO {} ({}) VALUES ({});",
                    table,
                    column_names.join(", "),
                    values_as_strings.join(", ")
                );

                target_conn.exec_drop(insert_query, ()).unwrap();
            }
            println!("Migrated data for table: {}", table);
        }
    }
}
