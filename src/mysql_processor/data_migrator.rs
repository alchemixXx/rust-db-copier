use std::collections::HashMap;

use crate::{config::Config, error::CustomError, mysql_processor::db::get_connection};
use mysql::{from_value, prelude::Queryable, PooledConn, Row};

use crate::CustomResult;
pub struct DataMigrator {
    pub config: Config,
}

impl DataMigrator {
    pub fn migrate(&self) -> CustomResult<()> {
        println!("Connecting to source database");
        let mut source_conn = get_connection(&self.config.source)?;
        println!("Connected to source database");

        println!("Connecting to target database");
        let mut target_conn = get_connection(&self.config.target)?;
        println!("Connected to target database");

        for table in &self.config.tables.data_source {
            if !self.config.technology.copy_structure {
                println!("Truncating table: {}", table);
                self.truncate_table(&mut target_conn, table)?;
                println!("Truncated table: {}", table);
            };

            println!("Migrating data for table: {}", table);
            let data: Vec<HashMap<String, mysql::Value>> =
                self.get_data(&mut source_conn, table)?;

            for row in data {
                let column_names: Vec<String> = row
                    .iter()
                    .map(|(key, _)| format!("`{}`", key.as_str()))
                    .collect();

                let values: Vec<mysql::Value> = row.values().map(|value| value.clone()).collect();

                let values_as_strings: Vec<String> = values
                    .iter()
                    .map(|value| match value {
                        mysql::Value::NULL => "NULL".to_string(),
                        _ => {
                            let mut value = from_value::<String>(value.clone());
                            if value.contains('\'') {
                                value = value.replace('\'', "\\'");
                            }
                            format!("'{}'", value)
                        }
                    })
                    .collect();

                let insert_query = format!(
                    "INSERT INTO {} ({}) VALUES ({});",
                    table,
                    column_names.join(", "),
                    values_as_strings.join(", ")
                );

                let insert_result = target_conn.exec_drop(insert_query, ());

                match insert_result {
                    Ok(_) => {}
                    Err(err) => {
                        println!("Error: {:?}", err);
                        return Err(CustomError::QueryExecution);
                    }
                }
            }
            println!("Migrated data for table: {}", table);
        }
        Ok(())
    }

    fn get_columns(&self, connection: &mut PooledConn, table: &str) -> CustomResult<Vec<String>> {
        let column_query = format!("SHOW COLUMNS FROM {};", table);
        let rows: Vec<String> = connection
            .query_map(column_query, |row: Row| -> CustomResult<String> {
                let columns = row.columns_ref();

                let mut index: Option<usize> = None;
                for (i, column) in columns.iter().enumerate() {
                    if column.name_str() == "Field" {
                        index = Some(i);
                        break;
                    }
                }

                let value = (match index {
                    None => Err(CustomError::DbTableStructure),
                    Some(value) => {
                        let query: String =
                            row.get(value).expect("Value should be present in the Roo");

                        Ok(query)
                    }
                })?;

                Ok(value)
            })
            .map_err(|err| {
                println!("Error: {:?}", err);
                CustomError::QueryExecution
            })?
            .into_iter()
            .filter_map(|el| el.map_err(|err| err).ok())
            .collect();

        Ok(rows)
    }

    fn get_data(
        &self,
        connection: &mut PooledConn,
        table: &str,
    ) -> CustomResult<Vec<HashMap<String, mysql::Value>>> {
        let columns = self.get_columns(connection, table)?;
        let data: Vec<HashMap<String, mysql::Value>> = connection
            .query_map(format!("SELECT * FROM {}", table), |row: Row| {
                let mut map: HashMap<String, mysql::Value> = HashMap::new();
                for (index, column_name) in columns.iter().enumerate() {
                    map.insert(column_name.clone(), row.get(index).unwrap());
                }
                map
            })
            .unwrap();

        Ok(data)
    }

    fn truncate_table(&self, connection: &mut PooledConn, table: &str) -> CustomResult<()> {
        connection
            .query_drop("SET FOREIGN_KEY_CHECKS = 0;")
            .map_err(|err| {
                println!("Error disabling foreign key checks: {:?}", err);
                CustomError::QueryExecution
            })?;

        let truncate_query = format!("TRUNCATE TABLE {};", table);
        connection.exec_drop(truncate_query, ()).map_err(|err| {
            println!("Error truncating table: {:?}", err);
            CustomError::QueryExecution
        })?;

        connection
            .query_drop("SET FOREIGN_KEY_CHECKS = 1;")
            .map_err(|err| {
                println!("Error enabling foreign key checks: {:?}", err);
                CustomError::QueryExecution
            })?;

        Ok(())
    }
}
