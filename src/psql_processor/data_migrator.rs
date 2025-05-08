use sqlx::{postgres::PgRow, Pool, Postgres, Row};

use crate::{
    config::Config, error::CustomError, logger::Logger, psql_processor::db::get_connections_pool,
    CustomResult,
};
pub struct DataMigrator {
    pub config: Config,
    pub target_schema: String,
    pub source_schema: String,
    pub source_conn: Pool<Postgres>,
    pub target_conn: Pool<Postgres>,
    pub logger: Logger,
}

impl DataMigrator {
    pub async fn init(config: Config) -> CustomResult<Self> {
        assert_ne!(config.target.schema, None, "Target schema is not provided");
        assert_ne!(config.source.schema, None, "Source schema is not provided");

        let logger = Logger::new();
        logger.info("Connecting to source database");
        let source_conn = get_connections_pool(&config.source).await?;
        logger.info("Connected to source database");

        logger.info("Connecting to target database");
        let target_conn = get_connections_pool(&config.target).await?;
        logger.info("Connected to target database");

        Ok(Self {
            config: config.clone(),
            target_schema: config.target.schema.as_ref().unwrap().clone(),
            source_schema: config.source.schema.as_ref().unwrap().clone(),
            source_conn,
            target_conn,
            logger,
        })
    }

    pub async fn migrate(&self) -> CustomResult<()> {
        let mut failed_tables = Vec::new();
        let mut success_tables = Vec::new();
        for table in &self.config.tables.data_source {
            let result = self.migrate_table(table).await;
            if result.is_err() {
                failed_tables.push(table.to_string());
            } else {
                success_tables.push(table.to_string());
            }
        }

        self.logger
            .info(format!("Failed tables: {:?}", failed_tables).as_str());
        self.logger
            .info(format!("Success tables: {:?}", success_tables).as_str());
        Ok(())
    }

    async fn migrate_table(&self, table: &str) -> CustomResult<()> {
        self.logger
            .debug(format!("Truncating data from table: {}", table).as_str());
        self.truncate_table(table).await?;
        self.logger
            .debug(format!("Truncated data from table: {}", table).as_str());

        self.logger
            .debug(format!("Migrating data for table: {}", table).as_str());

        // Get the select query for fetching data
        let select_query = self.get_select_string(&self.source_schema, table).await?;

        // Get column names for proper value extraction
        let raw_columns = self.get_table_columns(&self.source_schema, table).await?;

        let columns = raw_columns
            .iter()
            .filter(|(name, _, _)| name != "id") // Exclude ID column
            .map(|(name, data_type, is_nullable)| {
                (name.clone(), data_type.clone(), is_nullable.clone())
            })
            .collect::<Vec<(String, String, String)>>();

        let rows = self.get_rows(&select_query).await?;

        if rows.is_empty() {
            self.logger
                .debug(format!("No data to migrate for table: {}", table).as_str());

            return Ok(());
        }

        // Build the INSERT statement with multiple rows
        let column_list: Vec<String> = self.get_column_list(&columns)?;

        let values_list: Vec<String> = self.get_values_list(&rows, &columns)?;

        self.logger
            .debug(format!("Executing multi-row insert for table: {}", table).as_str());
        self.execute_insert(table, &column_list, &values_list)
            .await?;

        self.logger
            .debug(format!("Migrated data for table: {}", table).as_str());
        Ok(())
    }

    fn get_values_list(
        &self,
        rows: &[PgRow],
        columns: &[(String, String, String)],
    ) -> CustomResult<Vec<String>> {
        let values_list: Vec<String> = rows
            .iter()
            .enumerate()
            .map(|(row_num, row)| {
                let values: Vec<String> = columns
                    .iter()
                    .enumerate()
                    .map(|(idx, (name, data_type, _))| {
                        let value: Option<String> = row.try_get(name.as_str()).unwrap_or(None);
                        match value {
                            Some(v) => {
                                if data_type == "integer" || data_type == "bigint" {
                                    v
                                } else {
                                    format!("'{}'", v.replace("'", "''"))
                                }
                            }
                            None => {
                                // For ID column (first column), use row number + 1 if NULL
                                if idx == 0 && (data_type == "integer" || data_type == "bigint") {
                                    (row_num + 1).to_string()
                                } else {
                                    "NULL".to_string()
                                }
                            }
                        }
                    })
                    .collect();
                format!("({})", values.join(", "))
            })
            .collect();

        Ok(values_list)
    }

    fn get_column_list(&self, columns: &[(String, String, String)]) -> CustomResult<Vec<String>> {
        let column_list: Vec<String> = columns
            .iter()
            .map(|(name, _, _)| format!("\"{}\"", name))
            .collect();

        Ok(column_list)
    }

    async fn get_rows(&self, select_query: &str) -> CustomResult<Vec<PgRow>> {
        // Fetch data from source
        let rows = sqlx::query(select_query)
            .fetch_all(&self.source_conn)
            .await
            .map_err(|e| {
                self.logger
                    .error(format!("Failed to fetch data: {}", e).as_str());
                CustomError::QueryExecution
            })?;

        Ok(rows)
    }

    async fn truncate_table(&self, table: &str) -> CustomResult<()> {
        let query = format!(
            "TRUNCATE TABLE {}.{} RESTART IDENTITY CASCADE",
            self.target_schema, table
        );
        sqlx::query(query.as_str())
            .execute(&self.target_conn)
            .await
            .map_err(|e| {
                self.logger.error(e.to_string().as_str());
                CustomError::QueryExecution
            })?;
        Ok(())
    }
    async fn execute_insert(
        &self,
        table: &str,
        column_list: &[String],
        values_list: &[String],
    ) -> CustomResult<()> {
        let insert_statement = format!(
            "INSERT INTO {}.{} ({}) VALUES {}",
            self.target_schema,
            table,
            column_list.join(", "),
            values_list.join(", ")
        );
        sqlx::query(insert_statement.as_str())
            .execute(&self.target_conn)
            .await
            .map_err(|e| {
                self.logger.error(e.to_string().as_str());
                CustomError::QueryExecution
            })?;

        Ok(())
    }

    async fn get_select_string(&self, schema: &str, table: &str) -> CustomResult<String> {
        self.logger
            .debug(format!("Getting select string for table {}.{}", schema, table).as_str());

        // Get column information
        let query = r#"
            SELECT 
                column_name,
                data_type,
                is_nullable
            FROM information_schema.columns
            WHERE table_schema = $1 
            AND table_name = $2
            ORDER BY ordinal_position;
        "#;

        let columns: Vec<(String, String, String)> = sqlx::query(query)
            .bind(schema)
            .bind(table)
            .map(|row: sqlx::postgres::PgRow| {
                (
                    row.get("column_name"),
                    row.get("data_type"),
                    row.get("is_nullable"),
                )
            })
            .fetch_all(&self.source_conn)
            .await
            .map_err(|err| {
                self.logger
                    .error(format!("Failed to get column information: {}", err).as_str());
                CustomError::QueryExecution
            })?;

        if columns.is_empty() {
            self.logger
                .error(format!("No columns found for table {}.{}", schema, table).as_str());
            return Err(CustomError::DbTableStructure);
        }

        // Build column list with proper quoting
        let column_list: Vec<String> = columns
            .iter()
            .map(|(name, _, _)| format!("\"{}\"", name))
            .collect();

        // Build the SELECT statement
        let select_statement = format!(
            "SELECT {} FROM {}.{}",
            column_list.join(", "),
            schema,
            table
        );

        self.logger
            .debug(format!("Generated select statement: {}", select_statement).as_str());
        Ok(select_statement)
    }

    async fn get_table_columns(
        &self,
        schema: &str,
        table: &str,
    ) -> CustomResult<Vec<(String, String, String)>> {
        let query = r#"
            SELECT 
                column_name,
                data_type,
                is_nullable
            FROM information_schema.columns
            WHERE table_schema = $1 
            AND table_name = $2
            ORDER BY ordinal_position;
        "#;

        sqlx::query(query)
            .bind(schema)
            .bind(table)
            .map(|row: sqlx::postgres::PgRow| {
                (
                    row.get("column_name"),
                    row.get("data_type"),
                    row.get("is_nullable"),
                )
            })
            .fetch_all(&self.source_conn)
            .await
            .map_err(|err| {
                self.logger
                    .error(format!("Failed to get column information: {}", err).as_str());
                CustomError::QueryExecution
            })
    }
}
