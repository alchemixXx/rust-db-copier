use std::process::Command;

use sqlx::{Pool, Postgres};

use crate::{
    config::Config,
    error::{CustomError, CustomResult},
    logger::Logger,
    psql_processor::db::get_connections_pool,
};

pub struct PgDumpMigrator {
    pub config: Config,
    pub target_schema: String,
    pub source_schema: String,
    pub source_conn: Pool<Postgres>,
    pub target_conn: Pool<Postgres>,
    pub logger: Logger,
}

impl PgDumpMigrator {
    pub async fn new(config: Config) -> CustomResult<Self> {
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

    pub async fn migrate_data(&self) -> CustomResult<()> {
        self.logger.info(
            format!(
                "Migrating data from {} to {} with pg_dump",
                self.source_schema, self.target_schema
            )
            .as_str(),
        );
        let mut command = format!(
            "PGPASSWORD='{0}' pg_dump -U {1} -h {2} -d {3} --schema={4} --data-only",
            self.config.source.password,
            self.config.source.username,
            self.config.source.host,
            self.config.source.database,
            self.config.source.schema.as_ref().unwrap(),
        );

        for table in &self.config.tables.data_source {
            command.push_str(format!(" -t {}", table).as_str());
        }

        command.push_str(
            format!(
                " | PGPASSWORD='{0}' psql -U {1} -d {2} -h {3}",
                self.config.target.password,
                self.config.target.username,
                self.config.target.database,
                self.config.target.host
            )
            .as_str(),
        );

        let output = Command::new("zsh")
            .arg("-c")
            .arg(&command)
            .output()
            .map_err(|err| {
                self.logger
                    .error(format!("Failed to execute pg_dump command: {}", err).as_str());
                CustomError::CommandExecution
            })?;

        if !output.status.success() {
            self.logger
                .error(format!("Failed execute pg_dump command: {}", command).as_str());
            self.logger
                .error(format!("Error: {}", String::from_utf8_lossy(&output.stderr)).as_str());

            return Err(CustomError::CommandExecution);
        }

        if !output.stderr.is_empty() {
            self.logger
                .error(format!("Error: {}", String::from_utf8_lossy(&output.stderr)).as_str());

            return Err(CustomError::CommandExecution);
        }

        self.logger.info(
            format!(
                "Data migrated successfully from {} to {} with pg_dump",
                self.source_schema, self.target_schema
            )
            .as_str(),
        );
        Ok(())
    }

    pub async fn migrate_structure(&self) -> CustomResult<()> {
        self.logger
            .info(format!("Recreating target schema {}", self.target_schema).as_str());
        self.recreate_schema().await?;
        self.logger
            .info(format!("Re-created target schema {}", self.target_schema).as_str());

        let mut command = format!(
            "PGPASSWORD='{0}' pg_dump -U {1} -h {2} -d {3} --schema={4} --schema-only",
            self.config.source.password,
            self.config.source.username,
            self.config.source.host,
            self.config.source.database,
            self.config.source.schema.as_ref().unwrap(),
        );

        for table in &self.config.tables.skip {
            command.push_str(format!(" --exclude-table={}", table).as_str());
        }

        command.push_str(
            format!(
                " | PGPASSWORD='{0}' psql -U {1} -d {2} -h {3}",
                self.config.target.password,
                self.config.target.username,
                self.config.target.database,
                self.config.target.host
            )
            .as_str(),
        );

        let output = Command::new("zsh")
            .arg("-c")
            .arg(&command)
            .output()
            .map_err(|err| {
                self.logger
                    .error(format!("Failed to execute pg_dump command: {}", err).as_str());
                CustomError::CommandExecution
            })?;

        if !output.status.success() {
            self.logger
                .error(format!("Failed execute pg_dump command: {}", command).as_str());
            self.logger
                .error(format!("Error: {}", String::from_utf8_lossy(&output.stderr)).as_str());

            return Err(CustomError::CommandExecution);
        }

        if !output.stderr.is_empty() {
            self.logger
                .error(format!("Error: {}", String::from_utf8_lossy(&output.stderr)).as_str());
        }

        Ok(())
    }

    async fn recreate_schema(&self) -> CustomResult<()> {
        let drop_schema_query = format!("DROP SCHEMA IF EXISTS {} CASCADE;", self.target_schema);
        sqlx::query(&drop_schema_query)
            .execute(&self.target_conn)
            .await
            .map_err(|err| {
                self.logger
                    .error(format!("Failed to drop schema: {}", err).as_str());
                CustomError::QueryExecution
            })?;

        let create_schema_query = format!("CREATE SCHEMA IF NOT EXISTS {};", self.target_schema);
        sqlx::query(&create_schema_query)
            .execute(&self.target_conn)
            .await
            .map_err(|err| {
                self.logger
                    .error(format!("Failed to create schema: {}", err).as_str());
                CustomError::QueryExecution
            })?;

        self.logger
            .debug(format!("Re-created schema {}", self.target_schema).as_str());
        Ok(())
    }
}
