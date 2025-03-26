use std::process::Command;

use crate::config::Config;
use crate::error::{CustomError, CustomResult};
use crate::psql_processor::db::get_connections_pool;
use crate::traits::StructureMigratorTrait;

use crate::logger::Logger;
pub struct StructureMigrator {
    pub config: Config,
}

impl StructureMigrator {
    fn migrate_with_pg_dump(&self) -> CustomResult<()> {
        let logger = Logger::new();
        let command = format!(
            "PGPASSWORD='{0}' pg_dump -U {1} -h {2} -d {3} --schema={4} --schema-only | PGPASSWORD='{8}' psql -U {5} -d {6} -h {7}",
            self.config.source.password,
            self.config.source.username,
            self.config.source.host,
            self.config.source.database,
            self.config.source.schema.as_ref().unwrap(),
            self.config.target.username,
            self.config.target.database,
            self.config.target.host,
            self.config.target.password
        );

        let output = Command::new("zsh")
            .arg("-c")
            .arg(&command)
            .output()
            .map_err(|err| {
                logger.error(format!("Failed to execute pg_dump command: {}", err).as_str());
                CustomError::CommandExecution
            })?;

        if !output.status.success() {
            logger.error(format!("Failed execute pg_dump command: {}", command).as_str());
            logger.error(format!("Error: {}", String::from_utf8_lossy(&output.stderr)).as_str());

            return Err(CustomError::CommandExecution);
        }

        Ok(())
    }
}

impl StructureMigratorTrait for StructureMigrator {
    async fn migrate(&self) -> CustomResult<()> {
        let logger = Logger::new();
        logger.info("Connecting to source database");
        let _source_conn = get_connections_pool(&self.config.source).await?;
        logger.info("Connected to source database");

        logger.info("Connecting to target database");
        let _target_conn = get_connections_pool(&self.config.target).await?;
        logger.info("Connected to target database");

        logger.info("Migrating structure with pg_dump");
        self.migrate_with_pg_dump()?;
        logger.info("Migrated structure with pg_dump");

        Ok(())
    }

    fn is_private_table(&self, table_name: &str) -> bool {
        let internal_tables = [];

        internal_tables.contains(&table_name)
    }
}
