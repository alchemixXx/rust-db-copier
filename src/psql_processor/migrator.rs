use crate::logger::Logger;
use crate::psql_processor::data_migrator::DataMigrator;
use crate::{
    config::Config, psql_processor::structure_migrator::StructureMigrator,
    traits::StructureMigratorTrait,
};
use std::time::Instant;

use crate::error::CustomResult;

use super::pg_dump_migrator::PgDumpMigrator;

pub struct Migrator {
    pub config: Config,
}

impl Migrator {
    pub async fn migrate(&self) -> CustomResult<()> {
        assert_ne!(
            self.config.target.schema, None,
            "Target schema is not provided"
        );
        assert_ne!(
            self.config.source.schema, None,
            "Source schema is not provided"
        );

        let logger = Logger::new();

        if self.config.technology.copy_structure {
            logger.info("Migrating structure. start");
            let structure_migration_start_time = Instant::now();
            self.migrate_structure().await?;
            let structure_migration_end_time = Instant::now();
            let structure_migration_elapsed_time =
                structure_migration_end_time - structure_migration_start_time;
            logger.info(
                format!(
                    "Migrated structure in {:?}",
                    structure_migration_elapsed_time,
                )
                .as_str(),
            );
        } else {
            logger.warn("Skipping structure migration");
        }

        if self.config.technology.copy_data {
            logger.info("Migrating data");
            let data_migration_start_time = Instant::now();
            self.migrate_data().await?;
            let data_migration_end_time = Instant::now();
            let data_migration_elapsed_time = data_migration_end_time - data_migration_start_time;
            logger.info(format!("Migrated data in {:?}", data_migration_elapsed_time).as_str());
        } else {
            logger.warn("Skipping data migration");
        }

        Ok(())
    }

    async fn migrate_structure(&self) -> CustomResult<()> {
        if self.config.technology.use_pg_dump {
            let pg_dump_migrator = PgDumpMigrator::new(self.config.clone()).await?;
            pg_dump_migrator.migrate_structure().await?;
        } else {
            let struct_migrator = StructureMigrator::new(self.config.clone()).await?;
            struct_migrator.migrate().await?;
        }

        Ok(())
    }

    async fn migrate_data(&self) -> CustomResult<()> {
        if self.config.technology.use_pg_dump {
            let pg_dump_migrator = PgDumpMigrator::new(self.config.clone()).await?;
            pg_dump_migrator.migrate_data().await?;
        } else {
            let data_migrator = DataMigrator::init(self.config.clone()).await?;
            data_migrator.migrate().await?;
        }

        Ok(())
    }
}
