use crate::{
    config::Config,
    mysql_processor::{ data_migrator::DataMigrator, structure_migrator::StructureMigrator },
    traits::StructureMigratorTrait,
};
use std::time::Instant;

use crate::error::CustomResult;

pub struct Migrator {
    pub config: Config,
}

impl Migrator {
    pub fn migrate(&self) -> CustomResult<()> {
        let struct_migrator = StructureMigrator { config: self.config.clone() };
        let data_migrator = DataMigrator { config: self.config.clone() };

        println!("Migrating structure. start");
        let structure_migration_start_time = Instant::now();
        struct_migrator.migrate()?;
        let structure_migration_end_time = Instant::now();
        let structure_migration_elapsed_time =
            structure_migration_end_time - structure_migration_start_time;
        println!("Migrated structure in {:?}", structure_migration_elapsed_time);

        println!("Migrating data");
        let data_migration_start_time = Instant::now();
        data_migrator.migrate()?;
        let data_migration_end_time = Instant::now();
        let data_migration_elapsed_time = data_migration_end_time - data_migration_start_time;
        println!("Migrated data in {:?}", data_migration_elapsed_time);

        Ok(())
    }
}
