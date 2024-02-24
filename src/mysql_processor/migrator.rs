use crate::{
    config::Config,
    mysql_processor::{ data_migrator::DataMigrator, structure_migrator::StructureMigrator },
    traits::StructureMigratorTrait,
};

pub struct Migrator {
    pub config: Config,
}

impl Migrator {
    pub fn migrate(&self) {
        let struct_migrator = StructureMigrator { config: self.config.clone() };
        let data_migrator = DataMigrator { config: self.config.clone() };

        println!("Migrating structure");
        struct_migrator.migrate();
        println!("Migrated structure");

        println!("Migrating data");
        data_migrator.migrate();
        println!("Migrated data");
    }
}
