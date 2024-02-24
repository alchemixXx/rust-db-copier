use crate::config::Config;
use crate::mysql_processor::db::get_connection;
use mysql::{ prelude::Queryable, Row };
use crate::traits::StructureMigratorTrait;
pub struct StructureMigrator {
    pub config: Config,
}

impl StructureMigratorTrait for StructureMigrator {
    fn migrate(&self) {
        println!("Connecting to source database");
        let mut source_conn = get_connection(&self.config.source).unwrap();
        println!("Connected to source database");

        println!("Connecting to target database");
        let mut target_conn = get_connection(&self.config.target).unwrap();
        println!("Connected to target database");

        println!("Reading target tables");
        let target_tables: Vec<String> = target_conn
            .query_map("SHOW TABLES", |table_name| table_name)
            .unwrap();
        println!("Read target tables: {}", target_tables.len());

        println!("Disabling FK checks");
        target_conn.query_drop("SET FOREIGN_KEY_CHECKS = 0").unwrap();
        println!("Disabled FK checks");

        println!("Dropping target tables");
        for table in &target_tables {
            target_conn.query_drop(format!("DROP TABLE IF EXISTS `{}`", table)).unwrap();
        }
        println!("Dropped target tables");

        println!("Reading remote tables");
        let source_tables: Vec<String> = source_conn
            .query_map("SHOW TABLES", |table_name| table_name)
            .unwrap();
        println!("Read remote tables: {}", source_tables.len());

        let mut table_skipped: Vec<&str> = vec![];
        let mut table_processed: Vec<&str> = vec![];

        for table in &source_tables {
            if self.skip_table(&table) || self.is_private_table(&table) {
                table_skipped.push(table);
                continue;
            }

            let ddl_query = format!("SHOW CREATE TABLE `{}`", table);
            let ddl_statement: Option<Row> = source_conn.query_first(ddl_query).unwrap();

            if ddl_statement.is_none() {
                println!("DDL statement is empty for table: {}", table);
                continue;
            }

            let create_table_query: String = ddl_statement.unwrap().get(1).unwrap();

            target_conn.query_drop(&create_table_query).unwrap();

            table_processed.push(table);
        }

        println!("Skipped tables: {}", table_skipped.len());
        println!("Processed tables: {}", table_processed.len());

        println!("Enabling FK checks");
        target_conn.query_drop("SET FOREIGN_KEY_CHECKS = 1").unwrap();
        println!("Enabled FK checks");
    }

    fn is_private_table(&self, table_name: &str) -> bool {
        let internal_tables = vec!["schema_migrations", "ar_internal_metadata"];

        return internal_tables.contains(&table_name);
    }
}
