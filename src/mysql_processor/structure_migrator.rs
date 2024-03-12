use crate::config::Config;
use crate::mysql_processor::db::get_connection;
use mysql::PooledConn;
use mysql::{ prelude::Queryable, Row };
use crate::traits::StructureMigratorTrait;
use crate::error::{ CustomError, CustomResult };
pub struct StructureMigrator {
    pub config: Config,
}

impl StructureMigrator {
    fn exec_no_output_statement(
        &self,
        connection: &mut PooledConn,
        query: String
    ) -> CustomResult<()> {
        let result = connection.query_drop(query);

        match result {
            Ok(_) => Ok(()),
            Err(err) => Err(CustomError::DbQueryExecution(err.to_string())),
        }
    }

    fn get_tables(&self, connection: &mut PooledConn) -> CustomResult<Vec<String>> {
        let tables: Result<Vec<String>, mysql::Error> = connection.query_map(
            "SHOW TABLES",
            |table_name| table_name
        );

        match tables {
            Ok(data) => Ok(data),
            Err(_) => Err(CustomError::DbTableStructure),
        }
    }

    fn get_create_table_ddl(
        &self,
        connection: &mut PooledConn,
        table: &String
    ) -> CustomResult<String> {
        let ddl_query = format!("SHOW CREATE TABLE `{}`", table);
        let row = connection
            .query_first(ddl_query)
            .map_err(|err| CustomError::DbQueryExecution(err.to_string()))
            .and_then(|maybe_row|
                maybe_row.map_or(Err(CustomError::DbTableStructure), |row: Row| Ok(row))
            )
            .map_err(|_| CustomError::DbTableStructure)?;

        let mut index: Option<usize> = None;
        let columns = row.columns_ref();
        for (i, column) in columns.into_iter().enumerate() {
            if column.name_str() == "Create Table" {
                index = Some(i);
                break;
            }
        }

        let value = (match index {
            None => Err(CustomError::DbTableStructure),
            Some(value) => {
                let query: String = row.get(value).expect("Value should be present in the Roo");

                Ok(query)
            }
        })?;

        Ok(value)
    }
}

impl StructureMigratorTrait for StructureMigrator {
    fn migrate(&self) -> CustomResult<()> {
        println!("Connecting to source database");
        let mut source_conn = get_connection(&self.config.source)?;
        println!("Connected to source database");

        println!("Connecting to target database");
        let mut target_conn = get_connection(&self.config.target)?;
        println!("Connected to target database");

        println!("Reading target tables");
        let target_tables: Vec<String> = self.get_tables(&mut target_conn)?;
        println!("Read target tables: {}", target_tables.len());

        println!("Disabling FK checks");
        self.exec_no_output_statement(&mut target_conn, "SET FOREIGN_KEY_CHECKS = 0".to_string())?;
        println!("Disabled FK checks");

        println!("Dropping target tables");
        for table in &target_tables {
            self.exec_no_output_statement(
                &mut target_conn,
                format!("DROP TABLE IF EXISTS `{}`", table)
            )?;
        }
        println!("Dropped target tables");

        println!("Reading remote tables");
        let source_tables: Vec<String> = self.get_tables(&mut source_conn)?;
        println!("Read remote tables: {}", source_tables.len());

        let mut table_skipped: Vec<&str> = vec![];
        let mut table_processed: Vec<&str> = vec![];

        for table in &source_tables {
            if self.skip_table(&table) || self.is_private_table(&table) {
                table_skipped.push(table);
                continue;
            }

            let create_table_query: String = self.get_create_table_ddl(&mut source_conn, table)?;

            self.exec_no_output_statement(&mut target_conn, create_table_query)?;

            table_processed.push(table);
        }

        println!("Skipped tables: {}", table_skipped.len());
        println!("Processed tables: {}", table_processed.len());

        println!("Enabling FK checks");
        self.exec_no_output_statement(&mut target_conn, "SET FOREIGN_KEY_CHECKS = 1".to_string())?;
        println!("Enabled FK checks");

        Ok(())
    }

    fn is_private_table(&self, table_name: &str) -> bool {
        let internal_tables = vec!["schema_migrations", "ar_internal_metadata"];

        internal_tables.contains(&table_name)
    }
}
