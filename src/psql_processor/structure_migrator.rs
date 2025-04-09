use std::process::Command;

use regex::Regex;
use sqlx::{FromRow, Pool, Postgres};

use crate::config::Config;
use crate::error::{CustomError, CustomResult};
use crate::psql_processor::db::get_connections_pool;
use crate::traits::StructureMigratorTrait;

use crate::logger::Logger;

use super::table_migrator::TableMigrator;

#[derive(Debug, FromRow)]
struct EnumInfo {
    schema: String,
    enum_name: String,
    enum_values: Vec<String>,
}

#[derive(Debug, Clone, FromRow)]
struct TableInfo {
    schema: String,
    table_name: String,
}

pub struct StructureMigrator {
    pub config: Config,
    pub target_schema: String,
    pub source_schema: String,
    pub source_conn: Pool<Postgres>,
    pub target_conn: Pool<Postgres>,
    pub logger: Logger,
}

impl StructureMigrator {
    pub async fn new(config: Config) -> CustomResult<Self> {
        let logger = Logger::new();
        assert_ne!(config.target.schema, None, "Target schema is not provided");
        assert_ne!(config.source.schema, None, "Source schema is not provided");

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
}

impl StructureMigrator {
    fn migrate_with_pg_dump(&self) -> CustomResult<()> {
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

        Ok(())
    }

    async fn list_all_enums(&self) -> CustomResult<Vec<EnumInfo>> {
        let query = r#"
            SELECT 
                n.nspname as schema,
                t.typname as enum_name,
                array_agg(e.enumlabel ORDER BY e.enumsortorder) as enum_values
            FROM pg_type t
            JOIN pg_enum e ON t.oid = e.enumtypid
            JOIN pg_namespace n ON t.typnamespace = n.oid
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
            GROUP BY n.nspname, t.typname
            ORDER BY n.nspname, t.typname;
        "#;

        let enums: Vec<EnumInfo> = sqlx::query_as(query)
            .fetch_all(&self.source_conn)
            .await
            .map_err(|err| {
                self.logger
                    .error(format!("Failed to fetch enum types: {}", err).as_str());
                self.logger.error(query);
                CustomError::QueryExecution
            })?;

        Ok(enums)
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

    async fn create_enum(&self, enum_info: &EnumInfo) -> CustomResult<()> {
        // Create enum in the target schema
        let values_str = enum_info
            .enum_values
            .iter()
            .map(|v| format!("'{}'", v))
            .collect::<Vec<_>>()
            .join(", ");
        let create_enum_query = format!(
            "DO $$
                        BEGIN
                            IF NOT EXISTS (
                                SELECT 1
                                FROM pg_type t
                                JOIN pg_namespace n ON n.oid = t.typnamespace
                                WHERE t.typname = '{1}'
                                AND n.nspname = '{0}'
                            ) THEN
                                CREATE TYPE {0}.{1} AS ENUM ({2});
                            END IF;
                        END
                        $$;",
            self.target_schema, enum_info.enum_name, values_str
        );

        sqlx::query(&create_enum_query)
            .execute(&self.target_conn)
            .await
            .map_err(|err| {
                self.logger
                    .error(format!("Failed to create enum: {}", err).as_str());
                self.logger.error(create_enum_query.as_str());
                CustomError::QueryExecution
            })?;

        self.logger.debug(
            format!(
                "Created enum {}.{}",
                self.target_schema, enum_info.enum_name
            )
            .as_str(),
        );
        Ok(())
    }

    async fn list_all_tables(&self) -> CustomResult<Vec<TableInfo>> {
        let query = r#"
            SELECT 
                n.nspname as schema,
                c.relname as table_name,
                c.relkind = 'p' as is_partitioned,
                CASE 
                    WHEN c.relkind = 'p' THEN
                        pg_get_expr(c.relpartbound, c.oid)
                    ELSE NULL
                END as partition_key
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r', 'p')  -- 'r' for regular tables, 'p' for partitioned tables
            AND n.nspname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY n.nspname, c.relname;
        "#;

        let tables: Vec<TableInfo> = sqlx::query_as(query)
            .fetch_all(&self.source_conn)
            .await
            .map_err(|err| {
                self.logger
                    .error(format!("Failed to fetch tables: {}", err).as_str());
                self.logger.error(query);
                CustomError::QueryExecution
            })?;

        Ok(tables)
    }
}

impl StructureMigratorTrait for StructureMigrator {
    async fn migrate(&self) -> CustomResult<()> {
        if self.config.technology.use_pg_dump {
            self.logger.info("Using pg_dump to migrate structure");
            self.migrate_with_pg_dump()?;
            return Ok(());
        }

        self.logger.info("Re-creating target schema");
        self.recreate_schema().await?;
        self.logger.info("Re-created target schema");

        self.logger.info("Migrating structure");

        // First migrate all enums
        self.logger.debug("Migrating enums");
        let enums = self.list_all_enums().await?;
        self.logger
            .debug(format!("Found {} enums", enums.len()).as_str());

        for enum_info in enums {
            self.logger.debug(
                format!("Creating enum {}.{}", enum_info.schema, enum_info.enum_name).as_str(),
            );
            self.create_enum(&enum_info).await?;
        }
        self.logger.debug("Migrated enums");

        self.logger.debug("Getting all tables");
        // List all tables
        let tables = self.list_all_tables().await?;
        self.logger
            .debug(format!("Found {} tables to clone:", tables.len()).as_str());

        let mut success = vec![];
        let mut failures = vec![];
        let mut skipped = vec![];

        let table_migrator = TableMigrator::new(&self.config).await?;
        // Clone each table
        for table in tables {
            if table.schema != self.source_schema {
                continue;
            }

            // if !["cb_study_data"].contains(&table.table_name.as_str()) {
            //     self.logger
            //         .debug(format!("Skipping table {}", table.table_name).as_str());
            //     skipped.push(table.clone());
            //     continue;
            // }

            if self.skip_table(&table.table_name) {
                self.logger
                    .debug(format!("Skipping table {}", table.table_name).as_str());
                skipped.push(table.clone());
                continue;
            }

            let res = table_migrator
                .migrate(&table.schema, &table.table_name)
                .await;

            match res {
                Ok(_) => success.push(table),
                Err(e) => {
                    failures.push(table.clone());
                    self.logger.error(
                        format!("Failed to clone table {}: {}", table.table_name, e).as_str(),
                    );
                }
            }
        }

        self.logger
            .info(format!("Successfully cloned {} tables", success.len()).as_str());

        if !skipped.is_empty() {
            self.logger
                .warn(format!("Skipped {} tables", skipped.len()).as_str());
        }
        if !failures.is_empty() {
            self.logger
                .error(format!("Failed to clone {} tables", failures.len()).as_str());
            for table in failures {
                self.logger.error(
                    format!(
                        "Failed to clone table: {}.{}",
                        table.schema, table.table_name
                    )
                    .as_str(),
                );
            }
        }

        Ok(())
    }

    fn is_private_table(&self, table_name: &str) -> bool {
        let internal_tables = [];

        internal_tables.contains(&table_name)
    }

    fn skip_table(&self, table_name: &str) -> bool {
        if self.config.technology.copy_staging_tables.unwrap_or(false) {
            return false;
        }

        let skip_tables = ["test_tab"];
        let pattern = Regex::new(r"^\w+_\d+(_\d+)?$").unwrap();

        pattern.is_match(table_name) || skip_tables.contains(&table_name)
    }
}
