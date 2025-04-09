use sqlx::{Pool, Postgres, Row};

use crate::config::Config;
use crate::error::{CustomError, CustomResult};
use crate::logger::Logger;

use super::db::get_connections_pool;

pub struct TableMigrator {
    pub source_conn: Pool<Postgres>,
    pub target_conn: Pool<Postgres>,
    pub target_schema: String,
    pub logger: Logger,
}

impl TableMigrator {
    pub async fn new(config: &Config) -> CustomResult<Self> {
        let logger = Logger::new();
        let source_conn = get_connections_pool(&config.source).await?;
        let target_conn = get_connections_pool(&config.target).await?;

        Ok(Self {
            target_schema: config.target.schema.clone().unwrap(),
            source_conn,
            target_conn,
            logger,
        })
    }
}

impl TableMigrator {
    pub async fn migrate(&self, schema: &str, table: &str) -> CustomResult<()> {
        self.logger
            .info(format!("Cloning table {}.{}", schema, table).as_str());

        // Handle sequences
        self.migrate_sequences(schema, table).await?;

        // Handle table creation
        self.migrate_table_structure(schema, table).await?;

        // Handle partitions
        self.migrate_partitions(schema, table).await?;

        // Handle indexes
        self.migrate_indexes(schema, table).await?;

        // Handle constraints
        self.migrate_constraints(schema, table).await?;

        self.logger
            .debug(format!("Successfully cloned table {}.{}", schema, table).as_str());
        Ok(())
    }

    async fn migrate_sequences(&self, schema: &str, table: &str) -> CustomResult<()> {
        self.logger
            .debug(format!("Getting table sequences for table {}.{}", schema, table).as_str());
        let sequences = self.get_table_sequences(schema, table).await?;
        self.logger
            .debug(format!("Got sequences for table {}.{}", schema, table).as_str());

        self.logger
            .debug(format!("Creating sequences for table {}.{}", schema, table).as_str());
        for sequence in &sequences {
            let (seq_schema, seq_name) = self.extract_sequence_parts(sequence, schema);
            self.create_sequence(seq_schema, seq_name).await?;
        }
        self.logger
            .debug(format!("Created sequences for table {}.{}", schema, table).as_str());
        Ok(())
    }

    fn extract_sequence_parts<'a>(
        &self,
        sequence: &'a str,
        default_schema: &'a str,
    ) -> (&'a str, &'a str) {
        let parts: Vec<&str> = sequence.split('.').collect();
        if parts.len() > 1 {
            (parts[0], parts[1])
        } else {
            (default_schema, sequence)
        }
    }

    async fn migrate_table_structure(&self, schema: &str, table: &str) -> CustomResult<()> {
        self.logger
            .debug(format!("Getting DDL for table {}.{}", schema, table).as_str());
        let table_ddl = self.get_table_ddl(schema, table).await?;
        self.logger
            .debug(format!("Got DDL for table {}.{}", schema, table).as_str());
        self.logger
            .debug(format!("Original DDL: {}", table_ddl).as_str());

        let modified_ddl = self.prepare_table_ddl(schema, table_ddl);
        self.logger
            .debug(format!("Modified DDL: {}", modified_ddl).as_str());

        self.logger
            .debug(format!("Creating table {}.{}", schema, table).as_str());
        sqlx::query(&modified_ddl)
            .execute(&self.target_conn)
            .await
            .map_err(|err| {
                self.logger
                    .error(format!("Failed to create table: {}", err).as_str());
                self.logger.error(&modified_ddl);
                CustomError::QueryExecution
            })?;

        self.logger
            .debug(format!("Created table {}.{}", schema, table).as_str());
        Ok(())
    }

    fn prepare_table_ddl(&self, schema: &str, ddl: String) -> String {
        let modified_ddl = if schema == "public" {
            ddl.replace("public.", format!("{}.", self.target_schema).as_str())
        } else {
            ddl
        };
        self.clean_type_references(modified_ddl)
    }

    async fn migrate_partitions(&self, schema: &str, table: &str) -> CustomResult<()> {
        self.logger
            .debug(format!("Getting partitions for table {}.{}", schema, table).as_str());
        let partitions = self.get_partition_ddl(schema, table).await?;
        self.logger
            .debug(format!("Got partitions for table {}.{}", schema, table).as_str());

        self.logger
            .debug(format!("Creating partitions for table {}.{}", schema, table).as_str());
        for partition_ddl in partitions {
            let modified_ddl = self.prepare_ddl(schema, partition_ddl);
            sqlx::query(&modified_ddl)
                .execute(&self.target_conn)
                .await
                .map_err(|err| {
                    self.logger
                        .error(format!("Failed to create partition: {}", err).as_str());
                    CustomError::QueryExecution
                })?;
        }
        self.logger
            .debug(format!("Created partitions for table {}.{}", schema, table).as_str());
        Ok(())
    }

    async fn migrate_indexes(&self, schema: &str, table: &str) -> CustomResult<()> {
        self.logger
            .debug(format!("Getting indexes for table {}.{}", schema, table).as_str());
        let indexes = self.get_index_ddl(schema, table).await?;
        self.logger
            .debug(format!("Got indexes for table {}.{}", schema, table).as_str());

        self.logger
            .debug(format!("Creating indexes for table {}.{}", schema, table).as_str());
        for index_ddl in indexes {
            let modified_ddl = self.prepare_ddl(schema, index_ddl);
            sqlx::query(&modified_ddl)
                .execute(&self.target_conn)
                .await
                .map_err(|err| {
                    self.logger
                        .error(format!("Failed to create index: {}", err).as_str());
                    CustomError::QueryExecution
                })?;
        }
        self.logger
            .debug(format!("Created indexes for table {}.{}", schema, table).as_str());
        Ok(())
    }

    async fn migrate_constraints(&self, schema: &str, table: &str) -> CustomResult<()> {
        self.logger
            .debug(format!("Getting constraints for table {}.{}", schema, table).as_str());
        let constraints = self.get_constraint_ddl(schema, table).await?;
        self.logger
            .debug(format!("Got constraints for table {}.{}", schema, table).as_str());

        self.logger
            .debug(format!("Creating constraints for table {}.{}", schema, table).as_str());
        for constraint_ddl in constraints {
            let modified_ddl = self.prepare_ddl(schema, constraint_ddl);
            let constraint_name = self.extract_constraint_name(&modified_ddl);

            match sqlx::query(&modified_ddl).execute(&self.target_conn).await {
                Ok(_) => self.logger.debug(
                    format!(
                        "Added constraint {} to table {}.{}",
                        constraint_name, self.target_schema, table
                    )
                    .as_str(),
                ),
                Err(e) => {
                    // Check if the error is because the constraint already exists
                    if e.to_string().contains("already exists") {
                        self.logger.debug(
                            format!(
                                "Constraint {} already exists on table {}.{}, skipping",
                                constraint_name, self.target_schema, table
                            )
                            .as_str(),
                        );
                    } else {
                        // If it's a different error, return it
                        self.logger.error(&modified_ddl);
                        self.logger
                            .error(format!("Failed to create constraint: {}", e).as_str());
                        return Err(CustomError::QueryExecution);
                    }
                }
            }
        }
        self.logger
            .debug(format!("Created constraints for table {}.{}", schema, table).as_str());
        Ok(())
    }

    fn prepare_ddl(&self, schema: &str, ddl: String) -> String {
        if schema == "public" {
            ddl.replace("public.", format!("{}.", self.target_schema).as_str())
        } else {
            ddl
        }
    }

    async fn get_table_ddl(&self, schema: &str, table: &str) -> CustomResult<String> {
        self.logger
            .debug(format!("Getting DDL for table {}.{}", schema, table).as_str());

        let query = r#"
            WITH column_info AS (
                SELECT DISTINCT ON (c.column_name)
                    c.column_name,
                    c.table_schema,
                    c.table_name,
                    CASE 
                        WHEN c.data_type = 'USER-DEFINED' THEN
                            format('%I.%s', $1, 
                                (SELECT t.typname 
                                 FROM pg_type t 
                                 JOIN pg_namespace n ON t.typnamespace = n.oid 
                                 WHERE t.oid = a.atttypid)
                            )
                        ELSE c.data_type
                    END as data_type,
                    c.character_maximum_length,
                    c.is_nullable,
                    CASE 
                        WHEN c.column_default LIKE 'nextval(%' THEN
                            format('nextval(''%I.%s''::regclass)', 
                                $1, 
                                regexp_replace(c.column_default, 'nextval\(''([^'']+)''::regclass\)', '\1')
                            )
                        WHEN c.column_default LIKE '%::%' THEN
                            regexp_replace(
                                c.column_default,
                                '::([^'']+)',
                                format('::%I.\1', $1)
                            )
                        ELSE c.column_default
                    END as column_default,
                    c.ordinal_position
                FROM information_schema.columns c
                JOIN pg_class cl ON cl.relname = c.table_name
                JOIN pg_namespace n ON n.nspname = c.table_schema
                JOIN pg_attribute a ON a.attrelid = cl.oid AND a.attname = c.column_name
                WHERE c.table_schema = $1 AND c.table_name = $2
                ORDER BY c.column_name, c.ordinal_position
            )
            SELECT 
                'CREATE TABLE ' || quote_ident($1) || '.' || quote_ident($2) || ' (' ||
                string_agg(
                    quote_ident(column_name) || ' ' || data_type ||
                    CASE 
                        WHEN character_maximum_length IS NOT NULL 
                        THEN '(' || character_maximum_length || ')'
                        ELSE ''
                    END ||
                    CASE 
                        WHEN is_nullable = 'NO' THEN ' NOT NULL'
                        ELSE ''
                    END ||
                    CASE 
                        WHEN column_default IS NOT NULL 
                        THEN ' DEFAULT ' || column_default
                        ELSE ''
                    END,
                    ', '
                    ORDER BY ordinal_position
                ) || ');'
            FROM column_info
            GROUP BY table_schema, table_name;
        "#;

        let ddl = sqlx::query_scalar::<_, Option<String>>(query)
            .bind(schema)
            .bind(table)
            .fetch_optional(&self.source_conn)
            .await
            .map_err(|err| {
                self.logger
                    .error(format!("Failed to get table DDL: {}", err).as_str());
                self.logger.error(query);
                CustomError::QueryExecution
            })?;

        let ddl_str = match ddl {
            Some(Some(ddl)) => ddl,
            _ => {
                self.logger
                    .error(format!("No DDL returned for table {}.{}", schema, table).as_str());
                return Err(CustomError::DbTableStructure);
            }
        };

        if ddl_str.is_empty() {
            self.logger
                .error(format!("Empty DDL returned for table {}.{}", schema, table).as_str());
            return Err(CustomError::DbTableStructure);
        }

        self.logger
            .debug(format!("Generated DDL: {}", ddl_str).as_str());

        Ok(ddl_str)
    }

    async fn get_partition_ddl(&self, schema: &str, table: &str) -> CustomResult<Vec<String>> {
        let query = r#"
            SELECT 
                'CREATE TABLE ' || quote_ident(child_ns.nspname) || '.' || quote_ident(child.relname) || 
                ' PARTITION OF ' || quote_ident(parent_ns.nspname) || '.' || quote_ident(parent.relname) || 
                ' FOR VALUES ' || pg_get_expr(child.relpartbound, child.oid) || ';' as partition_ddl
            FROM pg_inherits
            JOIN pg_class parent ON parent.oid = inhrelid
            JOIN pg_class child ON child.oid = inhparent
            JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
            JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace
            WHERE parent_ns.nspname = $1 AND parent.relname = $2;
        "#;

        let partitions: Vec<String> = sqlx::query_scalar(query)
            .bind(schema)
            .bind(table)
            .fetch_all(&self.source_conn)
            .await
            .map_err(|err| {
                self.logger
                    .error(format!("Failed to get partitions: {}", err).as_str());
                self.logger.error(query);
                CustomError::QueryExecution
            })?;

        Ok(partitions)
    }

    async fn get_index_ddl(&self, schema: &str, table: &str) -> CustomResult<Vec<String>> {
        let query = r#"
            SELECT 
                indexdef || ';' as index_ddl
            FROM pg_indexes
            WHERE schemaname = $1 AND tablename = $2;
        "#;

        let indexes: Vec<String> = sqlx::query_scalar(query)
            .bind(schema)
            .bind(table)
            .fetch_all(&self.source_conn)
            .await
            .map_err(|err| {
                self.logger
                    .error(format!("Failed to get indexes: {}", err).as_str());
                self.logger.error(query);
                CustomError::QueryExecution
            })?;

        Ok(indexes)
    }

    async fn get_constraint_ddl(&self, schema: &str, table: &str) -> CustomResult<Vec<String>> {
        let query = r#"
        WITH constraint_info AS (
            SELECT 
                con.oid,
                con.conname,
                con.contype,
                con.conrelid,
                con.confrelid,
                con.conkey,
                con.confkey,
                con.condeferrable,
                con.condeferred,
                con.convalidated,
                pg_get_constraintdef(con.oid) as constraint_def,
                CASE 
                    WHEN con.contype = 'f' THEN
                        -- For foreign key constraints, ensure referenced table is schema-qualified
                        regexp_replace(
                            pg_get_constraintdef(con.oid),
                            'REFERENCES ([^"''\s]+)',
                            'REFERENCES ' || quote_ident($1) || '.' || '\1'
                        )
                    ELSE
                        pg_get_constraintdef(con.oid)
                END as modified_constraint_def
            FROM pg_constraint con
            INNER JOIN pg_class cl ON cl.oid = con.conrelid
            INNER JOIN pg_namespace nsp ON nsp.oid = cl.relnamespace
            WHERE nsp.nspname = $1 AND cl.relname = $2
        )
        SELECT 
            'ALTER TABLE ' || quote_ident($1) || '.' || quote_ident($2) ||
            ' ADD CONSTRAINT ' || quote_ident(conname) || ' ' || 
            modified_constraint_def || 
            CASE 
                WHEN condeferrable THEN ' DEFERRABLE'
                ELSE ''
            END ||
            CASE 
                WHEN condeferred THEN ' INITIALLY DEFERRED'
                ELSE ''
            END ||
            CASE 
                WHEN NOT convalidated THEN ' NOT VALID'
                ELSE ''
            END || ';' as constraint_ddl
        FROM constraint_info;
    "#;

        let constraints: Vec<String> = sqlx::query_scalar(query)
            .bind(schema)
            .bind(table)
            .fetch_all(&self.source_conn)
            .await
            .map_err(|err| {
                self.logger
                    .error(format!("Failed to get constraints: {}", err).as_str());
                self.logger.error(query);
                CustomError::QueryExecution
            })?;

        Ok(constraints)
    }

    async fn get_sequence_ddl(&self, schema: &str, sequence: &str) -> CustomResult<String> {
        let query = r#"
            SELECT 
                'CREATE SEQUENCE ' || quote_ident($1) || '.' || quote_ident($2) ||
                ' INCREMENT ' || COALESCE(increment::text, '1') ||
                ' MINVALUE ' || COALESCE(minimum_value::text, '1') ||
                ' MAXVALUE ' || COALESCE(maximum_value::text, '9223372036854775807') ||
                ' START ' || COALESCE(start_value::text, '1') || ';' as create_sequence_ddl
            FROM information_schema.sequences
            WHERE sequence_schema = $1 AND sequence_name = $2;
        "#;

        match sqlx::query_scalar(query)
            .bind(schema)
            .bind(sequence)
            .fetch_optional(&self.source_conn)
            .await
            .map_err(|err| {
                self.logger
                    .error(format!("Failed to get sequence DDL: {}", err).as_str());
                self.logger.error(query);
                CustomError::QueryExecution
            })? {
            Some(ddl) => {
                self.logger
                    .debug(format!("Got sequence DDL: {}", ddl).as_str());
                Ok(ddl)
            }
            None => {
                // If sequence doesn't exist in information_schema, create a default sequence
                self.logger.warn(
                    format!(
                        "Sequence {}.{} not found in information_schema, creating default sequence",
                        schema, sequence
                    )
                    .as_str(),
                );
                let default_ddl = format!(
                    "CREATE SEQUENCE {}.{} INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1;",
                    schema, sequence
                );
                self.logger
                    .warn(format!("Using default sequence DDL: {}", default_ddl).as_str());
                Ok(default_ddl)
            }
        }
    }

    async fn create_sequence(&self, schema: &str, sequence: &str) -> CustomResult<()> {
        // Clean up the sequence name if it contains nextval or regclass
        let clean_sequence = sequence
            .replace("nextval('", "")
            .replace("'::regclass)", "")
            .replace("'", "");

        self.logger.debug(
            format!(
                "Creating sequence: {}.{}",
                self.target_schema, clean_sequence
            )
            .as_str(),
        );

        let sequence_exists_query = r#"
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.sequences
                WHERE sequence_schema = $1
                AND sequence_name = $2
            );
            "#;
        // Check if sequence already exists
        let sequence_exists = sqlx::query_scalar(sequence_exists_query)
            .bind(&self.target_schema)
            .bind(&clean_sequence)
            .fetch_one(&self.target_conn)
            .await
            .map_err(|err| {
                self.logger
                    .error(format!("Failed to check sequence existence: {}", err).as_str());
                self.logger.error(sequence_exists_query);
                CustomError::QueryExecution
            })?;

        if sequence_exists {
            self.logger.debug(
                format!(
                    "Sequence {}.{} already exists, skipping",
                    self.target_schema, &clean_sequence
                )
                .as_str(),
            );
            return Ok(());
        }

        // Get sequence DDL from source
        let sequence_ddl = self.get_sequence_ddl(schema, &clean_sequence).await?;

        self.logger
            .debug(format!("Source sequence DDL: {}", sequence_ddl).as_str());

        // Replace schema in DDL if needed
        let modified_ddl =
            sequence_ddl.replace(&format!("{}.", schema), &format!("{}.", self.target_schema));

        self.logger
            .debug(format!("Modified sequence DDL: {}", modified_ddl).as_str());

        // Create sequence in target
        sqlx::query(&modified_ddl)
            .execute(&self.target_conn)
            .await
            .map_err(|err| {
                self.logger
                    .error(format!("Failed to create sequence: {}", err).as_str());
                self.logger.error(&modified_ddl);
                CustomError::QueryExecution
            })?;

        self.logger
            .debug(format!("Created sequence {}.{}", self.target_schema, clean_sequence).as_str());
        Ok(())
    }

    async fn get_table_sequences(&self, schema: &str, table: &str) -> CustomResult<Vec<String>> {
        let query = r#"
            SELECT DISTINCT
                column_default,
                CASE
                    WHEN column_default LIKE 'nextval(%' THEN
                        CASE
                            WHEN column_default LIKE 'nextval(''%.%'')' THEN
                                -- Extract schema.sequence from nextval('schema.sequence'::regclass)
                                regexp_replace(column_default, 'nextval\(''([^'']+)''\)', '\1')
                            ELSE
                                -- Extract just sequence name from nextval('sequence'::regclass)
                                $1 || '.' || regexp_replace(column_default, 'nextval\(''([^'']+)''\)', '\1')
                        END
                    ELSE NULL
                END as sequence_name
            FROM information_schema.columns
            WHERE table_schema = $1 
            AND table_name = $2
            AND column_default LIKE 'nextval(%';
        "#;

        let rows = sqlx::query(query)
            .bind(schema)
            .bind(table)
            .fetch_all(&self.source_conn)
            .await
            .map_err(|err| {
                self.logger
                    .error(format!("Failed to get table sequences: {}", err).as_str());
                self.logger.error(query);
                CustomError::QueryExecution
            })?;

        let mut sequences = Vec::new();
        for row in rows {
            let default: String = row.get(0);
            let seq_name: Option<String> = row.get(1);
            self.logger
                .debug(format!("Found column default: {}", default).as_str());
            if let Some(seq) = seq_name {
                self.logger
                    .debug(format!("Extracted sequence name: {}", seq).as_str());
                sequences.push(seq);
            }
        }

        self.logger.debug(
            format!(
                "Found {} sequences for table {}.{}",
                sequences.len(),
                schema,
                table
            )
            .as_str(),
        );

        Ok(sequences)
    }

    fn clean_type_references(&self, ddl: String) -> String {
        let mut cleaned_ddl = ddl;
        let built_in_types = [
            "text",
            "varchar",
            "character varying",
            "bigint",
            "integer",
            "int4",
            "int8",
            "jsonb",
            "timestamp",
            "boolean",
            "bool",
            "date",
            "time",
            "uuid",
            "numeric",
            "double precision",
            "real",
            "smallint",
            "interval",
            "bytea",
            "inet",
            "cidr",
            "macaddr",
            "money",
            "point",
            "line",
            "lseg",
            "box",
            "path",
            "polygon",
            "circle",
        ];

        for type_name in built_in_types.iter() {
            let pattern = format!("{}.{}", self.target_schema, type_name);
            cleaned_ddl = cleaned_ddl.replace(&pattern, type_name);
        }

        cleaned_ddl
    }

    // Helper function to extract constraint name from DDL
    fn extract_constraint_name(&self, ddl: &str) -> String {
        // First try to extract the constraint name directly
        if let Some(start) = ddl.find("CONSTRAINT ") {
            if let Some(end) = ddl[start..].find(" ") {
                if start + 11 < start + end {
                    return ddl[start + 11..start + end].to_string();
                }
            }
        }

        // If that fails, try to extract based on constraint type
        if ddl.contains("PRIMARY KEY") {
            // For primary key constraints, extract table name and append _pk
            if let Some(table_start) = ddl.find("TABLE ") {
                if let Some(table_end) = ddl[table_start..].find(" ADD") {
                    let table_name = ddl[table_start + 6..table_start + table_end].trim();
                    // Remove schema prefix if present
                    let table_name = if let Some(schema_end) = table_name.find('.') {
                        &table_name[schema_end + 1..]
                    } else {
                        table_name
                    };
                    return format!("{}_pk", table_name);
                }
            }
        } else if ddl.contains("FOREIGN KEY") {
            // For foreign key constraints, extract table name and append _fk
            if let Some(table_start) = ddl.find("TABLE ") {
                if let Some(table_end) = ddl[table_start..].find(" ADD") {
                    let table_name = ddl[table_start + 6..table_start + table_end].trim();
                    // Remove schema prefix if present
                    let table_name = if let Some(schema_end) = table_name.find('.') {
                        &table_name[schema_end + 1..]
                    } else {
                        table_name
                    };
                    return format!("{}_fk", table_name);
                }
            }
        } else if ddl.contains("UNIQUE") {
            // For unique constraints, extract table name and append _unique
            if let Some(table_start) = ddl.find("TABLE ") {
                if let Some(table_end) = ddl[table_start..].find(" ADD") {
                    let table_name = ddl[table_start + 6..table_start + table_end].trim();
                    // Remove schema prefix if present
                    let table_name = if let Some(schema_end) = table_name.find('.') {
                        &table_name[schema_end + 1..]
                    } else {
                        table_name
                    };
                    return format!("{}_unique", table_name);
                }
            }
        }

        // If all else fails, generate a generic constraint name
        "unknown_constraint".to_string()
    }
}
