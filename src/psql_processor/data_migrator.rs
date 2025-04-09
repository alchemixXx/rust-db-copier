// SELECT 'INSERT INTO your_table VALUES (' ||
//        string_agg(quote_literal(t.*), ', ') || ');'
// FROM your_table t;

use sqlx::{postgres::PgRow, Pool, Postgres, Row};

use crate::{
    config::Config, error::CustomError, logger::Logger, psql_processor::db::get_connections_pool,
    CustomResult,
};
pub struct DataMigrator {
    pub config: Config,
}

impl DataMigrator {
    pub async fn migrate(&self) -> CustomResult<()> {
        let logger = Logger::new();
        logger.info("Connecting to source database");
        let source_conn = get_connections_pool(&self.config.source).await?;
        logger.info("Connected to source database");

        logger.info("Connecting to target database");
        let target_conn = get_connections_pool(&self.config.target).await?;
        logger.info("Connected to target database");

        for table in &self.config.tables.data_source {
            logger.info(format!("Migrating data for table: {}", table).as_str());
            let data = self.get_insert_string(&source_conn, table).await?;

            logger.info(format!("Executing insert query: {}", data).as_str());

            self.execute_insert(&target_conn, data).await?;

            logger.info(format!("Migrated data for table: {}", table).as_str());
        }
        Ok(())
    }

    async fn execute_insert(&self, pool: &Pool<Postgres>, query: String) -> CustomResult<()> {
        let logger = Logger::new();
        sqlx::query(query.as_str())
            .execute(pool)
            .await
            .map_err(|e| {
                logger.error(e.to_string().as_str());
                CustomError::QueryExecution
            })?;

        Ok(())
    }

    async fn get_insert_string(&self, pool: &Pool<Postgres>, table: &str) -> CustomResult<String> {
        let logger = Logger::new();
        let query = format!(
            "SELECT 
            'INSERT INTO {1}.{0} VALUES (' || string_agg(quote_literal(t.*), ', ') || ');'  as query
            FROM {0} t;",
            table,
            self.config.target.schema.as_ref().unwrap()
        );
        let res = sqlx::query(query.as_str())
            .fetch_all(pool)
            .await
            .map_err(|e| {
                logger.error(e.to_string().as_str());
                CustomError::QueryExecution
            })?
            .into_iter()
            .map(|row: PgRow| row.get::<String, _>("query"))
            .collect::<Vec<String>>();

        Ok(res.join("\n"))
    }
}
