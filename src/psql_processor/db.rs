use sqlx::postgres::Postgres;
use sqlx::Pool;

use crate::config::DbConfig;
use crate::error::{CustomError, CustomResult};

pub async fn get_connections_pool(db_config: &DbConfig) -> CustomResult<Pool<Postgres>> {
    let logger = crate::logger::Logger::new();
    let url = get_url(db_config);
    let pool = Pool::<Postgres>::connect(&url).await;

    match pool {
        Ok(pool) => {
            logger.warn("Created connection Pool for DB");
            Ok(pool)
        }
        Err(err) => {
            logger.error(format!("Can't create connection Pool: {:#?}", err).as_str());
            Err(CustomError::DbConnection)
        }
    }
}

fn get_url(db_config: &DbConfig) -> String {
    let url = format!(
        "postgresql://{}:{}@{}:{}/{}",
        db_config.username, db_config.password, db_config.host, db_config.port, db_config.database
    );

    url
}
