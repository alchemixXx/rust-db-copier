use mysql::*;

use crate::config::DbConfig;
use crate::error::{ CustomResult, CustomError };

pub fn get_connection(db_config: &DbConfig) -> CustomResult<PooledConn> {
    let pool = get_connections_pool(db_config)?;

    let connection = match pool.get_conn() {
        Ok(conn) => {
            println!("Got connection from Pool");
            conn
        }
        Err(err) => {
            println!("Can't get connection from Pool: {:#?}", err);
            return Err(CustomError::DbConnection);
        }
    };

    Ok(connection)
}

pub fn get_connections_pool(db_config: &DbConfig) -> CustomResult<Pool> {
    let url = get_url(db_config);
    let pool = Pool::new(url.as_str());

    match pool {
        Ok(pool) => {
            println!("Created connection Pool for DB");
            Ok(pool)
        }
        Err(err) => {
            println!("Can't create connection Pool: {:#?}", err);
            Err(CustomError::DbConnection)
        }
    }
}

fn get_url(db_config: &DbConfig) -> String {
    let url = format!(
        "mysql://{}:{}@{}:{}/{}",
        db_config.username,
        db_config.password,
        db_config.host,
        db_config.port,
        db_config.database
    );

    url
}
