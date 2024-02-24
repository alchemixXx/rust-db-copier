use mysql::*;

use crate::config::DbConfig;

pub fn get_connection(
    db_config: &DbConfig
) -> std::result::Result<PooledConn, Box<dyn std::error::Error>> {
    let url = format!(
        "mysql://{}:{}@{}:{}/{}",
        db_config.username,
        db_config.password,
        db_config.host,
        db_config.port,
        db_config.database
    );
    let pool = Pool::new(url.as_str())?;
    let connection = pool.get_conn()?;

    return Ok(connection);
}

// pub fn get_connections_pool(
//     db_config: &DbConfig
// ) -> std::result::Result<Pool, Box<dyn std::error::Error>> {
//     let url = format!(
//         "mysql://{}:{}@{}:{}/{}",
//         db_config.username,
//         db_config.password,
//         db_config.host,
//         db_config.port,
//         db_config.database
//     );
//     let pool: Pool = Pool::new(url.as_str())?;

//     return Ok(pool);
// }
