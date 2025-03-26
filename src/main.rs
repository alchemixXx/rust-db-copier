use clap::Parser;
mod cli;
mod config;
mod error;
mod logger;
mod mysql_processor;
mod psql_processor;
mod traits;
use cli::CLi;
use error::CustomResult;
use logger::Logger;
use mysql_processor::migrator::Migrator as MysqlMigrator;
use psql_processor::migrator::Migrator as PsqllMigrator;

#[tokio::main]
async fn main() -> CustomResult<()> {
    println!("Reading cli args...");
    let cli_args = CLi::parse();
    println!("CLI args: {:#?}", cli_args);
    let config = config::read_config();

    Logger::init(config.log.log_level);
    if config.technology.category == "mysql" {
        let migrator = MysqlMigrator { config };
        migrator.migrate().await?;
        return Ok(());
    }
    if config.technology.category == "postgres" {
        let migrator = PsqllMigrator { config };
        migrator.migrate().await?;
        return Ok(());
    }

    panic!("Not supported technology received. Only mysql is supported. Exiting.");
}
