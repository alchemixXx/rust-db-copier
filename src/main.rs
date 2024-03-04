use clap::Parser;
mod config;
mod traits;
mod mysql_processor;
mod error;
mod cli;
use cli::CLi;
use error::CustomResult;
use mysql_processor::migrator::Migrator;

fn main() -> CustomResult<()> {
    println!("Reading cli args...");
    let cli_args = CLi::parse();
    println!("CLI args: {:#?}", cli_args);

    println!("Reading script config");
    let config = config::read_config();
    println!("Read script config");

    if config.technology.category == "mysql" {
        let migrator = Migrator { config };
        migrator.migrate()?;
        return Ok(());
    }

    panic!("Not supported technology received. Only mysql is supported. Exiting.");
}
