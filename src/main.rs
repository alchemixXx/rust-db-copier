mod config;
mod traits;
mod mysql_processor;
mod error;
use error::CustomResult;
use mysql_processor::migrator::Migrator;

fn main() -> CustomResult<()> {
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
