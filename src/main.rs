mod config;
mod traits;
mod mysql_processor;
use mysql_processor::migrator::Migrator;

fn main() {
    println!("Reading script config");
    let config = config::read_config();
    println!("Read script config");

    if config.technology.category == "mysql" {
        let migrator = Migrator { config };
        migrator.migrate();
        return;
    }

    panic!("Not supported technology received. Only mysql is supported. Exiting.");
}
