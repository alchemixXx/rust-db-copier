use regex::Regex;
use crate::error::CustomResult;

pub trait StructureMigratorTrait {
    fn migrate(&self) -> CustomResult<()>;

    fn is_private_table(&self, table_name: &str) -> bool;

    fn skip_table(&self, table_name: &str) -> bool {
        let pattern = Regex::new(r"^\w+_\d+(_\d+)?(_\w+)?$").unwrap();

        let result = pattern.is_match(table_name);

        result
    }
}
