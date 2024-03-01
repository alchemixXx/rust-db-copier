pub type CustomResult<T> = core::result::Result<T, CustomError>;

#[derive(Debug)]
pub enum CustomError {
    DbQueryExecution(String),
    DbTableStructure,
    DbConnection,
}

// impl From<T> for CustomErrorCode {
//     fn from(e: T) -> Self {
//         Self::DbQuery(e)
//     }
// }

impl std::error::Error for CustomError {}
impl core::fmt::Display for CustomError {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::result::Result<(), core::fmt::Error> {
        write!(f, "{self:?}")
    }
}
