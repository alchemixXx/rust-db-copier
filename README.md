# Refactoring plans

- add ability to provide config path as named CLI arg
- add logger with levels and ability to enable/disable logging from config
- add error handling (replace panic! with proper error)
- rewrite logic to perfor tables migration not sequentially, but in batches
- switch from connection to Pool
