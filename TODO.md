# TODO

- ~~auto create log tables~~
- ~~auto create triggers~~
- create proper Event model for table changes from global table. 
  Read change from log table, delete global log entry, mark log entry processed
- add output sink, http, stdout?
- Test setup/teardown
  - https://markphelps.me/posts/writing-tests-for-your-database-code-in-go/