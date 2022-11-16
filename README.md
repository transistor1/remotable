# Remotable
## Remote virtual tables for sqlite in Python

Remotable is an <a href='https://github.com/rogerbinns/apsw'>APSW</a> shell that allows you to pull remote data sources into your SQLite database.  Remotable is not a C-extension and thus will only work in the Python APSW shell.

### Quick Start
```
$ python remotable.py

apsw_prompt> CREATE VIRTUAL TABLE access_table USING remotable(pyodbc, select * from [mytable], 'DRIVER=Microsoft Access Driver (*.mdb, *.accdb);DBQ=C:/path/to/a/file.accdb');

```

### CREATE VIRTUAL TABLE Syntax:

```
CREATE VIRTUAL TABLE <table name> USING remotetable(<python dbapi driver>, <select statement>, <arg1>, <arg2>, ..., <argN>);
```

#### Parameters:

`python dbapi driver`: A <a href='https://peps.python.org/pep-0249/'>PEP 249-compatible</a> Python DBApi driver, such as pyodbc, teradatasql, ibm_db, sqlite3, etc...

`select statement`: A SELECT SQL statement to describe what data to pull from the remote source

`arg1`...`argN`: A set of arguments, passed to the database driver's `connect` statement, that adhere to the following specifications:
 - if the argument is a string, which uses single quotes, it is interpreted as a positional parameter (`*args` in Python parlance). Examples of strings:
   - `'DSN=DSN1;DATABASE=abc123'`
   - `'DRIVER=Microsoft Access Driver (*.mdb, *.accdb);DBQ=C:/path/to/a/file.accdb'`
 - if the argument is an **assignment**, it is interpreted as a keyword argument (passed to the `connect`'s `**kwargs`). Examples include:
   - `user='username'`
   - `password='abc'`
- if a keyword argument string contains the phrase `<getpass>`, Remotable will prompt the user for a password for that given argument.



