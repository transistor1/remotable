# Remotable
## Remote virtual tables for SQLite in Python

Remotable is an <a href='https://github.com/rogerbinns/apsw'>APSW</a> shell that allows you to pull remote data sources into your SQLite database.  Remotable is not a C-extension and thus will only work in the Python APSW shell.

### Quick Start

The below example uses `pyodbc` to connect to a Microsoft Access database on Windows.  You could also use any DBApi compliant driver, as long as you know its `connect` parameters.
```
$ python -m pip install remotable pyodbc
$ python -m remotable [database name]

apsw_prompt> CREATE VIRTUAL TABLE access_table USING remotable(pyodbc, mytable, 'DRIVER=Microsoft Access Driver (*.mdb, *.accdb);DBQ=C:/path/to/a/file.accdb', querytype=table);

```

### CREATE VIRTUAL TABLE Syntax:

```
CREATE VIRTUAL TABLE <table name> USING remotetable(<python dbapi driver>, <select statement>, <arg1>, <arg2>, ..., <argN>, <querytype>, <indices>);
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

### Special arguments:

After the `arg1`...`argN` parameters, there are some special keywords. You can optionally provide these special arguments after the `connect` arguments.
  - `indices`/`indexes`: Manually specify indexed columns, and provide a relative "cost" for using each index
    - Format: `indices=<index1>:<cost1>|<index2>:<cost2>|...|<indexN>:<costN>
    - Example: `indices=field1:300|field2:200`
    - It doesn't matter what numbers you use for the costs; just make them relatively lower or higher to each other to indicate how "expensive" using that index might be.
      - It is best if indices correspond to the actual indices in your remote source table
      - Give indices that have more uniqueness lower numbers than ones that are less unique
  - `querytype`: Either `table`, or `query`.  If not provided, the default is `table`.  This means that the query you provided corresponds to a table name, and is not actual SQL.



