import datetime
import decimal
import importlib
import getpass
import re

import apsw


SQL_MODE_TABLE = 1
SQL_MODE_QUERY = 2
REMOTE_TABLE_INFO = '_remote_table_info'
REMOTE_TABLE_METADATA = '_remote_table_metadata'


class RemoteTableException(Exception):
    pass


def create_metadata_table(conn: apsw.Connection, table_name, table_sql, metadata: dict):
    create_tables = f"""CREATE TABLE IF NOT EXISTS {REMOTE_TABLE_INFO} (
                table_name text,
                sql text,
                PRIMARY KEY(table_name)
                );
                CREATE TABLE IF NOT EXISTS {REMOTE_TABLE_METADATA} (
                table_name text,
                name text,
                type text,
                value text,
                PRIMARY KEY(table_name, name)
                )
        """
    cursor = conn.cursor()
    for sql in create_tables.split(';'):
        cursor.execute(sql)
    
    sql = f"""insert into {REMOTE_TABLE_INFO} (table_name, sql) values (?,?)
             on conflict(table_name) do nothing"""
    cursor.execute(sql, [table_name, table_sql])
    sql = f"""insert into {REMOTE_TABLE_METADATA} (table_name, name, type, value) values (?,?,?,?)
             on conflict(table_name, name) do nothing"""
    for name_, value in metadata.items():
        type_ = type(value).__name__
        cursor.execute(sql, [table_name, name_, type_, value])


def table_exists(conn, name):
    sql = """SELECT name FROM sqlite_master WHERE name = ?"""
    cursor = conn.cursor()
    cursor.execute(sql, [name])
    result = cursor.fetchone()
    return result != None and len(result) != 0


class Remotable(apsw.Connection):
    # NOTE: __new__  and __init__ are never called!



    def Create(self, db, modulename, dbname, *args):
        """Create the virtual database table.

        Args given in the CREATE VIRTUAL TABLE SQL statement should be:
            1. python DBApi module name, 
            2. sql,
            3. connect arguments,
            4. optional indices,
                a. indices=idx1:cost1;idx2:cost2...;idxN:costN
                b. idx is the index name in the table
                c. cost is an estimated cost as an integer. Lower
                   numbers should represent more unique columns (?)
            5. optional "sql" type: query or table
                a. sqltype=<query|table>
                b. 'query' means that the 2nd argument (sql) is a query
                c. 'table' means that the 2nd argument (sql) is a table name (default)

        Example:
        CREATE VIRTUAL TABLE artists using remotable(sqlite3, 
            select * from artists, 
            'C:/path/to/database.sqlite',
            indices=artist:100;score:20,
            querytype=table);
        """

        # TODO: In args, allow a mechanism for specifying a query for how one would
        # index information from the remote DBMS.  E.g. `show index abc on table`.
        # if it is None or not provided, there would be no such query.
        # Possibilities:
        #    1. A query to get all available indices on the table
        #    2. A way of manually specifying indices and their potential costs
        try:
            tablename = dbname
            module_name, sql = args[0], args[1]
            db_module = importlib.import_module(module_name)

            pargs = []
            kwargs = {}
            indices = {}
            querytype = 'table' # Default
            arg: str
            for arg in args[2:]:
                if '<getpass>' in arg:
                    password = getpass.getpass('Please enter your database password: ')
                    arg = arg.replace('<getpass>', password)
                if re.match(r'\s*[^\'"]+\s*=\s*.*', arg) != None:
                    args = re.split(r'\s*=\s*', arg, maxsplit=1)
                    arg_name, arg_val = (a.lower() for a in args)
                    if arg_name in ['indices', 'indexes']:
                        # Specify remote indices manually in terms of name and
                        # relative cost. A more unique index should potentially have
                        # a lower cost.  Format: indices=field 1,cost;field 2,cost ... field n,cost
                        # There may be spaces in the field names if necessary.
                        indices = re.split(r'\s*\;\s*',arg_val)
                        try:
                            indices = dict(re.split(r'\s*:\s*', idx, maxsplit=1) for idx in indices)
                            indices = {idx.lower(): int(cost) for idx, cost in indices.items()}
                        except ValueError:
                            print("Couldn't parse indexes/indices value. Format: indices=field 1,cost;field 2,cost ... field n,cost")
                            print("Ignoring indices.")
                            indices = {}
                    elif arg_name == 'querytype':
                        querytype = arg_val
                    else:
                        arg_val = eval(arg_val)
                        kwargs.update({arg_name: arg_val})
                else:
                    pargs.append(eval(arg))
            querytype = {
                'table': SQL_MODE_TABLE,
                'query': SQL_MODE_QUERY,
            }[querytype]
            connection = db_module.connect(*pargs, **kwargs)
            fields = []
            cur = connection.cursor()
            # Just get the column names from the table
            # This may not work with complex SQL statements
            # as the destination "table"
            if querytype == SQL_MODE_TABLE:
                cur.execute(f'select * from {sql} where 0=1')
            elif querytype == SQL_MODE_QUERY:
                cur.execute(f'select * from ({sql}) t where 0=1')
            for field in cur.description:
                name, _type, _, _, precision, scale, _ = field
                typemap = {
                    'NUMBER': 'integer',
                    'DECIMAL': 'real',
                    'DATETIME': 'real',
                    'str': 'text',
                    'float': 'real',
                    'datetime': 'text',
                    'bool': 'integer',
                }
                classname = getattr(type, '__name__', 'str')
                typename = typemap.get(classname, 'text')
                if typename == 'integer':
                    if precision or scale:
                        typename = 'real'
                fields.append({'name': name, 'typename': typename})
            fielddefs = ', '.join([f"\"{d['name']}\" {d['typename']}" for d in fields])
            schema = f'create table "{tablename}" ({fielddefs});'
            create_metadata_table(self, tablename, sql, {**indices, **{'querytype': querytype}})
            return schema, Table(self, connection, tablename, sql, tuple(fields), indices, querytype)
        except Exception:
            import traceback
            traceback.print_exc()

    Connect = Create

class Table:
    def __init__(self, apsw_connection, connection, tablename, sql, fields, indices, querytype):
        self.connection = connection
        self.cursor = None
        self.sql = sql
        self.fields = fields
        self.indices = indices
        self.querytype = querytype
        self.tablename = tablename
        self.apsw_connection = apsw_connection
        cursor = self.apsw_connection.cursor()
        sql = f'select name, type, value from {REMOTE_TABLE_METADATA} where table_name = ?'
        cursor.execute(sql, [self.tablename])
        indices_list = cursor.fetchall()
        for _name, _type, value in indices_list:
            # Get the type function
            _type = eval(_type)
            if _name == 'querytype':
                self.querytype = _type(value)
            else:
                self.indices[_name] = _type(value)

    def BestIndex(self, constraints, orderbys):
        constraint_map = {
            apsw.SQLITE_INDEX_CONSTRAINT_EQ: '=',
            apsw.SQLITE_INDEX_CONSTRAINT_FUNCTION: None,
            apsw.SQLITE_INDEX_CONSTRAINT_GE: '>=',
            apsw.SQLITE_INDEX_CONSTRAINT_GLOB: None,
            apsw.SQLITE_INDEX_CONSTRAINT_GT: '>',
            apsw.SQLITE_INDEX_CONSTRAINT_IS: 'is',
            apsw.SQLITE_INDEX_CONSTRAINT_ISNOT: 'is not',
            apsw.SQLITE_INDEX_CONSTRAINT_ISNOTNULL: 'is not null',
            apsw.SQLITE_INDEX_CONSTRAINT_ISNULL: 'is null',
            apsw.SQLITE_INDEX_CONSTRAINT_LE: '<=',
            apsw.SQLITE_INDEX_CONSTRAINT_LIKE: 'like',
            apsw.SQLITE_INDEX_CONSTRAINT_LIMIT: None,
            apsw.SQLITE_INDEX_CONSTRAINT_LT: '<',
            apsw.SQLITE_INDEX_CONSTRAINT_MATCH: None,
            apsw.SQLITE_INDEX_CONSTRAINT_NE: '<>',
            apsw.SQLITE_INDEX_CONSTRAINT_OFFSET: None,
            apsw.SQLITE_INDEX_CONSTRAINT_REGEXP: '',
            apsw.SQLITE_INDEX_SCAN_UNIQUE: None,
        }

        fields = self.fields
        indices = self.indices
        # constraints = [(field#, operation#)...]
        constraints_used = [(idx, fields[idx]['name'], constraint_map[op]) 
                             for idx, op in constraints
                             if fields[idx]['name'] in indices.keys()]
        estimated_cost = sum([indices[idx_name] for _, idx_name, _ in constraints_used])
        if not estimated_cost:
            estimated_cost = 9E+99
        # Return OR of sum of indices, 2 raised to idx power used as index number.
        # We can determine which fields in fields[] they are if necessary.
        # TODO: is this problematic?
        index_number = sum(2**idx for idx,_,_ in constraints_used)
        index_string = str([(idx_name, idx_op) for _, idx_name, idx_op in constraints_used])
        orderby_consumed = False
        constraints_used = [n if idx in [u for u,_,_ in constraints_used] else None 
                for n, (idx, _) in enumerate(constraints)]
        if index_string == '':
            constraints_used = []
        return [
            constraints_used,
            index_number,
            index_string,
            orderby_consumed,
            estimated_cost
        ]
        """Return

            You should return up to 5 items. Items not present in the return have a default value.

            0: constraints used (default None)
            This must either be None or a sequence the same length as constraints passed in. Each item should be as specified above saying if that constraint is used, and if so which constraintarg to make the value be in your VTCursor.Filter() function.

            1: index number (default zero)
            This value is passed as is to VTCursor.Filter()

            2: index string (default None)
            This value is passed as is to VTCursor.Filter()

            3: orderby consumed (default False)
            Return True if your output will be in exactly the same order as the orderbys passed in

            4: estimated cost (default a huge number)
            Approximately how many disk operations are needed to provide the results. SQLite uses the cost to optimise queries. For example if the query includes A or B and A has 2,000 operations and B has 100 then it is best to evaluate B before A.
        """
        
    def Open(self):
        self.cursor = Cursor(self)
        return Cursor(self)

    def Disconnect(self):
        print('Remote connection closing.')
        self.connection.close()

    Destroy = Disconnect

class Cursor:
    def __init__(self, table):
        self.table = table
        self.cursor = table.connection.cursor()
        self.current_line = None
        self.row_id = 0

    def Filter(self, indexnum, indexname, constraintargs):
        constraints = eval(indexname)
        args = [arg for (_, op,),arg in zip(constraints, constraintargs) if op != None]
        clauses = [(left,op) for left,op in constraints if op != None]
        clauses = [f'{left} {op} ?' for left,op in clauses]
        clauses = ' or '.join(clauses)
        where = f'where {clauses}' if clauses else ''
        sql = ''
        if self.table.querytype == SQL_MODE_TABLE:
            sql = f'select t.* from {self.table.sql} t {where}'
        elif self.table.querytype == SQL_MODE_TABLE:
            sql = f'select t.* from ({self.table.sql}) t {where}'
        self.cursor.execute(sql, args)
        self.row_id = 0
        self.current_line = self.cursor.fetchone()

    def Eof(self):
        is_eof = False if self.current_line else True
        return is_eof

    def Rowid(self):
        return self.row_id

    def Column(self, col):
        data = None
        try:
            if self.current_line:
                data = self.current_line[col]
            else:
                return None
        except Exception:
            import traceback
            traceback.print_exc()

        if type(data) is datetime.datetime:
            return data.strftime('%Y-%m-%d %H:%M:%S')
        if type(data) is datetime.date:
            return data.strftime('%Y-%m-%d')
        if type(data) is datetime.time:
            return data.strftime('%H:%M:%S')
        if type(data) is decimal.Decimal:
            return float(data)
        return self.current_line[col]


    def Next(self):
        self.current_line = self.cursor.fetchone()
        self.row_id += 1

    def Close(self):
        pass
    
    def __del__(self):
        pass
