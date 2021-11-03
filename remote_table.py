import datetime
import decimal
import getpass
import keyring

import apsw


class RemoteTableException(Exception):
    pass

class RemoteTable:
    def Create(self, db, modulename, dbname, tablename, *args):
        if args[0] == 'mssql':
            import pymssql
            server, user, database, sql = args[1:5]
            cred = keyring.get_credential(f'sqliteremote_{database}', None)
            if not cred:
                password = getpass.getpass(f'Password for {user}: ')
                keyring.set_password(f'sqliteremote_{database}', user, password)
            else:
                user = cred.username
                password = cred.password
            connection = pymssql.connect(host=server, user=user, password=password, database=database)
            fields = []
            with connection.cursor() as cur:
                cur.execute(sql)
                for field in cur.description:
                    name, _type, _, _, precision, scale, _ = field
                    #print(_type)
                    typename = 'text'
                    if _type == pymssql.NUMBER:
                        typename = 'integer'
                        if precision or scale:
                            typename = 'real'
                    if _type == pymssql.DECIMAL:
                        typename = 'real'
                    if _type == pymssql.DATETIME:
                        typename = 'real'
                    fields.append({'name': name, 'typename': typename})
            
            fielddefs = ', '.join([f"\"{d['name']}\" {d['typename']}" for d in fields])
            schema = f'create table "{tablename}" ({fielddefs});'
            #print(schema)
            return schema, Table(connection, sql, tuple(fields))
        else:
            raise RemoteTableException("Couldn't connect to database")

    Connect = Create

class Table:
    def __init__(self, connection, sql, fields):
        self.connection = connection
        self.cursor = None
        self.sql = sql
        self.fields = fields
        #print(self.fields)

    def BestIndex(self, constraints, orderbys):
        #print(f'constraints: {constraints}, orderbys: {orderbys}')
        #return ((0,),
        #        0,
        #        'idx_ordernum')
        return None


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

    def Filter(self, indexnum, indexname, constraintargs):
        self.cursor.execute(self.table.sql)
        self.current_line = self.cursor.fetchone()

    def Eof(self):
        is_eof = False if self.current_line else True
        return is_eof

    def Rowid(self):
        return None

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
        if type(data) is decimal.Decimal:
            return float(data)
        return self.current_line[col]


    def Next(self):
        self.current_line = self.cursor.fetchone()

    def Close(self):
        pass
    
    def __del__(self):
        pass

_shell = globals().get('shell')
if _shell:
    _shell.db.createmodule("remotetable", RemoteTable)

# $ python -c "import apsw;apsw.main()"
# sqlite> .read remote_table.py
# sqlite> create virtual table test using remotetable(testm, remotedbtype, remotedbaddr, remotedbuser, remotedatabasename, sql_query);

if __name__ == '__main__':
    _shell = apsw.Shell()
    _shell.db.createmodule("remotetable", RemoteTable)
    _shell.cmdloop()
