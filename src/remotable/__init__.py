import datetime
import decimal
import importlib
import getpass
import re


class RemoteTableException(Exception):
    pass


class Remotable:

    def Create(self, db, modulename, dbname, *args):
        """Create the virtual database table.

        Args given in the SQL statement should be:
            python module name, sql, connect arguments
        """
        try:
            tablename = dbname
            module_name, sql = args[0], args[1]
            db_module = importlib.import_module(module_name)

            pargs = []
            kwargs = {}
            arg: str
            for arg in args[2:]:
                if re.match(r'\s*[^\'"]+\s*=\s*.*', arg) != None:
                    arg_name, arg_val = arg.split('=', maxsplit=1)
                    if arg_val.lower() == "'<getpass>'":
                        arg_val = getpass.getpass('Please enter your database password: ')
                    else:
                        arg_val = eval(arg_val)
                    kwargs.update({arg_name: arg_val})
                else:
                    pargs.append(eval(arg))
            connection = db_module.connect(*pargs, **kwargs)
            fields = []
            with connection.cursor() as cur:
                cur.execute(sql)
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
                    classname = _type.__name__
                    typename = typemap.get(classname, 'text')
                    if typename == 'integer':
                        if precision or scale:
                            typename = 'real'
                    fields.append({'name': name, 'typename': typename})
                
                fielddefs = ', '.join([f"\"{d['name']}\" {d['typename']}" for d in fields])
                schema = f'create table "{tablename}" ({fielddefs});'
                return schema, Table(connection, sql, tuple(fields))
        except Exception:
            import traceback
            traceback.print_exc()


    Connect = Create

class Table:
    def __init__(self, connection, sql, fields):
        self.connection = connection
        self.cursor = None
        self.sql = sql
        self.fields = fields
        
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
