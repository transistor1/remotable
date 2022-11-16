import sys

import apsw

from remotable.__init__ import Remotable


if __name__ == '__main__':
    _shell = apsw.Shell(args=sys.argv[1:])
    _shell.db.createmodule("remotable", Remotable)
    _shell.cmdloop()
