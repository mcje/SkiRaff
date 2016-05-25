import os
import sqlite3

import pygrametl
from pygrametl.tables import Dimension

from framework.datawarehouse_representation \
    import DWRepresentation, DimRepresentation
from framework.predicates.column_not_null_predicate import \
    ColumnNotNullPredicate

__author__ = 'Arash Michael Sami Kjær'
__maintainer__ = 'Arash Michael Sami Kjær'

open(os.path.expanduser('null.db'), 'w')

null_conn = sqlite3.connect('null.db')
null_cur = null_conn.cursor()
null_cur.execute("CREATE TABLE dim1(key1 INTEGER PRIMARY KEY, attr1 INTEGER)")

data = [
    {'attr1': 20},
    {'attr1': None},
    {'attr1': 15},
    {'attr1': None},
    {'attr1': None},
    {'attr1': 1},
    {'attr1': 25},
    {'attr1': None},
    {'attr1': 5},
    {'attr1': None},
    {'attr1': 20},
    {'attr1': None},
    {'attr1': 15},
    {'attr1': None},
    {'attr1': None},
    {'attr1': 1},
    {'attr1': 25},
    {'attr1': None},
    {'attr1': 5},
    {'attr1': None},
    {'attr1': None}
]

wrapper = pygrametl.ConnectionWrapper(connection=null_conn)

dim1 = Dimension(
    name='dim1',
    key='key1',
    attributes=['attr1'],
)

for row in data:
    dim1.insert(row)

dim_rep = DimRepresentation(dim1, null_conn)
notnull_tester = ColumnNotNullPredicate('dim1')
null_rep = DWRepresentation([dim_rep], null_conn)

print(notnull_tester.run(null_rep))

null_conn.close()
