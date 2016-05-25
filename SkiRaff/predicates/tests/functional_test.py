import os
import sqlite3

import pygrametl
from pygrametl.tables import Dimension, SnowflakedDimension

from framework.datawarehouse_representation import \
    DWRepresentation, DimRepresentation
from framework.predicates import FunctionalDependencyPredicate

__author__ = 'Arash Michael Sami Kjær'
__maintainer__ = 'Arash Michael Sami Kjær'

open(os.path.expanduser('func.db'), 'w')

conn = sqlite3.connect('func.db')

cur = conn.cursor()

cur.execute("CREATE TABLE dim1 " +
            "(key1 INTEGER PRIMARY KEY, attr1 INTEGER, key2 INTEGER, "
            "key3 INTEGER)")

cur.execute("CREATE TABLE dim2 " +
            "(key2 INTEGER PRIMARY KEY, attr2 INTEGER, key4 INTEGER)")

cur.execute("CREATE TABLE dim3 " +
            "(key3 INTEGER PRIMARY KEY, attr3 INTEGER)")

cur.execute("CREATE TABLE dim4 " +
            "(key4 INTEGER PRIMARY KEY, attr4 INTEGER)")


data = [
    {'attr1': 3,
     'attr2': 6,
     'attr3': 3,
     'attr4': 9},

    {'attr1': 2,
     'attr2': 8,
     'attr3': 6,
     'attr4': 4},

    {'attr1': 4,
     'attr2': 5,
     'attr3': 3,
     'attr4': 3},

    {'attr1': 1,
     'attr2': 3,
     'attr3': 4,
     'attr4': 4}
]

wrapper = pygrametl.ConnectionWrapper(connection=conn)

dim1 = Dimension(
    name='dim1',
    key='key1',
    attributes=['attr1', 'key2', 'key3'],
    lookupatts=['attr1']
)

dim2 = Dimension(
    name='dim2',
    key='key2',
    attributes=['attr2', 'key4'],
    lookupatts=['attr2']
)

dim3 = Dimension(
    name='dim3',
    key='key3',
    attributes=['attr3']
)

dim4 = Dimension(
    name='dim4',
    key='key4',
    attributes=['attr4']
)

special_snowflake = SnowflakedDimension(references=[(dim1, [dim2, dim3]),
                                                    (dim2, dim4)])

for row in data:
    special_snowflake.insert(row)

conn.commit()

dim1_rep = DimRepresentation(dim1, conn)

dim2_rep = DimRepresentation(dim2, conn)

dim3_rep = DimRepresentation(dim3, conn)

dim4_rep = DimRepresentation(dim4, conn)

snow_dw_rep = DWRepresentation([dim1_rep, dim2_rep, dim3_rep, dim4_rep],
                               conn, snowflakeddims=(special_snowflake, ))

for dim in snow_dw_rep.dims:
    allatts = dim.all.copy()

    for row in dim.itercolumns(allatts):
        print(dim.name, row)
print('\n')

a = 'key3'
b = 'key1'
c = 'key4'
d = 'attr2'
e = 'key2'
f = 'attr1'

# key3 -> key1 | dim1
func_dep1 = FunctionalDependencyPredicate([dim1_rep.name], a, b)
# key4 -> attr2 | dim2
func_dep2 = FunctionalDependencyPredicate([dim2_rep.name], c, d)
# key4 -> key2 | dim2, dim4
func_dep3 = FunctionalDependencyPredicate([dim2_rep.name, dim4_rep.name], c, e)
# key4 -> key2 | dim2
func_dep4 = FunctionalDependencyPredicate([dim2_rep.name], c, e)
# attr1, key1 -> key2, key3 | dim1
func_dep5 = FunctionalDependencyPredicate([dim1_rep.name], (f, b), (e, a))
# key1 -> key3 and key2, attr2 -> attr4 | dim1, dim2
# multiple dependencies is not possible at this time, however it is easier to
# write one dependency now rather than as before
# func_dep6 = FunctionalDependencyPredicate([dim1_rep.name, dim2_rep.name], [
#     (('key1'), ('key3')), (('key2', 'attr2'), ('attr4'))
# ])

print(func_dep1.run(snow_dw_rep))
print(func_dep2.run(snow_dw_rep))
print(func_dep3.run(snow_dw_rep))
print(func_dep4.run(snow_dw_rep))
print(func_dep5.run(snow_dw_rep))
# print(func_dep6.run(snow_dw_rep))
conn.close()
