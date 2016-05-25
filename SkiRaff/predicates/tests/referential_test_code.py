import os
import sqlite3
import pygrametl
from pygrametl.tables import Dimension, SnowflakedDimension
from framework.datawarehouse_representation \
    import DWRepresentation, DimRepresentation
from framework.predicates.referential_integrity_predicate \
    import ReferentialIntegrityPredicate

__author__ = 'Arash Michael Sami Kjær'
__maintainer__ = 'Arash Michael Sami Kjær'

#  Snowflaking test

open(os.path.expanduser('ref.db'), 'w')

conn = sqlite3.connect('ref.db')
ref_cur = conn.cursor()

ref_cur.execute("CREATE TABLE dim1 " +
                "(key1 INTEGER PRIMARY KEY, attr1 INTEGER, key2 INTEGER, "
                "key3 INTEGER)")

ref_cur.execute("CREATE TABLE dim2 " +
                "(key2 INTEGER PRIMARY KEY, attr2 INTEGER, key4 INTEGER)")

ref_cur.execute("CREATE TABLE dim3 " +
                "(key3 INTEGER PRIMARY KEY, attr3 INTEGER)")

ref_cur.execute("CREATE TABLE dim4 " +
                "(key4 INTEGER PRIMARY KEY, attr4 INTEGER)")

data = [
    {'attr1': 24,
     'attr2': 24,
     'attr3': 24,
     'attr4': 24},

    {'attr1': 25,
     'attr2': 25,
     'attr3': 25,
     'attr4': 25},

    {'attr1': 26,
     'attr2': 26,
     'attr3': 26,
     'attr4': 26},

    {'attr1': 74,
     'attr2': 74,
     'attr3': 74,
     'attr4': 74},

    {'attr1': 75,
     'attr2': 75,
     'attr3': 75,
     'attr4': 75},

    {'attr1': 76,
     'attr2': 76,
     'attr3': 76,
     'attr4': 76}
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


snow_dw_rep.connection.cursor().execute("DELETE FROM dim2 WHERE key2 > 3")

# quick overview of the keys we have left
for dim in snow_dw_rep.dims:
    allatts = dim.all.copy()
    lookupatts = dim.lookupatts.copy()
    for attr in lookupatts:
        allatts.remove(attr)

    for row in dim.itercolumns(allatts):
        print(dim.name, row)

ref_tester = ReferentialIntegrityPredicate(
    refs={dim1.name: [dim2.name, dim3.name], dim2.name: [dim4.name]},
    points_to_all=True,
    all_pointed_to=True)

print(ref_tester.run(snow_dw_rep))

conn.close()
