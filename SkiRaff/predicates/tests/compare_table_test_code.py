import os
import sqlite3
import time

from pygrametl import ConnectionWrapper
from pygrametl.tables import Dimension, FactTable

from framework.datawarehouse_representation import \
    DWRepresentation, DimRepresentation,FTRepresentation
from framework.predicates import CompareTablePredicate

__author__ = 'Arash Michael Sami Kjær'
__maintainer__ = 'Arash Michael Sami Kjær'  # Are you sure this is mine?

# This just ensures we have a fresh database to work with.
open(os.path.expanduser('test.db'), 'w')

conn = sqlite3.connect('test.db')

c = conn.cursor()

# Making table to test on...
c.execute('''CREATE TABLE COMPANY
    (ID INTEGER PRIMARY KEY AUTOINCREMENT    NOT NULL,
    NAME           TEXT   NOT NULL,
    AGE            INT    NOT NULL,
    ADDRESS        CHAR(50),
    SALARY         REAL);''')

company_info = [('Anders', 43, 'Denmark', 21000.00),
                  ('Sauron', 1000000, 'Mordor', 42)
                ]

# ... and inserting the necessary data.
c.executemany("INSERT INTO COMPANY (NAME,AGE,ADDRESS,SALARY) VALUES (?,?,?,?)",
              company_info)

# Making table to test on...
c.execute('''CREATE TABLE BOMPANY
    (ID INTEGER PRIMARY KEY AUTOINCREMENT    NOT NULL,
    NAME           TEXT   NOT NULL,
    AGE            INT    NOT NULL,
    ADDRESS        CHAR(50),
    SALARY         REAL);''')

company_info = [
                ('Anders', 43, 'Denmark', 21000.00),
                  ('Sauron', 1000000, 'Mordor', 42),
                  ('Anders', 43, 'Denmark', 21000.00)
                ]

# ... and inserting the necessary data.
c.executemany("INSERT INTO BOMPANY (NAME,AGE,ADDRESS,SALARY) VALUES (?,?,?,?)",
              company_info)


# Making table to test on...
c.execute('''CREATE TABLE LASTNAME
    (
    NAME           TEXT   NOT NULL,
    LAST            TEXT    NOT NULL);''')

company_info = [('Anders', 'Andersen'),
                ('Sauron', 'Bob')]

# ... and inserting the necessary data.
c.executemany("INSERT INTO LASTNAME (NAME,LAST) VALUES (?,?)",
              company_info)


c = conn.cursor()

# Making table to test on...
c.execute('''CREATE TABLE FACTTABLE
    (Book           TEXT   NOT NULL,
    Issue            INT    )''')

company_info = [('The Hobbit', 1), ('The Bobbit', 5)]

# ... and inserting the necessary data.
c.executemany("INSERT INTO FACTTABLE (BOOK,ISSUE) VALUES (?,?)", company_info)

conn.commit()

ConnectionWrapper(conn)

dim = Dimension('COMPANY', 'ID', ['NAME', 'AGE', 'ADDRESS', 'SALARY'], ['NAME'])
dim_rep = DimRepresentation(dim, conn)

dim2 = Dimension('LASTNAME', 'NAME', ['LAST'])
dim_rep2 = DimRepresentation(dim2, conn)

ft = FactTable('FACTTABLE', ['Book'], ['Issue'])
ft_rep = FTRepresentation(ft, conn)

dw = DWRepresentation([dim_rep, ft_rep, dim_rep2], conn)

expected_list1 = [
    {'NAME': 'Anders', 'AGE': 43, 'SALARY': 21000.0, 'ADDRESS': 'Denmark',
     'ID': 1},
    {'NAME': 'Charles', 'AGE': 50, 'SALARY': 25000.0, 'ADDRESS': 'Texas',
     'ID': 2},
    {'NAME': 'Wolf', 'AGE': 28, 'SALARY': 19000.0, 'ADDRESS': 'Sweden',
     'ID': 3},
    {'NAME': 'Hannibal', 'AGE': 45, 'SALARY': 65000.0, 'ADDRESS': 'America',
     'ID': 4},
    {'NAME': 'Buggy', 'AGE': 67, 'SALARY': 2000.0, 'ADDRESS': 'America',
     'ID': 5}
]

expected_list2 = expected_list1.copy()
expected_list2.__delitem__(0)
start = time.monotonic()

c = conn.cursor()
c.execute("SELECT * FROM bompany")

compare1 = CompareTablePredicate(['company'], ['bompany'],['ID'],True,False,(), True, True)

p = compare1.run(dw)
for x in p:
    print(x)

end = time.monotonic()
elapsed = end - start
print('{}{}'.format(round(elapsed, 3), 's'))
conn.close()
