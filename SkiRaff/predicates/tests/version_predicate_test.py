import os
import sqlite3
from pygrametl import ConnectionWrapper
from pygrametl.tables import SlowlyChangingDimension
from framework.datawarehouse_representation import \
    DWRepresentation, SCDType2DimRepresentation
from framework.predicates import SCDVersionPredicate

__author__ = 'Arash Michael Sami Kjær'
__maintainer__ = 'Arash Michael Sami Kjær'

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
    VERSION         INT);''')

company_info = [('Anders', 43, 'Denmark', 1.0),
                ('Charles', 50, 'Texas', 1),
                ('Wolf', 28, 'Sweden', 1),
                ('Hannibal', 45, 'America', 1),
                ('Anders', 43, 'Denmark', 2.0)
                ]

# ... and inserting the necessary data.
c.executemany("INSERT INTO COMPANY (NAME,AGE,ADDRESS,VERSION) VALUES (?,?,?,?)",
              company_info)
conn.commit()

ConnectionWrapper(conn)

s = SlowlyChangingDimension('COMPANY', 'ID',
                             ['NAME', 'AGE', 'ADDRESS', 'VERSION'],
                              ['NAME', 'AGE'], 'VERSION')

dim = SCDType2DimRepresentation(s, conn)

dw = DWRepresentation([dim], conn)

a = SCDVersionPredicate('COMPANY', {'NAME': 'Anders', 'AGE': 43}, 2)
b = SCDVersionPredicate('COMPANY', {'NAME': 'Anders', 'AGE': 43}, 3)
print(a.run(dw))
print(b.run(dw))

conn.close()
