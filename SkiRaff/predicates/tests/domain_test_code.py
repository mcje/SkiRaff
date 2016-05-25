import sqlite3
import sys

sys.path.append('../')
from  domain_predicate import *
sys.path.append('../../')
from pygrametl_reinterpreter import *


def constraint1(a):
    if a > 26:
        return True
    else:
        return False


def constraint2(a=''):
    if a == 'Cockbook':
        return True
    else:
        return False


dw_name = '.\dw.db'  # The one found in pygrametl_examples
dw_conn = sqlite3.connect(dw_name)

f = FTRepresentation('factTable', ['bookid','locationid','timeid'],['sale'], dw_conn)
b = DimRepresentation('bookDim', 'bookid', ['genre'], ['book'], dw_conn)
l = DimRepresentation('locationDim', 'locationid', ['region'], ['city'], dw_conn)
t = DimRepresentation('timeDim', 'timeid', ['day', 'month', 'year'], [], dw_conn)
Big = DWRepresentation([b,l,t],[f],dw_conn)

constraint_tester1 = DomainPredicate(Big, 'factTable', 'sale', constraint1)
constraint_tester2 = DomainPredicate(Big, 'bookDim', 'genre', constraint2)

constraint_tester1.run()
constraint_tester2.run()