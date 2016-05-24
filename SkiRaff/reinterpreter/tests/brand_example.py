__author__ = 'Alexander'
import sys
sys.path.append('../')
from reinterpreter import Reinterpreter
from db_test_setup import *
import sqlite3
import os.path

BASE_DIR = os.path.dirname(os.path.abspath('__file__'))
DB = os.path.join(BASE_DIR, 'brandtest.db')
DW = os.path.join(BASE_DIR, 'dwtest.db')
CSV = os.path.join(BASE_DIR, 'weapontest.csv')


program_path = './brand_sample.py'

setup_input_db(DB)
setup_out_dw(DW)
# setup_input_csv(CSV)

db_conn = sqlite3.connect(DB)
dw_conn = sqlite3.connect(DW)


#csv_conn = sqlite3.connect(CSV)

conn_dict = {'conn0': db_conn, 'conn1': dw_conn}

try:
    tc = Reinterpreter(program=program_path, conn_scope=conn_dict, program_is_path=True)

    scope = tc.run()
finally:
    db_conn.close()
    dw_conn.close()
    pass
