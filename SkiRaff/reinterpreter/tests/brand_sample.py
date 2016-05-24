__author__ = 'Alexander Brandborg'
__maintainer__ = 'Alexander Brandborg'

from pygrametl import *
from sqlite3 import *
from db_setup import *
from pygrametl.tables import *
from pygrametl.steps import *
from pygrametl.datasources import *

DB = 'brand.db'
DW = 'dw.db'
#CSV = 'weapons.csv'

setup_input_db(DB)
setup_out_dw(DW)
#setup_input_csv(CSV)

db = connect(DB)
dw = connect(DW)

dw_conn_wrapper = ConnectionWrapper(connection=dw)

#csv_file_handle = open(CSV, "r")
#weapons = CSVSource(f=csv_file_handle, delimiter=',')
brands = SQLSource(connection=db, query="SELECT * FROM brand")

# We create and object for each dimension in the DW and the FactTable
wep_dimension = Dimension(
    name='wepDim',
    key='weapon',
    attributes=['type'],
    lookupatts=['weapon'])

fact_table = FactTable(
    name='factTable',
    keyrefs=['weapon'],
    measures=['name','kills'])


#[wep_dimension.insert(row) for row in weapons]
#csv_file_handle.close()

#for row in brands:
    #row['weapon'] = wep_dimension.ensure(row)
    #fact_table.insert(row)

dw_conn_wrapper.commit()
#dw_conn_wrapper.close()

