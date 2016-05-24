__author__ = 'Alexander'

__author__ = 'Alexander'

# IMPORTS
import sqlite3
import os


def setup_input_db(db_name='./brandtest.db'):
    """ This function sets up the DB that will be used as an input for the ETL
    """
    # If the some DB with our name already exists, delete it!
    if os.path.exists(db_name):
        os.remove(db_name)

    # We connect to our DB and make a cursor
    brand_conn = sqlite3.connect(db_name)
    brand_cur = brand_conn.cursor()

    # We make a new table
    brand_cur.execute("CREATE TABLE brand " +
                "(name TEXT, kills Integer, weapon TEXT)")

    # The stuff we wanna put in our table
    input_list = [('Morten', 5, 'club'),
                  ('Frederik', 5000, 'club')]

    # We do a series of INSERT operation with before mentioned list
    brand_cur.executemany("INSERT INTO brand(name, kills, weapon) " +
                    "VALUES(?, ?, ?)", input_list)

    # We save all this to our DB and close it
    brand_conn.commit()
    brand_conn.close()


def setup_out_dw(dw_name='./dwtest.db'):
    """ This function sets up the DW in which the ETL will out put to
    """
    # Delete the DB if it already exists
    if os.path.exists(dw_name):
        os.remove(dw_name)

    dw_conn = sqlite3.connect(dw_name)
    dw_cur = dw_conn.cursor()

    # We make a table for each dimension and the FactTable
    dw_cur.execute("CREATE TABLE wepDim " +
                   "(weapon TEXT PRIMARY KEY, Type TEXT)")

    dw_cur.execute("CREATE TABLE factTable " +
                   "(name TEXT, kills INTEGER, weapon TEXT)")

    dw_conn.commit()
    dw_conn.close()


def setup_input_csv(csv_name='./weapontest.csv'):
    """ This function sets up the input CSV required
    """
    with open(csv_name, 'w+') as file:
        file.write('weapon,type\n')
        file.write('club, blunt\n')