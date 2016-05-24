import sys
sys.path.append('../')
import os.path
import unittest
from unittest import *
from reinterpreter import Reinterpreter
import sqlite3


class TestReinterpreter(TestCase):
    def setUp(self):
        BASE_DIR = os.path.dirname(os.path.abspath('__file__'))
        db_path_a = os.path.join(BASE_DIR, "a.db")
        db_path_b = os.path.join(BASE_DIR, "b.db")
        self.conn1 = sqlite3.Connection(db_path_a)
        self.conn2 = sqlite3.Connection(db_path_b)
        self.conn_scope = {'conn_a': self.conn1, 'conn_b': self.conn2}
        self.program = \
"""import pygrametl
from pygrametl.datasources import SQLSource
from pygrametl.tables import Dimension, FactTable
import sqlite3

input_conn = sqlite3.connect('input.db')
output_conn = sqlite3.connect('output.db')

input_src = SQLSource(input_conn, query='SELECT * dim1')
output_wrapper = pygrametl.ConnectionWrapper(connection=output_conn)

dim1 = Dimension(
    name='dim1',
    key='key1',
    attributes=['attr1', 'attr2']
)

dim2 = Dimension(
    name='dim2',
    key='key2',
    attributes=['attr3', 'attr4']
)

ft1 = FactTable(
    name='ft1',
    keyrefs=['key1',]
)

input_conn.close()
output_conn.close()
"""
        self.reinterpreter = Reinterpreter(program=self.program,
                                           conn_scope=self.conn_scope,
                                           program_is_path=False)
        
    def tearDown(self):
        self.conn1.close()
        self.conn2.close()

    #@unittest.skip('')
    def test_run(self):
        # Arrange
        reinterpreter = self.reinterpreter

        # Act
        scope = reinterpreter.run()

        # Assert
        self.assertIn('dim1', scope)
        self.assertIn('dim2', scope)
        self.assertIn('ft1', scope)
        self.assertEqual(len(scope), 3)

    def test_run_not_enough_conns_in_scope(self):
        # Arrange
        conn_scope = {'conn_a': self.conn1} # We give too few connections
        reinterpreter = Reinterpreter(program=self.program,
                                      conn_scope=conn_scope,
                                      program_is_path=False)

        # Act/Assert
        with self.assertRaises(StopIteration):
            scope = reinterpreter.run() # Should throw an StopIteration exception
            
        
        
suite = makeSuite(TestReinterpreter)
TextTestRunner(verbosity=2).run(suite)
