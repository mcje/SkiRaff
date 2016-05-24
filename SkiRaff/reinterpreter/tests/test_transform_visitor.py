import sys
sys.path.append('../')
import unittest
from unittest import *
import ast
from ast import dump
from transform_visitor import TransformVisitor

MODIFY_LIST = ['SQLSource', 'CSVSource', 'ConnectionWrapper']

class TestTransformVisitor(TestCase):
    def setUp(self):
        self.conn_scope = {'conn_a': object(), 'conn_b': object(), 'conn_c': object()}

    def tearDown(self):
        pass

    def test_start_NoModifications(self):
        # Arrange
        program = """a = 2\nb = a + 3"""
        actual = ast.parse(program)
        expect = ast.parse(program)
        transform_visitor = TransformVisitor(self.conn_scope)
        
        # Act
        transform_visitor.start(actual)
        
        # Assert
        self.assertEqual(dump(actual), dump(expect))

        
    def test_start_ModifyPositionalArg(self):
        # Arrange
        program = "{}(conn, args)"
        transform_visitor = TransformVisitor(self.conn_scope)
        for s in MODIFY_LIST:
            p = program.format(s)
            actual = ast.parse(p)
            old = ast.parse(p)

            # Act
            transform_visitor.start(actual)
            
            # Assert
            # We check that some modification has been made
            self.assertNotEqual(dump(actual), dump(old))

            # We check that the correct change was made 
            self.assertEqual(actual.body[0].value.args[0].id, 'conn_a')
            
            # We check that nothing else was changed, this is done by removing
            # the part that should be changed from both.
            actual.body[0].value.args[0] = None
            old.body[0].value.args[0] = None
            self.assertEqual(dump(actual), dump(old))
   
                
    def test_start_ModifyKeywordArg(self):
        # Arrange
        program = "{}(connection=conn, args=args)"
        transform_visitor = TransformVisitor(self.conn_scope)
        for s in MODIFY_LIST:
            p = program.format(s)
            actual = ast.parse(p)
            old = ast.parse(p)

            # Act
            transform_visitor.start(actual)
            
            # Assert
            # We check that some modification has been made
            self.assertNotEqual(dump(actual), dump(old))

            # We check that the correct change was made
            self.assertEqual(actual.body[0].value.keywords[0].value.id, 'conn_a')

            # We check that nothing else was changed, this is done by removing
            # the part that should be changed from both.
            actual.body[0].value.keywords[0].value = None
            old.body[0].value.keywords[0].value = None
            self.assertEqual(dump(actual), dump(old))

            
    def test_start_ModifyMultiple(self):
        # Arrange
        program = "SQLSource(conn1, args)\nConnectionWrapper(conn2, args)\nCSVSource(conn3)"
        transform_visitor = TransformVisitor(self.conn_scope)
        actual = ast.parse(program)

        # Act
        transform_visitor.start(actual)

        # Assert
        self.assertEqual(actual.body[0].value.args[0].id, 'conn_a')
        self.assertEqual(actual.body[1].value.args[0].id, 'conn_b')
        self.assertEqual(actual.body[2].value.args[0].id, 'conn_c')

        
    def test_start_TooFewConnectionsInDict(self):
        # Arrange
        program = "SQLSource(conn1, args)\nConnectionWrapper(conn2, args)" +\
                  "\nCSVSource(conn3)\nSQLSource(conn4, args)"
        transform_visitor = TransformVisitor(self.conn_scope)
        actual = ast.parse(program)

        # Act/Assert
        with self.assertRaises(StopIteration):
            transform_visitor.start(actual)      

            
suite = makeSuite(TestTransformVisitor)
TextTestRunner(verbosity=2).run(suite)
