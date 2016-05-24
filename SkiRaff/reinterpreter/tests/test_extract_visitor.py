import sys
sys.path.append('../')
import unittest
from unittest import *
import ast
from ast import dump
from extract_visitor import ExtractVisitor

class ConnectionWrapper(object):
    def __init__(self, obj):
        self.obj = obj

class TestExtractVisitor(TestCase):
    def test_start_NoExtractionsNoWrapper(self):
        # Arrange
        program = """a = 2\nb = a + 3"""
        tree = ast.parse(program)
        extract_visitor = ExtractVisitor('test0')

        # Act/Assert
        with self.assertRaises(ValueError):
            result = extract_visitor.start(tree)

    def test_start_NoExtractionsPositionalConn(self):
        # Arrange
        conn_name = 'conn0'
        program = "a = ConnectionWrapper(" + conn_name + ")"
        tree = ast.parse(program)
        assign_name = 'test0'
        extract_visitor = ExtractVisitor(assign_name)

        # Act
        result = extract_visitor.start(tree)

        # Assert
        self.assertEqual(extract_visitor.wrapper_conn.id, conn_name)
        self.assertEqual(len(result.body), 1)
        self.assertEqual(result.body[0].targets[0].id, assign_name)
        self.assertEqual(len(result.body[0].value.keys), 0)

    def test_start_OneExtraction(self):
        # Arrange
        conn_name = 'conn0'
        table_name = 'dim1'
        program = "a = ConnectionWrapper(" + conn_name + ")\n" +\
                  "dim = Dimension(name='" + table_name + "')"
        tree = ast.parse(program)
        assign_name = 'test0'
        extract_visitor = ExtractVisitor(assign_name)

         # Act
        result = extract_visitor.start(tree)

        # Assert
        self.assertEqual(extract_visitor.wrapper_conn.id, conn_name)
        self.assertEqual(len(result.body), 1)
        self.assertEqual(result.body[0].targets[0].id, assign_name)
        self.assertEqual(len(result.body[0].value.keys), 1)
        self.assertEqual(result.body[0].value.keys[0].s, table_name)
        self.assertEqual(result.body[0].value.values[0].func.id, 'SQLSource')
        self.assertEqual(result.body[0].value.values[0].args[0].id, conn_name)
        self.assertEqual(result.body[0].value.values[0].args[1].s, 'SELECT * FROM ' + table_name)


    def test_start_MultipleExtractions(self):
        # Arrange
        conn_name = 'conn0'
        table_name1 = 'dim1'
        table_name2 = 'ft1'
        program = "a = ConnectionWrapper(" + conn_name + ")\n" +\
                  "dim = Dimension(name='" + table_name1 + "')\n" +\
                  "ft = FactTable(name='" + table_name2 + "')"
        tree = ast.parse(program)
        assign_name = 'test0'
        extract_visitor = ExtractVisitor(assign_name)

         # Act
        result = extract_visitor.start(tree)

        # Arrange
        self.assertEqual(len(result.body[0].value.keys), 2)
        self.assertEqual(result.body[0].value.keys[1].s, table_name1)
        self.assertEqual(result.body[0].value.values[1].func.id, 'SQLSource')
        self.assertEqual(result.body[0].value.values[1].args[0].id, conn_name)
        self.assertEqual(result.body[0].value.values[1].args[1].s, 'SELECT * FROM ' + table_name1)
        self.assertEqual(result.body[0].value.keys[0].s, table_name2)
        self.assertEqual(result.body[0].value.values[0].func.id, 'SQLSource')
        self.assertEqual(result.body[0].value.values[0].args[0].id, conn_name)
        self.assertEqual(result.body[0].value.values[0].args[1].s, 'SELECT * FROM ' + table_name2)
                         

        
        

suite = makeSuite(TestExtractVisitor)
TextTestRunner(verbosity=2).run(suite)
