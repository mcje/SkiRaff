import ast
from .transform_visitor import TransformVisitor


__author__ = 'Mathias Claus Jensen'
__all__ = ['Reinterpreter']


class Reinterpreter(object):
    """ Class in charge of reinterpreting a pygrametl program, using different
    connections.
    """

    def __init__(self, program, source_conns, dw_conn):
        """ 
        :param program: A string containing the program that is to be 
        reinterpreted.
        :param source_conns: A list of replacement connections to place
        into the program. A replacement must be ordered according to when
        source appears in the program.
        :param dw_conn: PEP249 connection object, connecting to a DW
        """

        self.program = program
        self.conn_scope = source_conns
        self.dw_conn = dw_conn
        self.source_ids = []

        # Generates id names for sources and DW,
        # zipping names an replacement objects into a dictionary,
        # which is later used as a scope.
        self.dw_id = '__0__'
        self.scope = {self.dw_id: self.dw_conn}
        counter = 0

        for entry in source_conns:
            source_id = "__" + str(source_conns.index(entry) + 1) + "__"
            self.source_ids.append(source_id)
            source = self.conn_scope.__getitem__(counter)
            self.scope[source_id] = source
            counter += 1

    def __transform(self, node):
        """
        :param node: an ast node that is the root of the node tree
        Swaps out the connections in the old program, with the ones given.
        """
        tv = TransformVisitor(self.source_ids, self.dw_id)
        tv.start(node)

        if not tv.dw_flag:
            raise RuntimeError('No ConnectionWrapper instantiated in' +
                               ' pygrametl program')
        elif tv.counter < len(self.source_ids):
            raise RuntimeError('Too many sources have been given')

    def run(self):
        """
        Reinterpretes the pygrametl program, returns a DWRepresentation
        :return: DWRepresentation of dw from the given program
        """

        # Parsing the pygrametl program to an AST
        tree = ast.parse(self.program)

        # Transforming the AST to include the user defined connections
        self.__transform(tree)

        # Executing the transformed AST
        p = compile(source=tree, filename='<string>', mode='exec')

        exec(p, self.scope)

        return self.scope


