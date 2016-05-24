import ast

__author__ = 'Mathias Claus Jensen'
__maintainer__ = 'Mathias Claus Jensen'
__all__ = ['TransformVisitor']

ATOMIC_SOURCES = ['SQLSource', 'CSVSource', 'TypedCSVSource']
WRAPPERS = ['ConnectionWrapper']


class TransformVisitor(ast.NodeVisitor):
    """
    Class responsible for making changes to an AST so that it can be run
    with other connections
    """
    def __init__(self, sources_ids, dw_id):
        """
        :param: source_ids: names of the replacement sources
        :param: dw_id: name of the replacement dw connection
        """
        self.source_ids = sources_ids
        self.dw_id = dw_id
        self.counter = 0
        self.dw_flag = False

    def __get_id(self):
        """
        Goes through a single iteration of the keys of the source_ids.
        """
        if self.counter == len(self.source_ids):
            raise StopIteration('There are no more mappings to use')
        else:
            id = self.source_ids[self.counter]
            self.counter += 1
            return id

    def __replace_connection(self, id, node):
        """
        Replace name of connection with user-defined id
        :param id: Name to replace with
        :param node: A call node
        """

        newnode = ast.Name(id=id, ctx=ast.Load())
        if len(node.args) != 0:   # Conn given as positional arg
            node.args[0] = newnode
        else:  # Conn given by keyword i.e. "connection = x"
            for keyword in node.keywords:
                if keyword.arg == 'connection'\
                        or keyword.arg == 'f'\
                        or keyword.arg == 'csvfile':
                    keyword.value = newnode

        # Call to fill in line number and indentation information for the new
        # node and its children.
        ast.fix_missing_locations(node)

    def __find_call_name(self, node):
        """
        Function that finds the name of a call node
        :param node: The call node, who's name we will find.
        :return: The name of the call node
        """
        name = None
        if hasattr(node.func, 'id'):      # SQLSource() type call
            name = node.func.id
        elif hasattr(node.func, 'attr'):  # pygrametl.SQLSource() type call.
            name = node.func.attr         # Occurs when we don't import with
                                          # "From".

        else:
            raise NotImplementedError('Cannot get the name of ' + str(node))
        return name

    def visit_Call(self, node):
        """
        The visit of a call node.
        Ignores all calls except for those we need to modify.
        :param node: A call node
        """
        name = self.__find_call_name(node)
        if name in ATOMIC_SOURCES:
            id = self.__get_id()
            self.__replace_connection(id, node)

        elif name in WRAPPERS:
            if self.dw_flag:
                raise Exception('There is more than one wrapper in this '
                                'program')
            else:
                id = self.dw_id
                self.__replace_connection(id, node)
                self.dw_flag = True

    def start(self, node):
        """
        We start the visitor.
        :param node: the root of an AST.
        """
        self.counter = 0
        self.visit(node)
