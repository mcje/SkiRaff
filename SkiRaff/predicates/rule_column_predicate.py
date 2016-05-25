import inspect
from .predicate import Predicate
from .report import Report

__author__ = 'Mikael Vind Mikkelsen'
__maintainer__ = 'Alexander Brandborg'


class RuleColumnPredicate(Predicate):
    """
    Predicate for asserting using a user defined-function returning a bool.
    The function takes a set of columns and is run a single time.
    """

    def __init__(self, table_name, constraint_function,
                 column_names=None, constraint_args=[],
                 column_names_exclude=False):
        """
        :param table_name: name of table used for test
        :param column_names: list of column names
        :param constraint_function: user-defined function to run on each row.
        Must return a boolean.
        :param constraint_args: Additional arguments for the constrain function.
        :param column_names_exclude: bool, indicating how column_names is
        used to fetch columns from the table.
        """
        self.table_name = table_name
        self.constraint_function = constraint_function
        self.constraint_args = constraint_args
        self.column_names = column_names
        self.column_names_exclude = column_names_exclude

    def run(self, dw_rep):
        """
        Runs the constraint function on the specified columns.
        :param dw_rep: A DWRepresentation object allowing us to access DW
        :return: Report object to inform whether assertion held
        """

        # Gets the attribute names for columns needed for test
        column_arg_names = self.setup_columns(dw_rep, self.table_name,
                                              self.column_names,
                                              self.column_names_exclude)

        func_args = inspect.getargspec(self.constraint_function).args
        if len(func_args) != len(column_arg_names) + len(self.constraint_args):
            raise ValueError("""Number of columns and number of arguments
                                do not match""")

        # Makes a dictionary. Each key is a column_name pointing
        # at a list of the corresponding column
        columns = {name: [] for name in column_arg_names}
        for row in dw_rep.iter_join(self.table_name):
            for name in column_arg_names:
                columns[name].append(row[name])

        # Finds parameters. First columns, then additional params.
        arguments = []
        for name in column_arg_names:
            arguments.append(columns[name])

        if self.constraint_args:
            arguments.append(*self.constraint_args)

        # Running the constraint function
        self.__result__ = self.constraint_function(*arguments)

        return Report(result=self.__result__,
                      tables=self.table_name,
                      predicate=self,
                      elements=None,
                      msg='The predicate did not hold against the constraint')
