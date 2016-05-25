from .predicate import Predicate
from .report import Report

__author__ = 'Arash Michael Sami Kjær'
__maintainer__ = 'Mikael Vind Mikkelsen'


class RowCountPredicate(Predicate):
    """
    Predicate for asserting that a table has a certain number of rows
    """

    def __init__(self, table_name, number_of_rows):
        """
        :param table_name: name of the table we are testing.
        Can be given as a list of tables if we want a join.
        :param number_of_rows: number of rows asserted to be in table
        """
        if isinstance(table_name, str):
            self.table_name = [table_name]
        else:
            self.table_name = table_name
        self.number_of_rows = number_of_rows

    def run(self, dw_rep):
        """
        Runs SQL to get the number of rows in a table. Then compares.
        :param dw_rep: A DWRepresentation object allowing us to access DW
        :return: Report object to inform whether assertion held
        """

        # Generates and runs SQL  for finding number of rows
        pred_sql = \
            " SELECT COUNT(*) " + \
            " FROM " + "NATURAL JOIN ".join(self.table_name)

        cursor = dw_rep.connection.cursor()
        cursor.execute(pred_sql)
        (query_result,) = cursor.fetchall()
        cursor.close()

        # Comparing actual number of rows with those asserted
        if query_result[0] == self.number_of_rows:
            self.__result__ = True

        return Report(result=self.__result__,
                      tables=self.table_name,
                      predicate=self,
                      elements=None,
                      msg="""The predicate did not hold, tested for {} row(s),
                      actual number of row(s): {}""".format(
                          self.number_of_rows, query_result[0]
                      ))


