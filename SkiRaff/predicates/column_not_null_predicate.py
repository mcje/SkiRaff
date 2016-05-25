from .predicate import Predicate
from .report import Report

__author__ = 'Mikael Vind Mikkelsen'
__maintainer__ = 'Alexander Brandborg'


class ColumnNotNullPredicate(Predicate):
    """
    Predicate for asserting that nulls do not exist in the columns of a table
    """

    def __init__(self, table_name, column_names=None,
                 column_names_exclude=False):
        """
        :param table_name: name of the table we are testing.
        Can be given as a list of tables if we want a join.
        :param column_names: set of column names
        :param column_names_exclude: bool indicating if  all columns not in
        column_names should instead be used in the assertion.
        """

        if isinstance(table_name, str):
            self.table_name = [table_name]
        else:
            self.table_name = table_name
        self.column_names = column_names
        self.column_names_exclude = column_names_exclude

    def run(self, dw_rep):
        """
        Runs SQL to return all rows containing null.
        :param dw_rep: A DWRepresentation object allowing us to access DW
        :return: Report object to inform whether assertion held
        """

        # Gets the columns to concern
        chosen_columns = self.setup_columns(dw_rep, self.table_name,
                                            self.column_names,
                                            self.column_names_exclude)

        # Generates and runs SQL for finding rows with null
        null_condition_sql = (x + " IS NULL" for x in chosen_columns)

        pred_sql = " SELECT * " + \
                   " FROM " + " NATURAL JOIN ".join(self.table_name) + \
                   " WHERE " + " OR ".join(null_condition_sql)

        cursor = dw_rep.connection.cursor()
        cursor.execute(pred_sql)
        query_result = cursor.fetchall()
        cursor.close()

        # Create dict, so that attributes have names
        names = [t[0] for t in cursor.description]
        dict_result = []
        for row in query_result:
            dict_result.append(dict(zip(names, row)))

        # If any rows were fetched. Assertion fails
        if not dict_result:
            self.__result__ = True

        return Report(result=self.__result__,
                      predicate=self,
                      tables=self.table_name,
                      elements=dict_result,
                      msg=None)
