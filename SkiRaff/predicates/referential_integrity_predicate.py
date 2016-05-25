from .predicate import Predicate
from .report import Report

__author__ = 'Arash Michael Sami Kjær'
__maintainer__ = 'Alexander Brandborg'


def ref_sql(table1, table2, key):
        """
        Create SQL for checking referential in one way.
        If result not empty, the integrity does not hold.
        :param table1: Main table
        :param table2: Foreign key table
        :param key: Key between the tables
        :return: Resulting sql string
        """

        sql = \
            " SELECT * " + \
            " FROM " + table1.name + \
            " WHERE NOT EXISTS" \
            "( " + \
            "SELECT NULL " + \
            " FROM " + table2.name + \
            " WHERE " + table1.name + "." + key + \
            " = " \
            + table2.name + "." + key + \
            " )"
        return sql


def referential_check(table1, table2, key, dw_rep):
        """
        Produces and runs query checking referential integrity one way
        :param table1: Main table
        :param table2: Foreign key table
        :param key: Key between the tables
        :param dw_rep: Representation of DW
        :return: Query result
        """

        # Creates query
        table_to_dim_sql = ref_sql(table1, table2, key)

        # Run query and return result
        cursor = dw_rep.connection.cursor()
        cursor.execute(table_to_dim_sql)

        query_result = cursor.fetchall()
        cursor.close()
        return query_result


class ReferentialIntegrityPredicate(Predicate):
    """
    Checks referential integrity between tables.
    Treats all tables as distinct.
    """

    def __init__(self, refs=None, points_to_all=True,
                 all_pointed_to=True):
        """
        :param refs: Dictionary with table pairs to perform checks between
        :param points_to_all: If true. We check that for each entry in
        the main table that there is a match at the foreign key table.
        :param all_pointed_to: If true. We check that for each entry in
        the foreign table that there is a match at the main table.
        :return:
        """
        if not refs:
            self.refs = {}
        else:
            self.refs = refs
        self.points_to_all = points_to_all
        self.all_pointed_to = all_pointed_to
        if not points_to_all and not all_pointed_to:
            raise RuntimeError("Both points_to_all"
                               " and all_pointed_to"
                               " can not both be set to false")

    def run(self, dw_rep):
        """
        Runs SQL to return any keys not upholding referential integrity
        :param dw_rep: A DWRepresentation object allowing us to access DW
        :return: Report object to inform whether assertion held
        """
        missing_keys = []

        # Maps table names to table_representations
        refs = {}
        for alpha, beta in self.refs.items():
                b = []
                if isinstance(alpha, str):
                        a = dw_rep.get_data_representation(alpha)
                else:
                        raise ValueError('Expected string in refs, got: ' +
                                         str(type(x)))
                if isinstance(beta, str):
                        b.append(dw_rep.get_data_representation(beta))
                else:
                        for x in beta:
                                if isinstance(x, str):
                                        b.append(dw_rep.
                                                 get_data_representation(x))
                                else:
                                        raise ValueError('Expected string' +
                                                         ' in refs, got: ' +
                                                         str(type(x)))
                refs[a] = tuple(b)
        self.refs = refs

        # If references not given. We check refs between all tables.
        if not self.refs:
            self.refs = dw_rep.refs

        # Performs check for each pair of main table and foreign key table.
        for table, dims in self.refs.items():
            for dim in dims:
                key = dim.key

                # Check that each entry in main table has match
                if self.points_to_all:
                    query_result = referential_check(table, dim, key, dw_rep)

                    if query_result:
                        for row in query_result:
                            msg = '{}: {} in {} not found in {}' \
                                  .format(key, row[0], table.name, dim.name)
                            missing_keys.append(msg)

                # Check that each entry in foreign key table has match
                if self.all_pointed_to:
                    query_result = referential_check(dim, table, key, dw_rep)

                    if query_result:
                        for row in query_result:
                            msg = '{}: {} in {} not found in {}'\
                                .format(key, row[0], dim.name, table.name)
                            missing_keys.append(msg)

        if not missing_keys:
            self.__result__ = True

        # Get names of main tables
        names = []
        for key in self.refs.keys():
            names.append(key.name)

        return Report(result=self.__result__,
                      tables=names,
                      predicate=self,
                      elements=missing_keys
                      )


