from .predicate import Predicate
from .report import Report

__author__ = 'Alexander Brandborg'
__maintainer__ = 'Mikael Vind Mikkelsen'


class FunctionalDependencyPredicate(Predicate):
    """
    Predicate that can check if a table or a join of table_names holds a
    certain functional dependency.
    """
    def __init__(self, table_name, alpha, beta):
        """
        :param table_name: table_names from the database,
        which we wish to join
        :param alpha: alpha which are depended upon by other
        alpha. Given as either a single attribute name, or a tuple of
        attribute names.
        :param beta: alpha which are functionally
        dependent on the former alpha. Given as either a single attribute
        name, or a tuple of attribute names.
        Example:
        alpha = ('a','b') and beta = 'c'
        corresponds to the functional dependency: a, b -> c
        """
        self.table_name = table_name
        if isinstance(alpha, str):
            self.alpha = [alpha]
        else:
            self.alpha = alpha

        if isinstance(beta, str):
            self.beta = [beta]
        else:
            self.beta = beta

    def run(self, dw_rep):
        """
        Checks whether a function dependency holds.
        :param dw_rep: A DWRepresentation object allowing us to access DW
        :return: Report object to inform whether assertion held
        """

        # SQL setup for the left side of the dependency in WHERE-clause
        alpha_sql_generator = (" t1.{} = t2.{} ".format(a, a)
                               for a in self.alpha)
        and_alpha = ' AND '.join(alpha_sql_generator)

        # SQL setup for the right side of the dependency in WHERE-clause
        beta_sql_generator = (" (t1.{} <> t2.{}) ".format(b, b)
                              for b in self.beta)
        or_beta = ' OR '.join(beta_sql_generator)

        # Creates part of select statement to get keys
        select_alpha = ["t1." + str(a) for a in self.alpha]
        select_beta = ["t2." + str(b) for b in self.beta]
        select_sql = select_alpha + select_beta

        # Final setup of the entire SQL command
        lookup_sql = "SELECT DISTINCT " + ','.join(select_sql) +\
                     " FROM " +\
                     " ( " + " NATURAL JOIN ".join(self.table_name) + " ) " +\
                     " AS t1 ," +\
                     " ( " + " NATURAL JOIN ".join(self.table_name) + " ) " +\
                     " AS t2 " +\
                     " WHERE " + and_alpha + " AND " + "( {} )".format(or_beta)

        func_dep = "{} --> {}".format(self.alpha, self.beta)

        cursor = dw_rep.connection.cursor()
        cursor.execute(lookup_sql)
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
                      elements=dict_result,
                      tables=self.table_name,
                      predicate=self,
                      msg='The predicate failed for the functional '
                          'dependency "{}" \n'
                          ' |  It did not hold on the following elements:'.
                      format(func_dep))
