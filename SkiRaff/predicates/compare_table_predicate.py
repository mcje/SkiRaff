__author__ = 'Alexander Brandborg'
__maintainer__ = 'Alexander Brandborg'
from itertools import filterfalse
from operator import itemgetter

from framework.datawarehouse_representation import DimRepresentation, \
    FTRepresentation
from .predicate import Predicate
from .report import Report


def difference(a, b):
    """
    Equivalent to A-B or A\B in set theory. Difference/Relative Complement
    :param a: First list of dicts
    :param b: Second list of dicts
    :return: List of elements in a but not in b
    """
    return list(filterfalse(lambda x: x in b, a))


def get_next_row(table, names):
    """
    Used to get next row of either list of dicts or cursor
    :param table: Table to get next row from
    :return: Next row, and a bool saying whether list is empty
    """

    if isinstance(table, list):
        if not table:
            return None, True
        else:
            return table.pop(0), False
    else:
        row = table.fetchone()
        if row is None:
            return None, True
        else:
            return dict(zip(names, row)), False


def grouped_sql(table, columns):
    """
    Used to group together all rows in a table
    :param table: Table to group
    :param columns: Columns to group on
    :return: sql code for creating a grouped table
    """

    sql = \
        " ( " + \
        "SELECT " + ",".join(columns) + ", COUNT(*) " + \
        "AS COUNT " + \
        " FROM " + ",".join(table) + \
        " GROUP BY " + ",".join(columns) + \
        " ) "
    return sql


def unsorted_not_distinct(table1, table2, subset=False):
    """
    Used to find all rows in one table but not in another,
    not treating rows as distinct.
    :param table1: List of dicts for one table
    :param table2: List of dicts for another table
    :param subset: Indicates whether we are looking for a subset.
    :return:
    """

    only_in_table1 = []
    if subset:
        # When subset, a row in table1 is not subset,
        # if its contains more instances of a row than table2
        for row in table1:
            count1 = table1.count(row)
            count2 = table2.count(row)
            if count1 > count2 or None in row.values():
                dic = row.copy()
                dic['count'] = count1
                only_in_table1.append(dic)

    else:  # not Subset
        for row in table1:
            count1 = table1.count(row)
            count2 = table2.count(row)
            if count1 != count2 or None in row.values():
                dic = row.copy()
                dic['count'] = count1
                only_in_table1.append(dic)

    return only_in_table1


def tab_unsorted(table1, table2, where_conditions, dw_rep):
    """
    Used to find all rows in one table but not in another.
    :param table1: SQL code giving one table
    :param table2:SQL code giving another table
    :param where_conditions: Conditions to be met for two rows to be equal
    :param dw_rep: DWRepresentation object
    :return: List of tuples of rows in table1 but not table2
    """
    sql = \
        " SELECT  * " + \
        " FROM " + table1 + \
        " AS table1 " + \
        " WHERE NOT EXISTS" \
        " ( " + \
        " SELECT NULL " + \
        " FROM " + table2 + \
        " AS table2 " + \
        " WHERE " + " AND ".join(where_conditions) + \
        " ) "

    cursor = dw_rep.connection.cursor()
    cursor.execute(sql)
    return cursor.fetchall()


def sorted_compare(actual, expected):
    """
    Does a positional comparison of two sorted tables
    :param actual: Table in DW
    :param expected: Table given by user
    :return: two lists containing exclusive rows to each table
    """

    # Get names of attributes
    names = [t[0] for t in actual.description]
    result = True
    actual_empty = False
    expected_empty = False

    # Run through both lists as long as we find no errors and no list is empty.
    while result and not actual_empty and not expected_empty:
        a_row, actual_empty = get_next_row(actual, names)
        if not actual_empty:
            result = None not in a_row.values()

        if result:
            e_row, expected_empty = get_next_row(expected, names)
            if not expected_empty:
                result = None not in e_row.values()

        if not expected_empty and not actual_empty and result:
            result = a_row == e_row

    # Return true if no error found and both
    return result and actual_empty and expected_empty


def subset_sorted_compare(actual, expected):
    """
    Does a subset comparison of two sorted tables
    :param actual: Table in DW
    :param expected: Table given by user
    :return: A bool indicating whether expected is a subset of actual
    """

    # Get names of attributes
    names = [t[0] for t in actual.description]

    e_row, expected_empty = get_next_row(expected, names)
    if not expected_empty:
        result = None not in e_row.values()
    else:
        result = False
    actual_empty = False

    # Run through actual table until false or until expected table is empty
    while result and not expected_empty and not actual_empty:
        a_row, actual_empty = get_next_row(actual, names)

        if not actual_empty:
            if a_row == e_row:
                e_row, expected_empty = get_next_row(expected, names)
                if not expected_empty:
                    result = None not in e_row.values()

    # Test passed if expected table is empty and result not False
    return expected_empty and result


class CompareTablePredicate(Predicate):
    """ Predicate that compares two tables, actual and expected, to each other. 
    Asserting they are equivalent or that the expected is a subset of actual.
    The user can ignore specific attributes when comparing.
    """

    def __init__(self, actual_table, expected_table,
                 column_names=None, column_names_exclude=False, sort=True,
                 sort_keys=(), distinct=True, subset=False):
        """
        :param table_name: name of the table we are testing.
        Can be given as a list of tables if we want a join.
        :param column_names: set of column names
        :param expected_table: User defined table to compare with
        :param column_names_exclude: bool indicating if  all columns not in
        column_names should instead be used in the assertion.
        :param sort_keys: Set of attributes to sort on.
        :param distinct: If tables should be treated as having no duplicates
        :param subset: If we should check that expected_table is only a subset

        """

        # Make sure that actual table is a list of names
        if isinstance(actual_table, str):
            self.actual_table = [actual_table]
        elif isinstance(actual_table, list):
            self.actual_table = actual_table
        else:
            raise RuntimeError('Actual table not given as string'
                               ' or list of strings')

        # Make sure that expected table is a list of names
        if isinstance(expected_table, str):
            self.expected_table = [expected_table]
            self.expected_in_db = True

        # If list we check that it is either of names or dicts.
        # Also sets a flag to indicate, which one it is.
        elif isinstance(expected_table, list):
            if all(isinstance(n, dict) for n in expected_table):
                self.expected_in_db = False
            elif all(isinstance(n, str) for n in expected_table):
                self.expected_in_db = True
            else:
                raise RuntimeError('Neither list of names nor dicts given'
                                   'for expected table')
            self.expected_table = expected_table

        # If not any cases caught the input, it must be a cursor.
        # We fetch all data from it so we have a list of dicts.
        else:
            # If expected table is given as a cursor, we fetch from it
            try:
                tuples = expected_table.fetchall()
                names = [t[0] for t in expected_table.description]
                self.expected_table = []

                # Create dict, so that attributes have names
                for row in tuples:
                    self.expected_table.append(dict(zip(names, row)))
                self.expected_in_db = False
            except Exception:
                raise RuntimeError('Expected table not given as a name,' +
                                   'list of names, dicts nor cursor')

        self.column_names = column_names
        self.column_names_exclude = column_names_exclude
        self.sort = sort
        self.sort_keys = set(sort_keys)
        self.distinct = distinct
        self.subset = subset

    def run(self, dw_rep):
        """
        Compares the two tables and sets their surpluses for reporting.
        :param dw_rep: A DWRepresentation object allowing us to access DW
        :return: Report object to inform whether assertion held
        """

        only_in_actual = []
        only_in_expected = []
        sort_result = False

        # Gets the actual columns we want to compare on.
        chosen_columns = self.setup_columns(dw_rep, self.actual_table,
                                            self.column_names,
                                            self.column_names_exclude)

        if self.sort and not self.sort_keys:

            for table_name in self.actual_table:
                table = dw_rep.get_data_representation(table_name)

                # For dimensions
                if isinstance(table, DimRepresentation):

                    # We can either sort on key
                    if set([table.key]).issubset(set(chosen_columns)):
                        self.sort_keys = self.sort_keys.union(set([table.key]))

                    # ... or lookupatts
                    elif set(table.lookupatts).issubset(set(chosen_columns)):
                        self.sort_keys = self.sort_keys.union(
                            set(table.lookupatts))

                    else:  # In the case that sort key of table is not present
                        self.sort_keys.clear()
                        break

                # For fact tables
                elif isinstance(table, FTRepresentation):
                    # Can sort on keyrefs
                    if set(table.keyrefs).issubset(set(chosen_columns)):
                        self.sort_keys = self.sort_keys.union(
                            set(table.keyrefs))

                    else:  # In the case that sort key of table is not present
                        self.sort_keys.clear()
                        break

        if self.expected_in_db:  # When expected is in DW

            if self.sort_keys and self.sort:  # Sorted comparison
                if self.distinct:
                    select_sql = " SELECT DISTINCT "
                else:  # not distinct
                    select_sql = " SELECT "

                # Query for getting actual table sorted on keys
                actual_table_sql = \
                    select_sql + ",".join(chosen_columns) + \
                    " FROM " + " NATURAL JOIN ".join(self.actual_table) + \
                    " ORDER BY " + ",".join(self.sort_keys)

                actual_cursor = dw_rep.connection.cursor()
                actual_cursor.execute(actual_table_sql)

                # Query for getting expected table sorted on keys
                expected_table_sql = \
                    select_sql + ",".join(chosen_columns) + \
                    " FROM " + " NATURAL JOIN ".join(
                        self.expected_table) + \
                    " ORDER BY " + ",".join(self.sort_keys)

                expected_cursor = dw_rep.connection.cursor()
                expected_cursor.execute(expected_table_sql)

                if self.subset:
                    sort_result = \
                        subset_sorted_compare(actual_cursor, expected_cursor)

                else:
                    sort_result = \
                        sorted_compare(actual_cursor, expected_cursor)

            else:  # Unsorted comparison

                # Constructs conditions for the where clause
                where_conditions = []
                for name in chosen_columns:
                    equal_sql = "table1." + name + " = " + "table2." + name
                    where_conditions.append(equal_sql)

                if self.distinct:
                    actual_sql = ",".join(self.actual_table)
                    expected_sql = ",".join(self.expected_table)

                else:  # Not distinct
                    # We group together instances of the same row.
                    # For each group we calculate count, the number of
                    # Instances of a given row.

                    actual_sql = grouped_sql(self.actual_table,
                                             chosen_columns)

                    expected_sql = grouped_sql(self.expected_table,
                                               chosen_columns)

                    # Comparison of count changes based on, how to compare
                    if self.subset:
                        sql_count = " table1.COUNT <= table2.COUNT "
                    else:
                        sql_count = " table1.COUNT =  table2.COUNT"

                    # Added as an extra condition to the where clause
                    where_conditions.append(sql_count)

                if not self.subset:
                    # Get all entries only in expected
                    expected_query = \
                        tab_unsorted(expected_sql, actual_sql,
                                     where_conditions, dw_rep)
                    if expected_query:
                        only_in_expected = expected_query

                    # Get all entries only in actual
                    actual_sql_query = \
                        tab_unsorted(actual_sql, expected_sql,
                                     where_conditions, dw_rep)
                    if actual_sql_query:
                        only_in_actual = actual_sql_query

                if self.subset:
                    # Get all entries only in expected
                    expected_query = \
                        tab_unsorted(expected_sql, actual_sql,
                                     where_conditions, dw_rep)
                    if expected_query:
                        only_in_expected = expected_query

        else:  # Expected table as dicts
            # From expected, extract only columns used for comparison
            self.expected_table = \
                [{k: v for k, v in d.items() if k in chosen_columns}
                 for d in self.expected_table]

            if self.distinct:
                select_sql = " SELECT DISTINCT "

                # Remove duplicates from expected
                expected_dict = []
                [expected_dict.append(x) for x in self.expected_table
                 if not expected_dict.count(x)]

            else:  # not distinct
                select_sql = " SELECT "
                expected_dict = self.expected_table

            if self.sort:  # Sorted compare
                # Sort actual table in SQL and fetch
                actual_table_sql = \
                    select_sql + ",".join(chosen_columns) + \
                    " FROM " + " NATURAL JOIN ".join(self.actual_table) + \
                    " ORDER BY " + ",".join(self.sort_keys)

                actual_cursor = dw_rep.connection.cursor()
                actual_cursor.execute(actual_table_sql)

                # Sort expected table
                expected_dict = \
                    sorted(expected_dict,
                           key=itemgetter(*self.sort_keys))

                if self.subset:
                    sort_result = \
                        subset_sorted_compare(actual_cursor, expected_dict)

                else:
                    sort_result = \
                        sorted_compare(actual_cursor, expected_dict)

            else:  # Unsorted compare
                # Fetch contents of actual
                actual_table_sql = \
                    select_sql + ",".join(chosen_columns) + \
                    " FROM " + " NATURAL JOIN ".join(self.actual_table)

                cursor = dw_rep.connection.cursor()
                cursor.execute(actual_table_sql)
                query_result = cursor.fetchall()

                # Create dictionary from fetched tuples and attribute names
                actual_dict = []
                names = [t[0] for t in cursor.description]
                for row in query_result:
                    actual_dict.append(dict(zip(names, row)))

                if self.distinct:
                    if not self.subset:
                        # Fetch and remove nulls from actual
                        only_in_actual = \
                            [x for x in actual_dict
                             if None in x.values()]
                        actual_dict = \
                            [x for x in actual_dict
                             if None not in x.values()]

                    # Fetch and remove nulls from expected
                    only_in_expected = \
                        [x for x in expected_dict
                         if None in x.values()]
                    expected_dict = \
                        [x for x in expected_dict
                         if None not in x.values()]

                    # Find all rows in expected that are not in actual
                    only_in_expected.extend(difference(expected_dict,
                                                       actual_dict))
                    if not self.subset:
                        # Find all rows in actual that are not in expected
                        only_in_actual.extend(difference(actual_dict,
                                                         expected_dict))

                else:  # not distinct
                    # For each row in expected we see if the number of
                    # duplicates is the same in actual.

                    if not self.subset:
                        # Expected
                        expected = unsorted_not_distinct(
                            expected_dict, actual_dict)

                        # Making elements unique
                        unique_expected = []
                        [unique_expected.append(x) for x in expected
                         if not unique_expected.count(x)]

                        only_in_expected.extend(unique_expected)

                        # Actual
                        actual = unsorted_not_distinct(
                            actual_dict, expected_dict)

                        # Making elements unique
                        unique_actual = []
                        [unique_actual.append(x) for x in actual
                         if not unique_actual.count(x)]

                        only_in_actual.extend(unique_actual)

                    else:  # subset
                        # Expected
                        expected = unsorted_not_distinct(
                            expected_dict,
                            actual_dict,
                            True)
                        unique_expected = []
                        [unique_expected.append(x) for x in expected
                         if not unique_expected.count(x)]

                        only_in_expected.extend(unique_expected)

        report_list = []

        if self.sort:
            self.__result__ = sort_result

            if self.expected_in_db:
                table_names = \
                    " Expected: " + ",".join(self.expected_table) + \
                    " AND  Actual: " + ",".join(self.actual_table)
            else:
                table_names = \
                    " Expected: User table " + \
                    "  Actual: " + ",".join(self.actual_table)

            if self.subset:
                msg = "Comparison failed during subset sort compare"
            else:
                msg = "Comparison failed during sort compare"

            sort = Report(result=self.__result__,
                          tables=table_names,
                          predicate=self,
                          elements=[],
                          msg=msg)
            report_list.append(sort)

        else:
            # If no non-matching rows found, the assertion held.
            if not only_in_expected and not only_in_actual:
                self.__result__ = True

            if self.expected_in_db:
                table_names = ",".join(self.expected_table)
            else:
                table_names = "User table"

            expected = Report(result=self.__result__,
                              tables=table_names,
                              predicate=self,
                              elements=only_in_expected,
                              msg="Elements found only in expected")

            report_list.append(expected)

            if only_in_actual:
                actual = Report(result=self.__result__,
                                tables=",".join(self.actual_table),
                                predicate=self,
                                elements=only_in_actual,
                                msg="Elements found only in actual")

                report_list.append(actual)

        return report_list
