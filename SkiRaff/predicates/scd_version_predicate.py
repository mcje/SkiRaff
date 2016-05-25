__author__ = 'Alexander Brandborg'
__maintainer__ = 'Alexander Brandborg'

from framework.datawarehouse_representation import \
    SCDType2DimRepresentation
from .predicate import Predicate
from .report import Report


class SCDVersionPredicate(Predicate):
    """
     Predicate that can check whether a specific entry in a Type2SCD has an
     asserted maximum version.
    """

    def __init__(self, table_name, entry, version):
        """
        :param table_name: Name of Type2SCD table
        :param entry: Dict, pairing lookupsatts with values. Describes an
        entry in the table, which may have several versions.
        :param version: The asserted maximum version value of entry
        """
        self.table_name = table_name
        self.entry = entry
        self.version = version

    def run(self, dw_rep):
        """
        Checks that the actual highest version for entry is as asserted
        :param dw_rep: A DWRepresentation object allowing us to access DW
        :return: Report object to inform whether assertion held
        """

        dim = dw_rep.get_data_representation(self.table_name)

        if not isinstance(dim, SCDType2DimRepresentation):
            raise RuntimeError('Given table is not'
                               ' a SCDType2DimRepresentation')

        if not set(dim.lookupatts) == set(self.entry.keys()):
            raise RuntimeError('Correct lookupatts not given')

        # Creates conditions for the where-clause
        # These are meant to find all instances of row
        null_condition_sql = []
        for a, b in self.entry.items():
            if isinstance(b, str):
                new = "\'" + b + "\'"
                null_condition_sql.append(a + " = " + new)

            else:
                null_condition_sql.append(a + " = " + str(b))

        # Find all instances of row and selects the maximum version
        lookup_sql = " SELECT max(" + dim.versionatt + ")" + \
                     " FROM " + self.table_name + \
                     " WHERE " + " AND ".join(null_condition_sql)

        cursor = dw_rep.connection.cursor()
        cursor.execute(lookup_sql)
        (query_result,) = cursor.fetchall()
        cursor.close()

        if query_result[0] is None:
            raise RuntimeError('Table empty or entry not present in table')

        if query_result[0] == self.version:
            self.__result__ = True

        return Report(result=self.__result__,
                      tables=self.table_name,
                      predicate=self,
                      elements=(),
                      msg='Version number not as asserted. ' +
                          'Was asserted to be ' + str(self.version) +
                          ',but was instead ' + str(query_result[0]))
