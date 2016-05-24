from pygrametl.tables import Dimension, FactTable, \
    TypeOneSlowlyChangingDimension, CachedDimension, SlowlyChangingDimension, \
    SCDimension, BulkDimension, CachedBulkDimension, BatchFactTable, \
    BulkFactTable, SnowflakedDimension

from framework.datawarehouse_representation import DWRepresentation,\
    DimRepresentation, FTRepresentation, SCDType1DimRepresentation, \
    SCDType2DimRepresentation

__author__ = 'Alexander, Arash'
__Maintainer__ = 'Alexander, Arash'
__all__ = ['RepresentationMaker']
DIM_CLASSES = [Dimension, CachedDimension,
               TypeOneSlowlyChangingDimension, SlowlyChangingDimension,
               SCDimension, BulkDimension, CachedBulkDimension]
FT_CLASSES = [FactTable, BatchFactTable, BulkFactTable]


class RepresentationMaker(object):
    """
    Creates a DWRepresentation object from an associated program scope
    """
    def __init__(self, dw_conn, scope):
        """
        :param dw_conn: PEP249 connection to DW
        :param scope: A program scope containing a pygrametl program
        """
        self.dw_conn = dw_conn
        self.scope = scope

        # Contains representations of dimension and fact table
        self.dim_reps = []
        self.fts_reps = []

    def check_table_type(self, table, typelist):
        """"
        Checks whether a table is part of a specific subset of table types.
        :param table: an instance of a table from pygrametl.tables
        :param typelist: list of types to compare against
        :return: a boolean indicating if the table was of a type from typelist.
        """
        for table_type in typelist:
            if isinstance(table, table_type):
                return True
        return False

    def run(self):
        """
        Extracts table objects from the scope, then creates new representation
        objects which are placed into the DWRepresentation
        :return: A DWRepresentation object for the given scope
        """

        # Gets all table objects in the scope
        pygrametl = self.scope['pygrametl']
        tables = pygrametl._alltables

        # Creates representation objects
        for table in tables:

            # If the table is a dimension.
            if self.check_table_type(table, DIM_CLASSES):
                if isinstance(table, TypeOneSlowlyChangingDimension):
                    dim = SCDType1DimRepresentation(table, self.dw_conn)
                elif isinstance(table, SlowlyChangingDimension):
                    dim = SCDType2DimRepresentation(table, self.dw_conn)
                else:
                    dim = DimRepresentation(table, self.dw_conn)
                self.dim_reps.append(dim)

            # If the table is a fact table
            elif self.check_table_type(table, FT_CLASSES):
                    ft = FTRepresentation(table, self.dw_conn)
                    self.fts_reps.append(ft)

        # From the scope, gets all SnowflakedDimensions.
        # These are used to re-create the referencing structure of the DW,
        # when instantiating DWRepresentation.
        snowflakes = []
        for x, value in self.scope.items():
            if isinstance(value, SnowflakedDimension):
                snowflakes.append(value)

        dw_rep = DWRepresentation(self.dim_reps, self.dw_conn, self.fts_reps,
                                  snowflakes)

        # Clears the list of tables as its contents may otherwise be retained,
        # when a new Case is executed. This is because the list is mutable.
        pygrametl._alltables.clear()

        return dw_rep
