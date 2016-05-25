__author__ = 'Mathias Claus Jensen'

MAX_ELEMENTS = 5


class Report(object):
    """ Container object, that holds information regarding predicate errors
    """
    
    def __init__(self, result, predicate, tables, elements=[], msg=None):
        """
        :param result: Boolean denoting whether the predicate was successful or
        not.
        :param predicate: The predicate object from which the report originates
        :param tables: The tables that the predicate ran on.
        :param elements: A list of elements in which errors were found. Each 
        element has to be printable
        :param msg: The message printed when reporting a false result. If None,
        a default message will be used.
        """
        self.result = result
        self.predicate = predicate
        self.tables = tables
        self.predname = predicate.__class__.__name__
        if elements or msg or result:
            self.elements = elements
            self.msg = msg
        else:
            raise ValueError('Either msg or elements has to be set, or both')

    def __str__(self):
        s = ' |' + self.predname + ': \n' \
            ' |\tTables: ' + str(self.tables) + '\n' \
            ' |\tResult: '
        
        if self.result:
            return s + 'SUCCESS\n'
        else:
            s += 'FAILED\n'
            
        if self.elements:
            if self.msg:
                s += ' |\t' + self.msg + '\n'
            else:
                s += ' |\tThe predicate did not hold on the following ' \
                     'elements:\n'
            i = 0
            for e in self.elements:
                if i < MAX_ELEMENTS:
                    s += ' |\t\t' + str(e) + '\n'
                    i += 1
                elif i == MAX_ELEMENTS:
                    s += ' |\t\t...\n'
                    return s
            return s
        else:
            return s + ' |\t' + self.msg + '\n'






