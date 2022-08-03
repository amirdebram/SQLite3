#!/usr/bin/env python3

__author__ = 'Amir Debram'
__version__ = '1.0'
__email__ = 'amirdebram@gmail.com'

import sqlite3
import csv

class Database():
    """
    ### SQLite3 database tools

    Usage: db = db(database_name)
    """
    def __init__(self, db_name: str):
        super().__init__()

        self.db_name = db_name
        self.connection = sqlite3.connect(db_name)
        self.cursor = self.connection.cursor()

    def close(self):
        """
        #### Closes database connection
        """
        self.connection.close()
        return 'Database connection has been closed'

    def save(self):
        """
        #### Saves all changes made to the database
        """
        self.connection.commit()
        return 'Changes have been saved'
        
    def name(self):
        """
        #### Returns name of current database
        """
        return str(self.db_name)

    def tables(self):
        """
        #### Returns a list of table names
        """
        __tablenames = [i[0] for i in self.cursor.execute("""SELECT name FROM "main".sqlite_master;""")]
        return __tablenames

    def columns(self, table_name: str):
        """
        #### Returns a list of column names

        Usage:
        
        db.columns( tablename | db.tables[0] )
        """
        __columnnames = [i[1] for i in self.cursor.execute(f"""PRAGMA "main".table_info({table_name});""")]
        return __columnnames

    def create_table(self, table_name: str, *args):
        """
        #### Creates a table

        Usage:
        
        create_table( 'table_name', 'column_name')
        create_table( 'table_name', ('column_name', 'column_name') )

        Column Types: INTEGER | TEXT | BLOB | REAL | NUMERIC
        """
        if type(args[0]) is tuple or list:
            if len(args[0]) == 1:
                self.cursor.execute(f"""CREATE TABLE IF NOT EXISTS "{table_name}"("{args[0]}" TEXT);""")
            else:
                self.cursor.execute(f"""CREATE TABLE IF NOT EXISTS "{table_name}"({', '.join(['"' + x + '" TEXT' for x in args[0]])});""")
        else:
            self.cursor.execute(f"""CREATE TABLE IF NOT EXISTS "{table_name}"("{args[0]}" TEXT);""")
        self.save()
        return f"Table *{table_name}* has been created with columns: {args[0]}"

    def delete_table(self, table_name: str):
        """
        #### Deletes a table

        Usage:
        
        delete_table( table_name )
        """
        try:
            self.cursor.execute(f"""DROP TABLE "main"."{table_name}";""")
            self.save()
            return f"Table {table_name} has been deleted."
        except sqlite3.OperationalError:
            return f"Error: No such table exists"
    
    def add_column_to_table(self, table_name: str, *args):
        """
        #### Add one or more columns to a table

        Usage:
        
        add_column_to_table( 'table_name', 'column_name')
        add_column_to_table( 'table_name', ('column_name', 'column_name') )

        Column Types: INTEGER | TEXT | BLOB | REAL | NUMERIC
        """
        if type(args[0]) is tuple:
            if len(args[0]) == 1:
                self.cursor.execute(f"""ALTER TABLE "main"."{table_name}" ADD COLUMN "{args[0]}" TEXT;""")
            else:
                for x in args[0]:
                    self.cursor.execute(f"""ALTER TABLE "main"."{table_name}" ADD COLUMN "{x}" TEXT;""")
        else:
            self.cursor.execute(f"""ALTER TABLE "main"."{table_name}" ADD COLUMN "{args[0]}" TEXT;""")
        self.save()
        return f"Table *{table_name}* has been altered with columns: {args[0]}"
    
    def remove_duplicates(self, table_name: str, *args):
        """
        #### Removes duplicates from one or more columns in a table

        Usage:
        
        remove_duplicates( 'table_name', 'column_name')
        remove_duplicates( 'table_name', ('column_name', 'column_name') )
        """
        if len(args[0]) == 1:
            self.cursor.execute(f"""DELETE FROM "{table_name}" WHERE "_rowid_" > ( SELECT MIN("_rowid_") FROM "{table_name}" tab WHERE "{table_name}".{args[0]} = tab."{args[0]}");""")
        else:
            self.cursor.execute(f"""DELETE FROM "main"."{table_name}" WHERE "_rowid_" > ( SELECT MIN("_rowid_") FROM "{table_name}" tab WHERE { ' AND '.join([f'"{table_name}"."'+ x + f'" = tab."' + x + '"' for x in args[0] ]) });""")
        return self.save()

    def fetchall(self, table_name: str):
        # All = [i for i in self.cursor.execute(f"""SELECT "_rowid_", * FROM "main"."{table_name}" WHERE "{Columns[0]}" LIKE '%%';""")]
        return [i for i in self.cursor.execute(f"""SELECT "_rowid_", * FROM "main"."{table_name}";""")]

    def insert(self, table_name: str, column_name: str, value: str):
        self.cursor.execute(f"""INSERT INTO "main"."{table_name}" ("{column_name}") VALUES ("{value}");""")
    
    def import_csv(self, file_location: str, tablename: str = 'Table_0', has_header: str = 'No', newline: str = '\n', delimiter: str = ',', encoding: str = 'utf-8'):
        """
        #### Creates a table from csv file

        Usage:
        
        import_csv(file_location, has_header = Yes, newline = '\\n', delimiter = ',', encoding = 'utf-8')
        """
        from string import ascii_uppercase

        def number_of_columns():
            
            with open(file_location, newline=newline, encoding=encoding) as csvfile:
                readfile = csv.reader(csvfile, delimiter=delimiter, quoting=csv.QUOTE_NONE)
                number_of_columns = 0
                for line in readfile:
                    if len(line) > number_of_columns:
                        number_of_columns = len(line)
            return int(number_of_columns)
        
        if has_header in ['Yes', 'yes', 'Y', 'y']:
            
            with open(file_location, newline=newline, encoding=encoding) as csvfile:
                readfile = csv.reader(csvfile, delimiter=delimiter, quoting=csv.QUOTE_NONE)
                for n, row in enumerate(readfile):
                    if n == 0:
                        self.create_table(tablename, row)
                    else:
                        self.cursor.execute(f"""INSERT INTO "main"."{tablename}" ({ ', '.join([ '"' + x + '"' for x in self.columns(tablename) ]) }) VALUES ({ ', '.join([ '"' + x + '"' for x in row ]) });""")

        elif has_header in ['No', 'no', 'N', 'n']:

            with open(file_location, newline=newline, encoding=encoding) as csvfile:
                readfile = csv.reader(csvfile, delimiter=delimiter, quoting=csv.QUOTE_NONE)

                self.create_table(tablename, tuple(x[1] for x in zip(range(number_of_columns()), ascii_uppercase)))
                
                for row in readfile:
                    self.cursor.execute(f"""INSERT INTO "main"."{tablename}" ({ ', '.join([ '"' + x + '"' for x in self.columns(tablename) ]) }) VALUES ({ ', '.join([ '"' + x + '"' for x in row ]) });""")

        else:
            print('Invalid Response, Please try agin')
            return self.import_csv()
        
        return self.save()