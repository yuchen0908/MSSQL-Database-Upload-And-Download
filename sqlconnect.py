import pyodbc
from datetime import datetime


def tuple_list_trans(tpl_list):
    """
    Function to convert a tuple list for upload to a MSSQL accepted string of tuples
    :param tpl_list:
    :return:
    """
    temp_str = ""
    for tpl in tpl_list:
        temp_str += "(" + ",".join(["'" + str(item).replace("\'", "\"") + "'" for item in tpl]) + "),"
    return temp_str[:-1]


class SqlConnect(object):

    """
    v1.0 MsSQL Server Connection Class Built On pyodbc
        # To Do: This Class Needs Exception Functions
    """

    def __init__(self, SQL_HOST, SQL_USER, SQL_PASS, SQL_DB):
        """
        :param SQL_HOST: Server Host IP
        :param SQL_USER: Username for the server
        :param SQL_PASS: Credential of the username
        :param SQL_DB: Database name
        """
        self.SQL_HOST = SQL_HOST
        self.SQL_USER = SQL_USER
        self.SQL_PASS = SQL_PASS
        self.SQL_DB = SQL_DB
        self.SQL_PORT = "1433"
        self.SQL_DRIVER = "SQL SERVER"

    def write_query_pyodbc(self, query):

        #print(query)
        cnxn = pyodbc.connect('DRIVER={SQL SERVER};SERVER=%s;PORT=%s;DATABASE=%s;UID=%s;PWD=%s' % (
            self.SQL_HOST, self.SQL_PORT, self.SQL_DB, self.SQL_USER, self.SQL_PASS))
        cursor = cnxn.cursor()
        cursor.execute(query)
        cnxn.commit()
        cnxn.close()

    def read_query_pyodbc(self, query):

        #print(query)
        cnxn = pyodbc.connect('DRIVER={SQL SERVER};SERVER=%s;PORT=%s;DATABASE=%s;UID=%s;PWD=%s' % (
            self.SQL_HOST, self.SQL_PORT, self.SQL_DB, self.SQL_USER, self.SQL_PASS))
        cursor = cnxn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cnxn.close()
        return result

    def single_row_pyodbc(self, tpl, SQL_TABLE):

        cnxn = pyodbc.connect('DRIVER={SQL SERVER};SERVER=%s;PORT=%s;DATABASE=%s;UID=%s;PWD=%s' % (
            self.SQL_HOST, self.SQL_PORT, self.SQL_DB, self.SQL_USER, self.SQL_PASS))
        cursor = cnxn.cursor()
        temp_str = "(" + ",".join(["?" for i in range(0, len(tpl))]) + ")"
        query = "INSERT INTO [dbo].[%s] VALUES " % SQL_TABLE + temp_str
        #print(query)
        cursor.execute(query, tpl)
        cnxn.commit()
        cnxn.close()

    def multiple_row_pyodbc(self, tpl_list, SQL_TABLE, load_size=500):

        if len(tpl_list) % load_size > 0:
            loop_num = int(len(tpl_list) / load_size) + 1
        else:
            loop_num = int(len(tpl_list) / load_size)

        print("START TO LOAD...\n")
        new_tpl_list = list()

        loop_index = [load_size * i for i in range(0, loop_num)]
        loop_index.append(len(tpl_list))

        for j in range(0, loop_num):
            new_tpl_list.append(tpl_list[loop_index[j]:loop_index[j + 1]])

        # get the number of element in a single tuple
        temp_str = "(" + ",".join(["?" for i in range(0, len(tpl_list[0]))]) + ")"
        query = "INSERT INTO [dbo].[%s] VALUES " % SQL_TABLE + temp_str

        cnxn = pyodbc.connect('DRIVER={SQL SERVER};SERVER=%s;PORT=%s;DATABASE=%s;UID=%s;PWD=%s' % (
            self.SQL_HOST, self.SQL_PORT, self.SQL_DB, self.SQL_USER, self.SQL_PASS))
        cursor = cnxn.cursor()

        for k in range(0, loop_num):
            print("LOADING %d OUT OF %d" % (k + 1, loop_num))
            #print(query)
            current_time = datetime.now()
            cursor.executemany(query, new_tpl_list[k])
            cnxn.commit()
            print(datetime.now()-current_time)

        cnxn.close()

    def multiple_row_pyodbc_fast(self, tpl_list, SQL_TABLE, load_size=500):

        if len(tpl_list) % load_size > 0:
            loop_num = int(len(tpl_list) / load_size) + 1
        else:
            loop_num = int(len(tpl_list) / load_size)

        print("START TO LOAD...\n")
        new_tpl_list = list()

        loop_index = [load_size * i for i in range(0, loop_num)]
        loop_index.append(len(tpl_list))

        for j in range(0, loop_num):
            new_tpl_list.append(tpl_list[loop_index[j]:loop_index[j + 1]])

        # get the number of element in a single tuple
        query = "INSERT INTO [dbo].[%s] VALUES " % SQL_TABLE

        cnxn = pyodbc.connect('DRIVER={SQL SERVER};SERVER=%s;PORT=%s;DATABASE=%s;UID=%s;PWD=%s' % (
            self.SQL_HOST, self.SQL_PORT, self.SQL_DB, self.SQL_USER, self.SQL_PASS))
        cursor = cnxn.cursor()

        for k in range(0, loop_num):
            print("LOADING %d OUT OF %d" % (k + 1, loop_num))
            current_time = datetime.now()
            temp_str = tuple_list_trans(new_tpl_list[k])
            cursor.execute(query + temp_str + ";")
            cnxn.commit()
            print(datetime.now() - current_time)

        cnxn.close()

    def find_table(self, SQL_TABLE):
        """
        :return: Checking whether the SQL TALBE exists or not
        """
        query = "SELECT * FROM " + self.SQL_DB + ".INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';"
        table_list = self.read_query_pyodbc(query)
        for item in table_list:
            if SQL_TABLE in item:
                return True
        return False

    def find_column(self, SQL_TABLE, column_index=3):
        """
        :param column_index: the index of database's information schema that indicates column name
        :return: a list of column's that SQL TABLE has
        """
        query = "SELECT * FROM " + self.SQL_DB + ".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + SQL_TABLE + "';"
        response = self.read_query_pyodbc(query)
        column_list = list()
        for item in response:
            column_list.append(item[column_index])
        return column_list
