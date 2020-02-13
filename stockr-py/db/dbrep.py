import _secrets as sec
import pyodbc 

class dbrep:

    def __init__(self, object_name):
        self.object_name = object_name


    def sp_exec(self, spname):

        conn = pyodbc.connect(sec.db_conn)

        with conn:
            cursor = conn.cursor()
            with cursor:
                cursor.execute(spname)
                cursor.commit()

    def sp_exec(self, spname, param, vals):

        sql = "EXEC " + spname + " " + param
        conn = pyodbc.connect(sec.db_conn)

        with conn:
            cursor = conn.cursor()
            with cursor:
                cursor.execute(sql, vals)
                cursor.commit()

    def get_rows(self, cols, where):

        select_stmt = self.sql_sel_stmt(cols, where)
        conn = pyodbc.connect(sec.db_conn)

        data = []
        with conn:
            cursor = conn.cursor()
            with cursor:
                cursor.execute(select_stmt)
                data = cursor.fetchall()
        
        return data

    def get_row(self, cols, where):

        select_stmt = self.sql_sel_stmt(cols, where)
        conn = pyodbc.connect(sec.db_conn)

        data = []
        with conn:
            cursor = conn.cursor()
            with cursor:
                cursor.execute(select_stmt)
                data = cursor.fetchone()        
        return data

    def insert_row(self, cols, vals):
        ins_stmt = self.sql_ins_stmt(cols, vals)
        conn = pyodbc.connect(sec.db_conn)

        with conn:
            cursor = conn.cursor()
            with cursor:
                cursor.execute(ins_stmt)
                conn.commit()

        return ins_stmt

    def sql_sel_stmt(self, cols, where):

        select_stmt = "SELECT "
        select_stmt += cols if cols else '*'
        select_stmt += " FROM dbo." + self.object_name
        if where:
            select_stmt += " WHERE " + where

        return select_stmt

    def sql_ins_stmt(self, cols, vals):

        #vals = vals.replace("'", "")

        ins_stmt = "INSERT INTO dbo." + self.object_name
        ins_stmt += f" ({cols})" if cols else ''
        ins_stmt += f" VALUES ({vals})"

        return ins_stmt
