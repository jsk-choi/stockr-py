import _secrets as sec
import pyodbc 

def logmsg(msg):

    conn = pyodbc.connect(sec.db_conn)
    cursor = conn.cursor()
    insert_stmt = "insert into [dbo].[Log] (SystemTime, Msg) values (getdate(), '" + msg + "')"
    cursor.execute(insert_stmt)
    cursor.commit()
    conn.close()
