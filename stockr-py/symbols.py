import _conf as cf
import _secrets as sec
import log
import urllib.request, json
import pyodbc 

def symbols_load():

    counter = 0
    response = urllib.request.urlopen(cf.url_symbols_all)
    symbols = json.loads(response.read())

    conn = pyodbc.connect(sec.db_conn)
    cursor = conn.cursor()

    insert_stmt = 'insert into [dbo].[SymbolsStag] ([symbol], [exchange], [name], [date], [type], [iexId], [region], [currency], [isEnabled]) values '
    insert_vals = []

    for s in symbols:

        insert = "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s)" %(
            s["symbol"],
            s["exchange"],
            s["name"].replace("'", "''"),
            s["date"],
            s["type"],
            s["iexId"],
            s["region"],
            s["currency"],
            1 if s["isEnabled"] else 0)

        insert_vals.append(insert)
    
        if counter == 998:

            cursor.execute(insert_stmt + (",".join(insert_vals)))
            cursor.commit()

            log.logmsg("load sym - " + str(len(insert_vals)))

            counter = 0
            insert_vals = []

        counter+=1

    if len(insert_vals) > 0:
        cursor.execute(insert_stmt + (",".join(insert_vals)))
        cursor.commit()

        log.logmsg("load sym - " + str(len(insert_vals)))

    cursor.execute("EXEC dbo.spSymbolsConsolidation")
    cursor.commit()
    conn.close()

