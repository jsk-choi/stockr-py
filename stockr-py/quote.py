import sys
import _conf as cf
import json
import pprint
import sseclient

import urllib3
import requests
import threading

import db.log as log
from db.dbrep import dbrep

def with_requests(url, chunk):
    
    response = requests.get(url, stream=True)
    client = sseclient.SSEClient(response)

    for event in client.events():
        if (event.data):

            try:
                jsn = json.loads(event.data)

                if not jsn:
                    continue
                
                insCols, insVals = build_ins(jsn[0])

                #jsn_str = json.dumps(jsn[0])
                dbsymins = dbrep('Quote_stag')
                ins_stmt = dbsymins.sql_ins_stmt(insCols, insVals)
                dbsymins.insert_row(insCols, insVals)
                #dbsymins.sp_exec('spQuotesConsolidation', "@msg=?", (str(chunk)))
            except:
                log.logmsg('error : ' + ins_stmt)

def build_ins(s):

    #ins_col = 'INSERT INTO dbo.Quote_stag ('
    #ins_val = 'VALUES ('

    val_tmp = 0

    ins_col = '[symbol],'
    val_tmp = s.setdefault('symbol', None)
    ins_val = quote_surr(str('NULL' if val_tmp is None else val_tmp)) + ','

    ins_col += '[companyName],'
    val_tmp = s.setdefault('companyName', None)
    ins_val += quote_surr(str('NULL' if val_tmp is None else val_tmp)) + ','

    ins_col += '[primaryExchange],'
    val_tmp = s.setdefault('primaryExchange', None)
    ins_val += quote_surr(abbr(str('NULL' if val_tmp is None else val_tmp))) + ','

    ins_col += '[calculationPrice],'
    val_tmp = s.setdefault('calculationPrice', None)
    ins_val += quote_surr(str('NULL' if val_tmp is None else val_tmp)) + ','

    ins_col += '[latestSource],'
    val_tmp = s.setdefault('latestSource', None)
    ins_val += quote_surr(abbr(str('NULL' if val_tmp is None else val_tmp))) + ','

    ins_col += '[latestTime],'
    val_tmp = s.setdefault('latestTime', None)
    ins_val += quote_surr(str('NULL' if val_tmp is None else val_tmp)) + ','















    ins_col += '[open],'
    val_tmp = s.setdefault('open', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[openTime],'
    val_tmp = s.setdefault('openTime', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[close],'
    val_tmp = s.setdefault('close', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[closeTime],'
    val_tmp = s.setdefault('closeTime', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[high],'
    val_tmp = s.setdefault('high', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[low],'
    val_tmp = s.setdefault('low', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[latestPrice],'
    val_tmp = s.setdefault('latestPrice', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[latestUpdate],'
    val_tmp = s.setdefault('latestUpdate', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[latestVolume],'
    val_tmp = s.setdefault('latestVolume', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[iexRealtimePrice],'
    val_tmp = s.setdefault('iexRealtimePrice', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[iexRealtimeSize],'
    val_tmp = s.setdefault('iexRealtimeSize', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[iexLastUpdated],'
    val_tmp = s.setdefault('iexLastUpdated', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[delayedPrice],'
    val_tmp = s.setdefault('delayedPrice', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[delayedPriceTime],'
    val_tmp = s.setdefault('delayedPriceTime', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[oddLotDelayedPrice],'
    val_tmp = s.setdefault('oddLotDelayedPrice', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[oddLotDelayedPriceTime],'
    val_tmp = s.setdefault('oddLotDelayedPriceTime', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[extendedPrice],'
    val_tmp = s.setdefault('extendedPrice', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[extendedChange],'
    val_tmp = s.setdefault('extendedChange', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[extendedChangePercent],'
    val_tmp = s.setdefault('extendedChangePercent', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[extendedPriceTime],'
    val_tmp = s.setdefault('extendedPriceTime', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[previousClose],'
    val_tmp = s.setdefault('previousClose', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[previousVolume],'
    val_tmp = s.setdefault('previousVolume', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[change],'
    val_tmp = s.setdefault('change', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[changePercent],'
    val_tmp = s.setdefault('changePercent', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[volume],'
    val_tmp = s.setdefault('volume', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[iexMarketPercent],'
    val_tmp = s.setdefault('iexMarketPercent', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[iexVolume],'
    val_tmp = s.setdefault('iexVolume', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[avgTotalVolume],'
    val_tmp = s.setdefault('avgTotalVolume', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[iexBidPrice],'
    val_tmp = s.setdefault('iexBidPrice', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[iexBidSize],'
    val_tmp = s.setdefault('iexBidSize', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[iexAskPrice],'
    val_tmp = s.setdefault('iexAskPrice', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[iexAskSize],'
    val_tmp = s.setdefault('iexAskSize', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[marketCap],'
    val_tmp = s.setdefault('marketCap', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[peRatio],'
    val_tmp = s.setdefault('peRatio', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[week52High],'
    val_tmp = s.setdefault('week52High', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[week52Low],'
    val_tmp = s.setdefault('week52Low', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[ytdChange],'
    val_tmp = s.setdefault('ytdChange', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ','

    ins_col += '[lastTradeTime]'
    val_tmp = s.setdefault('lastTradeTime', None)
    ins_val += str('NULL' if val_tmp is None else val_tmp) + ''

    return ins_col, ins_val

def quote_surr(val):
    return "'" + str(val).replace("'", "''") + "'"

def abbr(str):
    return ''.join(list(map(lambda x: x[0], str.split(' '))))

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

syms = []
sym_urls = []
url_templ = cf.url_quote_sse

dbsym = dbrep('vSymbols')
syms = dbsym.get_rows('symbol, exchange, [type]', "[type] = 'cs'")
syms_list = list(map(lambda x: x[0], syms))

chuncked = list(chunks(syms_list, 40))

for chk in chuncked:
#for i in range(0,5):
    sym_csv = ','.join(chk)
    sym_urls.append(url_templ.format(sym_csv))
    print(url_templ.format(sym_csv))

#with_requests(sym_urls[1], 1)

chk = 1

for url in sym_urls:
    threading.Thread(target=with_requests, args=(url,chk)).start()
    chk += 1

