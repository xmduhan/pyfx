
import pandas
from datetime import datetime


data = pandas.read_csv(r'i:\csv\TB_HFMarketsLtd_EURCAD_M1_EveryTick.csv') 


i = 0

OP_BUY = 1
OP_SELL = -1

takeprofit = 0.0005
stoploss = 0.0015

 
def Ask():
    return data["ask"][i]

def Bid():
    return data["bid"][i]

def Rsi():
    return data["rsi"][i]

class TradingRecord:
    OpenTime = ""
    CloseTime = ""
    OpenPrice = 0
    ClosePrice = 0
    OrderType = 0

lastBuyCreated = ""
lastSellCreated = ""

TradingCurrent = {}
TradingHistory = {}

def TimeCurrent():
    return data["time"][i]

def isTradingHour():
    hh24=data["time"][i][11:13]
    if  hh24 in ("23","0","1","2"):
        return True
    else:
        return False

def isSameHour(dt1,dt2):
    if dt1[0:13] == dt2[0:13]:
        return True
    else:
        return False
    

def MinutesBetween(dt1,dt2):
    datetime1= datetime.strptime(dt1,"%Y.%m.%d %H:%M")
    datetime2= datetime.strptime(dt2,"%Y.%m.%d %H:%M")
    dlt = datetime1-datetime2
    return 1.0 * dlt.total_seconds() / 60


def checkForOpen():
    global lastBuyCreated,lastSellCreated
    if isTradingHour() :
        if not lastBuyCreated or not isSameHour(lastBuyCreated,TimeCurrent()):
            if Rsi() < 30: 
                lastBuyCreated = TimeCurrent()
                tr=TradingRecord()
                tr.OpenTime = TimeCurrent()
                tr.OrderType = OP_BUY            
                tr.OpenPrice = Ask()            
                TradingCurrent[TimeCurrent()]=tr            
        if not lastSellCreated or not isSameHour(lastSellCreated,TimeCurrent()):
            if Rsi() > 70:
                lastSellCreated = TimeCurrent()
                tr=TradingRecord()
                tr.OpenTime = TimeCurrent()
                tr.OrderType = OP_SELL            
                tr.OpenPrice = Bid()            
                TradingCurrent[TimeCurrent()]=tr                        
    

def checkForClose():
    global lastBuyCreated,lastSellCreated
    keys = TradingCurrent.keys()
    for key in keys:
        tr=TradingCurrent[key]
        if tr.OrderType == OP_BUY : 
            if Bid() - tr.OpenPrice > takeprofit or tr.OpenPrice - Bid() > stoploss or \
               MinutesBetween(TimeCurrent(),tr.OpenTime) > 120:
                tr.CloseTime = TimeCurrent()
                tr.ClosePrice = Bid()
                del(TradingCurrent[key])
                TradingHistory[key]=tr
        if tr.OrderType == OP_SELL :
            if tr.OpenPrice - Ask() > takeprofit or Ask() - tr.OpenPrice > stoploss or \
               MinutesBetween(TimeCurrent(),tr.OpenTime) > 120:
                tr.CloseTime = TimeCurrent()
                tr.ClosePrice = Ask()
                del(TradingCurrent[key])
                TradingHistory[key]=tr
            
def start():
    checkForOpen()
    checkForClose()
       

for i in range(len(data)):
    start()



file = open('1.csv', 'w')
file.write("open_time,close_time,open_price,close_price,order_type\n")
for key in TradingHistory :
    tr = TradingHistory[key]
    file.write(tr.OpenTime+",")
    file.write(tr.CloseTime+",")
    file.write(str(tr.OpenPrice)+",")
    file.write(str(tr.ClosePrice)+",")
    file.write(str(tr.OrderType))
    file.write("\n")
file.close()