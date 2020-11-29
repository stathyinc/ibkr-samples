import sys
sys.path.append('./')

import argparse
import datetime
import collections
import inspect

import logging
import time
import os.path
import random

import threading
import numpy
from pandas import pandas, DataFrame, Series
import pandas_market_calendars as mcal
pandas.set_option('precision', 2)
pandas.set_option('max_colwidth', 16)
pandas.set_option('max_rows', 25)
pandas.set_option('large_repr', 'truncate')
pandas.set_option('chained_assignment', None)

from ibapi import wrapper
from ibapi import utils

from ibapi.client import EClient
from ibapi.utils import iswrapper

# types
from ibapi.common import * # @UnusedWildImport
from ibapi.order_condition import * # @UnusedWildImport
from ibapi.contract import * # @UnusedWildImport
from ibapi.order import * # @UnusedWildImport
from ibapi.order_state import * # @UnusedWildImport
from ibapi.execution import Execution
from ibapi.execution import ExecutionFilter
from ibapi.commission_report import CommissionReport
from ibapi.ticktype import * # @UnusedWildImport
from ibapi.tag_value import TagValue

from ibapi.account_summary_tags import *

# enable logging when member vars are assigned
Order.__setattr__ = utils.setattr_log
Contract.__setattr__ = utils.setattr_log
DeltaNeutralContract.__setattr__ = utils.setattr_log
TagValue.__setattr__ = utils.setattr_log
TimeCondition.__setattr__ = utils.setattr_log
ExecutionCondition.__setattr__ = utils.setattr_log
MarginCondition.__setattr__ = utils.setattr_log
PriceCondition.__setattr__ = utils.setattr_log
PercentChangeCondition.__setattr__ = utils.setattr_log
VolumeCondition.__setattr__ = utils.setattr_log

BOLD = '\033[1m'
DIM = '\033[2m'
UNDERLINE = '\033[4m'
WHITE  = '\033[37m'
GREEN = '\033[92m'
YELLOW = '\033[93m'
RED = '\033[91m'
PURPLE = '\033[95m'
CYAN = '\033[96m'
DARKCYAN = '\033[36m'
BLUE = '\033[94m'
END = '\033[0m'
CLEAR = '\033[2J'

def SetupLogger():
    recfmt = '(%(threadName)s) %(asctime)s.%(msecs)03d %(levelname)s %(filename)s:%(lineno)d %(message)s'
    timefmt = '%y%m%d_%H:%M:%S'

    if not os.path.exists("log"):
        os.makedirs("log")

    logging.basicConfig(filename=time.strftime("log/pyibapi-data.log"),
        filemode="w",
        level=logging.INFO,
        format=recfmt, datefmt=timefmt)
    logger = logging.getLogger()
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logger.addHandler(console)

def printWhenExecuting(fn):
    def fn2(self):
        print("   doing", fn.__name__)
        fn(self)
        print("   done w/", fn.__name__)

    return fn2

def printinstance(inst:Object):
    attrs = vars(inst)
    print(', '.join("%s: %s" % item for item in attrs.items()))

class Activity(Object):
    def __init__(self, reqMsgId, ansMsgId, ansEndMsgId, reqId):
        self.reqMsdId = reqMsgId
        self.ansMsgId = ansMsgId
        self.ansEndMsgId = ansEndMsgId
        self.reqId = reqId


class RequestMgr(Object):
    def __init__(self):
        # I will keep this simple even if slower for now: only one list of
        # requests finding will be done by linear search
        self.requests = []

    def addReq(self, req):
        self.requests.append(req)

    def receivedMsg(self, msg):
        pass

# ! [socket_declare]
class TestClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)
        # ! [socket_declare]

        # how many times a method is called to see test coverage
        self.clntMeth2callCount = collections.defaultdict(int)
        self.clntMeth2reqIdIdx = collections.defaultdict(lambda: -1)
        self.reqId2nReq = collections.defaultdict(int)
        self.setupDetectReqId()

    def countReqId(self, methName, fn):
        def countReqId_(*args, **kwargs):
            self.clntMeth2callCount[methName] += 1
            idx = self.clntMeth2reqIdIdx[methName]
            if idx >= 0:
                sign = -1 if 'cancel' in methName else 1
                self.reqId2nReq[sign * args[idx]] += 1
            return fn(*args, **kwargs)

        return countReqId_

    def setupDetectReqId(self):

        methods = inspect.getmembers(EClient, inspect.isfunction)
        for (methName, meth) in methods:
            if methName != "send_msg":
                # don't screw up the nice automated logging in the send_msg()
                self.clntMeth2callCount[methName] = 0
                # logging.debug("meth %s", name)
                sig = inspect.signature(meth)
                for (idx, pnameNparam) in enumerate(sig.parameters.items()):
                    (paramName, param) = pnameNparam # @UnusedVariable
                    if paramName == "reqId":
                        self.clntMeth2reqIdIdx[methName] = idx

                setattr(TestClient, methName, self.countReqId(methName, meth))

                # print("TestClient.clntMeth2reqIdIdx", self.clntMeth2reqIdIdx)

# ! [ewrapperimpl]
class TestWrapper(wrapper.EWrapper):
    # ! [ewrapperimpl]
    def __init__(self):
        wrapper.EWrapper.__init__(self)

        self.wrapMeth2callCount = collections.defaultdict(int)
        self.wrapMeth2reqIdIdx = collections.defaultdict(lambda: -1)
        self.reqId2nAns = collections.defaultdict(int)
        self.setupDetectWrapperReqId()

    # TODO: see how to factor this out !!

    def countWrapReqId(self, methName, fn):
        def countWrapReqId_(*args, **kwargs):
            self.wrapMeth2callCount[methName] += 1
            idx = self.wrapMeth2reqIdIdx[methName]
            if idx >= 0:
                self.reqId2nAns[args[idx]] += 1
            return fn(*args, **kwargs)

        return countWrapReqId_

    def setupDetectWrapperReqId(self):

        methods = inspect.getmembers(wrapper.EWrapper, inspect.isfunction)
        for (methName, meth) in methods:
            self.wrapMeth2callCount[methName] = 0
            # logging.debug("meth %s", name)
            sig = inspect.signature(meth)
            for (idx, pnameNparam) in enumerate(sig.parameters.items()):
                (paramName, param) = pnameNparam # @UnusedVariable
                # we want to count the errors as 'error' not 'answer'
                if 'error' not in methName and paramName == "reqId":
                    self.wrapMeth2reqIdIdx[methName] = idx

            setattr(TestWrapper, methName, self.countWrapReqId(methName, meth))

            # print("TestClient.wrapMeth2reqIdIdx", self.wrapMeth2reqIdIdx)

# ! [socket_init]
class sincBot(TestWrapper, TestClient):
    def __init__(self):
        TestWrapper.__init__(self)
        TestClient.__init__(self, wrapper=self)
        # ! [socket_init]
        self.logger = logging.getLogger()
        self.runT = None

        self.nKeybInt = 0
        self.started = False
        self.nextValidOrderId = None
        self.permId2ord = {}
        self.reqId2nErr = collections.defaultdict(int)
        self.globalCancelOnly = False
        self.simplePlaceOid = None

        self.id2hist = {}
        self.id2sym = {}
        self.throttleCnt = 49
        self.cacheEnrichTimeout = time.time()

        self.duration = '1 Y'
        self.barSize = '1 day'
        self.type = None
        self.timeStr = datetime.datetime.now().strftime('%Y%m%d %H:%M:%S')
        self.cache = DataFrame()
        self.dataCacheFile = 'hist_data.csv'
        self.cols = None

    def dumpTestCoverageSituation(self):
        for clntMeth in sorted(self.clntMeth2callCount.keys()):
            logging.debug("ClntMeth: %-30s %6d" % (clntMeth,
                                                   self.clntMeth2callCount[clntMeth]))

        for wrapMeth in sorted(self.wrapMeth2callCount.keys()):
            logging.debug("WrapMeth: %-30s %6d" % (wrapMeth,
                                                   self.wrapMeth2callCount[wrapMeth]))

    def dumpReqAnsErrSituation(self):
        logging.debug("%s\t%s\t%s\t%s" % ("ReqId", "#Req", "#Ans", "#Err"))
        for reqId in sorted(self.reqId2nReq.keys()):
            nReq = self.reqId2nReq.get(reqId, 0)
            nAns = self.reqId2nAns.get(reqId, 0)
            nErr = self.reqId2nErr.get(reqId, 0)
            logging.debug("%d\t%d\t%s\t%d" % (reqId, nReq, nAns, nErr))

    @iswrapper
    # ! [nextvalidid]
    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)

        logging.debug("setting nextValidOrderId: %d", orderId)
        self.nextValidOrderId = orderId
        print("NextValidId:{}".format(orderId ) )
    # ! [nextvalidid]

    def keyboardInterrupt(self):
        self.nKeybInt += 1
        if self.nKeybInt == 1:
            self.stop()
        else:
            print("Finishing test")
            self.done = True

    def nextOrderId(self):
        with threading.Lock():
            oid = self.nextValidOrderId
            self.nextValidOrderId += 1
            return oid

    @iswrapper
    # ! [error]
    def error(self, reqId: TickerId, errorCode: int, errorString: str):
        if reqId != -1:
            super().error(reqId, errorCode, errorString)
    # ! [error] self.reqId2nErr[reqId] += 1

    @iswrapper
    # ! [marketdatatype]
    def marketDataType(self, reqId: TickerId, marketDataType: int):
        super().marketDataType(reqId, marketDataType)
        print("MarketDataType. ReqId:", reqId, "Type:", marketDataType)
        # if marketDataType == MarketDataTypeEnum.REALTIME:
        #     self.cancelMktData(reqId)
    # ! [marketdatatype]

    @iswrapper
    # ! [tickprice]
    def tickPrice(self, reqId: TickerId, tickType: TickType, price: float,
        attrib: TickAttrib):
        super().tickPrice(reqId, tickType, price, attrib)
    # ! [tickprice]

    @iswrapper
    # ! [historicaldata]
    def historicalData(self, idx:int, b: BarData):
        self.cacheEnrichTimeout = time.time() + 60
        # super().historicalData(idx, b)
        s = Series(data=[b.average], index=pandas.DatetimeIndex(
            [b.date], dtype='datetime64[ns]', freq='D') )
        self.id2hist[idx]['data'] = self.id2hist[idx]['data'].append(s)
    # ! [historicaldata]

    @iswrapper
    # ! [historicaldataend]
    def historicalDataEnd(self, idx: int, start: str, end: str):
        super().historicalDataEnd(idx, start, end)

        sym = self.id2hist[idx]['symbol']
        r = self.id2hist[idx]['data'].rename(sym).fillna(0)
        with threading.Lock():
            l = self.cache
            self.cache = pandas.concat([l, r], axis=1)

        self.logger.info( '{}) {}'.format(len(self.id2hist), sym) )
        self.id2hist.pop(idx, None)
    # ! [historicaldataend]

    @iswrapper
    # ! [contractdetails]
    def contractDetails(self, idx: int, cd: ContractDetails):
        c = cd.contract
        sid, sym = c.conId, c.symbol

        s = Series(
            data=[],
            dtype=float,
            index=pandas.DatetimeIndex(
                [],
                dtype='datetime64[ns]',
                freq='D' ) )
        self.id2hist[sid] = {
            'symbol': sym,
            'data': s }
        self.reqHistoricalData(
            sid, c, self.timeStr, self.duration,
            self.barSize, self.type, 0, 1, False, [] )
    # ! [contractdetails]

    @iswrapper
    # ! [contractdetailsend]
    def contractDetailsEnd(self, reqId: int):
        pass
    # ! [contractdetailsend]

    # ! [reconnect]
    def reconnect(self):
        if not (self.conn and self.isConnected()):
            self.connect("127.0.0.1", self.portNum, clientId=self.clientId)

            print("client:%s serverVersion:%s connectionTime:%s" % (
                self.clientId,
                self.serverVersion(),
                self.twsConnectionTime() ) )

            t = threading.Thread( target=self.run, daemon=False )
            t.start()
            self.runT = t

            self.reqMarketDataType( MarketDataTypeEnum.FROZEN )
    # ! [reconnect]

    # ! [start]
    def start(self, udf=Series()):
        if self.started:
            return

        self.started = True

        if os.path.isfile( self.dataCacheFile ):
            hdf = pandas.read_csv(
                self.dataCacheFile,
                index_col=0,
                memory_map=True,
                infer_datetime_format=True,
                parse_dates=True )

            if udf.empty:
                self.cache = hdf

            else:
                missing = sorted(set(hdf.columns) - set(udf.array) )
                self.cache = hdf.drop(missing, axis=1).fillna(0)

                remaining_sym = sorted( set(udf.array) - set(hdf.columns) )
                udf = udf.loc[ remaining_sym ]

        for idx, sym in udf.iteritems():
            c = Contract()
            c.symbol = sym
            c.secType = 'STK'
            c.exchange = 'SMART'
            c.currency = 'USD'

            self.reqContractDetails(idx, c)
            time.sleep( .01 + (1 / self.throttleCnt) )
            #Keep queue for processing relatively small to throttle requests
            if idx % 1000 == 0: self.writeCache()

        while len(self.id2hist) > 0 and time.time() < self.cacheEnrichTimeout: time.sleep(1)
        self.cols = numpy.array(self.cache.columns)
    # ! [start]

    # ! [stop]
    def stop(self):
        self.done = True
        try:
            self.disconnect()
        except AttributeError:
            self.logger.debug('Exception: skipping disconnect()')

        self.runT.join()

        self.writeCache()
    # ! [stop]

    # ! [writecache]
    def writeCache(self):
        out = self.dataCacheFile
        if self.cache.empty:
            self.logger.warning( '{R}‼︎{E}{B} Warning: history empty{E}'.format(
                R=RED, B=BOLD, D=DIM, E=END) )

        else:
            self.cache.fillna(0).sort_index(axis=1).to_csv(out)
    # ! [writecache]


def main():
    SetupLogger()
    logging.debug("now is %s", datetime.datetime.now())
    logging.getLogger().setLevel(logging.INFO)

    cmdLineParser = argparse.ArgumentParser("api tests")
    cmdLineParser.add_argument("-a", "--account", action="store", type=str,
        dest="account", help="The account number to use" )
    cmdLineParser.add_argument("-p", "--port", action="store", type=int,
        dest="port", default=7497, help="The TCP port to use" )
    cmdLineParser.add_argument("-C", "--global-cancel", action="store_true",
        dest="global_cancel", default=False, help="whether to trigger a globalCancel req" )
    args = cmdLineParser.parse_args()
    print("Using args", args)
    logging.debug("Using args %s", args)

    # <<<<<<<< BEGIN
    # Be sure to read requirements:
    #   https://interactivebrokers.github.io/tws-api/introduction.html#requirements
    # AND IMPORTANT TWS Setup
    #   https://interactivebrokers.github.io/tws-api/initial_setup.html
    try:
        bot1 = sincBot()
        bot1.clientId = random.randint(1, 32) #This can be random or 0 for master client
        bot1.account = args.account #Set this to your account number
        bot1.portNum = args.port #Set this to the portnumber you configured in Trader
        # 
        #Read constraints in API Doc:
        # https://interactivebrokers.github.io/tws-api/historical_bars.html
        bot1.duration = '1 M' #How far back to go
        bot1.barSize = '1 day'
        bot1.type = 'TRADES'
        bot1.timeStr = datetime.datetime.now().strftime('%Y%m%d %H:%M:%S') #From today

        bot1.reconnect()

        # Add interesting symbols here
        symbols = numpy.array(
            [ 'AAPL',
              'MSFT' ] )
        data = pandas.Series( symbols )
        bot1.start(data) #This begine executions

        print( bot1.cache )

        bot1.stop()

        sys.exit(1)
        # ! [clientrun]

    except:
        raise
    finally:
        bot1.dumpTestCoverageSituation()
        bot1.dumpReqAnsErrSituation()


if __name__ == "__main__":
    main()
