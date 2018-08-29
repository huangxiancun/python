# -*- coding: utf-8 -*-

import json
import sys
sys.path.append('../..')
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


from config import SQLConfig  #, SPARQLConfig

class SingleStock(object):
    """
    dataType: 1, 2, 5, 6, 7, 9, 10 are OK

    params = {"version": "v0.1.0",
              "stockId": "600000",
              "dataType": 1,
              "detailType": "stock_name",
              "relatedTime":"",
              "startTime":"",
              "endTime":"",
             }
    """

    def __init__(self, params):
        if isinstance(params, dict):
            self.params = params
        else:
            self.params = json.loads(params)

        self.version = params.get("version")
        self.stockId = params.get("stockId")
        self.dataType = params.get("dataType")
        self.detailType = params.get("detailType")
        self.relatedTime = params.get("relatedTime")
        self.startTime = params.get("startTime")
        self.endTime = params.get("endTime")


        self.available_data = {}
        if self.dataType == 6 or self.dataType == 9:
            try:
                with open("app/singledata/available.json") as f:
                    self.available_data = json.load(f)
            except Exception as e:
                print(e.message)
                with open("available.json") as f:
                    self.available_data = json.load(f)

        self.mysql_user = SQLConfig.USER
        self.mysql_passpwd = SQLConfig.PASSWD
        self.mysql_db = SQLConfig.DB
        #self._connect_sql()
        if self.stockId:
            self.id_stock = self.attribute_id("StockCode", "stock_code", self.stockId)

        self.results = {}

    def attribute_id(self, table_name, attribute_name, value):
        id_ = None
        s_sql = 'select id from %s where %s="%s"'%(table_name, attribute_name, value)
        #print(s_sql)
        values = self.mysql_db_execute(s_sql)
        if len(values) >= 1:
            id_ = values[0][0]

        return id_

    def mysql_db_execute(self, query_statement, get_column_name=False):

        db_engine = create_engine('mysql+pymysql://%s:%s@localhost/%s?charset=utf8'%(self.mysql_user, self.mysql_passpwd, self.mysql_db))
        mysqlSessionMaker = sessionmaker(bind=db_engine)
        mysqlSession = mysqlSessionMaker()
        try:
            # print "1:, ", query_statement
            res = mysqlSession.execute(query_statement)
            # print "2:, ", query_statement
            mysqlSession.commit()
            if not get_column_name:
                return res.fetchall()
            else:
                c_tuple = res.cursor.description
                c_name = [k[0] for k in c_tuple]
                return c_name

        except Exception as err:
            # print err.message
            mysqlSession.rollback()
        finally:
            mysqlSession.close()
            db_engine.dispose()
    def resp_code(self):
        if self.results:
            self.results["respCode"] = "1000"
            return self.results
        elif self.dataType == 0:
            self.results["respCode"] = "0000"
        elif self.dataType == 1:
            self.results["respCode"] = "0001"
            return self.results
        elif self.dataType == 2:
            self.results["respCode"] = "0002"
            return self.results
        elif self.dataType == 3:
            self.results["respCode"] = "0003"
            return self.results
        elif self.dataType == 4:
            self.results["respCode"] = "0004"
            return self.results
        elif self.dataType == 5:
            self.results["respCode"] = "0005"
            return self.results
        elif self.dataType == 6:
            self.results["respCode"] = "0006"
            return self.results
        elif self.dataType == 7:
            self.results["respCode"] = "0007"
            return self.results
        elif self.dataType == 8:
            self.results["respCode"] = "0008"
            return self.results
        elif self.dataType == 9:
            self.results["respCode"] = "0009"
            return self.results
        elif self.dataType == 10:
            self.results["respCode"] = "0010"
            return self.results
        else:
            self.results["respCode"] = "0000"
            return self.results

    ###@staticmethod
    def latest_sql(self):
        sql_s = """
        select stock_code as stockUid, 
               price as latestPrice, 
               price_change as latestGainRate,
               price_change_rate as latestGainRate,
               volume as latestCumVolume,
               amount as latestCumAmount,
               amplitude as latestCumAmount
               from DayTicks_help_  
        """
        #self.data_execute(sql_s)
        return sql_s
    def brief_sql(self, multiple=False):
        sql_s_m = """
        select StockName.stock_name as company,
               StockCode.stock_code as stockUid, 
               AllTicks.price as latestPrice, 
               AllTicks.price_change as latestGainRate,
               AllTicks.volume as latestCumVolume,
               AllTicks.amount as latestCumAmount,
               AllTicks.amplitude as latestCumAmount
               from AllTicks  
               left join StockCode on AllTicks.stock_code=StockCode.id 
               left join Stock on StockCode.id=Stock.stock_code 
               left join StockName on StockName.id=Stock.stock_name 
        """
        if multiple:
            return sql_s_m
        sql_s = sql_s_m + " where AllTicks.stock_code=%s "%self.id_stock
        if self.relatedTime:
            id_ = self.attribute_id("DayDate", "day_date", self.relatedTime)
            sql_s += ' and AllTicks.day_date=%s '%(id_)
        elif self.startTime and self.endTime:
            start_id = self.attribute_id("DayDate", "day_date", self.startTime)
            end_id = self.attribute_id("DayDate", "day_date", self.endTime)
            sql_s += ' and day_date > %s and day_date < %s'%(start_id, end_id)
        else:
            sql_s += '\n order by day_date desc limit 1'
        return sql_s

    def diagnose_sql(self, id_stock):
        sql_s = """
        SELECT StockName.stock_name as companyName, 
               StockCode.stock_code as stockCode,
               AllTicks.price as price,
               AllTicks.price_change as gainRate,
               composite_score as syntheticalScores, 
               technical as techScore, 
               fundamental as basicScore, 
               informational as newsScore, 
               funding as capitalScore, 
               industrial as industryScore, 
               position_advise as diagnosis,
               short_term as shortTermTendence,
               medium_term as middleTermTendence,
               long_term as longTermTendence,
               AllTicks.day_date as pubDate
        FROM StockDiagnose  
        left join StockCode on StockDiagnose.stock_code=StockCode.id  
        left join Stock on StockCode.id=Stock.stock_code 
        left join StockName on StockName.id=Stock.stock_name 
        left join AllTicks on StockDiagnose.stock_code=AllTicks.stock_code
        where StockDiagnose.stock_code=%s
        order by pubDate desc limit 1
        """%id_stock
        return sql_s
    def basic_sql(self, id_stock):
        sql_s = """
        SELECT StockCode.stock_code,
        StockName.stock_name,
        comp_name, 
        ExchangeType.exchange,
        Board.board,
        listing_date, 
        ipo_date, 
        issue_share, issue_price, issue_pe_init, issue_way_init, total_share, circulating_share, national_share, corporate_share, founder_share, transferred_allotted_share, b_share, h_share, issue_type, issue_start_date, issue_property, StockType.issue_stock_type,issue_way, issue_public_share, issue_price_rmb, issue_price_foreign, actual_raise_capital, actual_issue_cost, issue_pe, online_price_lottery_rate, 2nd_ration_lottery_rate, Area.area 
        FROM Stock  
        left join StockCode on Stock.stock_code=StockCode.id  
        left join StockName on Stock.stock_name=StockName.id  
        left join ExchangeType on Stock.exchange=ExchangeType.id  
        left join Board on Stock.board=Board.id  
        left join StockType on Stock.issue_stock_type=StockType.id  
        left join Area on Stock.area=Area.id  
        where Stock.stock_code=%s

        """%id_stock
        return sql_s
    def special_sql(self):
        sqls = []
        if isinstance(self.detailType, list):
            special_data = self.detailType
        else:
            special_data = [self.detailType]

        for i in special_data:
            for attr in self.available_data["available_data"]:
                if i == attr["attribute_name"]:
                    if attr["is_foreign"]:
                        foreign_table = attr["is_foreign"]["table_name"]
                        foreign_attr = attr["is_foreign"]["attribute_name"]
                        sql_s = """
                        select %s.%s
                        from  %s
                        left join %s on %s.id=%s.%s
                        where %s.stock_code=%s
                        """%(foreign_table, foreign_attr, attr["table_name"],
                             foreign_table, foreign_table, attr["table_name"],i,
                             attr["table_name"], self.id_stock)
                        if attr["table_name"] == "AllTicks":
                            sql_s += " order by day_date desc limit 1"
                        sqls.append(sql_s)
                    else:
                        sql_s = """
                        select %s
                        from %s
                        where %s.stock_code=%s
                        """%(i,attr["table_name"],attr["table_name"],self.id_stock)
                        if attr["table_name"] == "AllTicks":
                            sql_s += " order by day_date desc limit 1"
                        sqls.append(sql_s)
        return sqls

    def holder_sql(self, id_stock):
        sql_s = """
        SELECT DayDate.day_date as timestamp,
        StockName.stock_name as subject, 
        StockCode.stock_code as stockCode,
        Holders.shareholder_name as object,
        concat('StockName@', StockName.id) as subjectId,
        concat('Holders@', HasHolder.shareholder_name) as objectId,
        share_amount as shareNum, 
        share_ratio as shareRatio 
        FROM HasHolder  
        left join StockCode on HasHolder.stock_code=StockCode.id  
        left join Holders on HasHolder.shareholder_name=Holders.id  
        left join DayDate on HasHolder.day_date=DayDate.id  
        left join Stock on StockCode.id=Stock.stock_code 
        left join StockName on StockName.id=Stock.stock_name 
        where HasHolder.stock_code=%s
        order by HasHolder.day_date desc limit 10
    """%id_stock
        return sql_s
    def available_data_(self, id_stock, attribute=None):
        if self.dataType == 1:
            self.available_list = ["open", "high", "low", "pb", "pelfy", "upper_limit", "lower_limit", "pre_close", "totmktcap", "crps", "upps", "roediluted", "napsnewp"]
            if attribute in self.available_list:
                sql_s = """
                select %s
                       from AllTicks  
                       where AllTicks.stock_code=%s 
                """%(attribute, self.id_stock)
                columns = self.mysql_db_execute(sql_s, get_column_name=True)
                values = self.mysql_db_execute(sql_s)
                if values:
                    if self.startTime or self.startTime:
                        self._format_results(values, columns)

                    else:
                        self._format_results_dict(values, columns)
                return 
            return
        elif self.dataType == 2:
            sql_s = self.diagnose_sql(self.id_stock)

    def position_sql(self, id_stock):
        sql_s = """
        select StockName.stock_name as stockShortName,
               StockCode.stock_code as stockCode, 
               AllTicks.price as price, 
               AllTicks.price_change as gainRate 
               from AllTicks  
               left join StockCode on AllTicks.stock_code=StockCode.id 
               left join Stock on StockCode.id=Stock.stock_code 
               left join StockName on StockName.id=Stock.stock_name 
               where AllTicks.stock_code=%s 
               order by day_date desc limit 1
        """%id_stock
        return sql_s
    def return_data(self):
        sql_s = ""
        if not self.dataType:
            sql_s = self.latest_sql()
            #print(sql_s)
        elif self.dataType == 0:
            sql_s = self.latest_sql()
        elif self.dataType == 1:
            sql_s = self.brief_sql()
        elif self.dataType == 2:
            sql_s = self.diagnose_sql(self.id_stock)
            #print(sql_s)
        elif self.dataType == 6:
            #attr_ = [a["attribute_name"] for a in self.available_data["available_data"]]
            #end_results  = {}
            sql_list = self.special_sql()
            #for sql in sql_list:
            #    print(sql)
            for i in sql_list:
                columns = self.mysql_db_execute(i, get_column_name=True)
                values = self.mysql_db_execute(i)
                if values:
                    self._format_results_dict(values, columns)
            return

        elif self.dataType == 7:
            sql_s = self.holder_sql(self.id_stock)
            print(sql_s)
        elif self.dataType == 9:
            end_results = []
            en = []
            cn = []
            for i in self.available_data.get("available_data"):
                en.append(i["attribute_name"])
                cn.append(i["cn"])
            end_results.append(en)
            end_results.append(cn)
            self.results["availableData"] = end_results
            return self.results
        elif self.dataType == 10:
            sql_s = self.position_sql(self.id_stock)
        
        #print(sql_s)
        self.data_execute(sql_s)

    ###@staticmethod
    def _data_execute(self, sql_s):

        columns = self.mysql_db_execute(sql_s, get_column_name=True)
        values = self.mysql_db_execute(sql_s)
        if values:
            self._format_results(values, columns)

    def data_execute(self, sql_s):

        columns = self.mysql_db_execute(sql_s, get_column_name=True)
        values = self.mysql_db_execute(sql_s)
        if values:
            if self.startTime or self.startTime or self.dataType ==7 or self.dataType is None or self.stockId is None:
                self._format_results(values, columns)

            else:
                self._format_results_dict(values, columns)
        #return self.results
    def _format_results_dict(self, values, column_name):
        dict_ = dict(zip(column_name, values[0]))

        if self.results.get("brief"):
            self.results["brief"].update(dict_)
        else:
            self.results["brief"] = dict_


        return
    def _format_results(self, values, column_name):
        
        end_results = {"title":[], "data":[]}
        for t in column_name:
            end_results["title"].append(t)
        for v in values:
            tmp_list = []
            for i in v:
                tmp_list.append(i)
            end_results["data"].append(tmp_list)

        self.results["timeRelatedData"] = end_results
        return
if __name__ == "__main__":
    print("##################################\n"\
          "Waiting to modify:\n"\
          "1. day_date to day_date\n"\
          "2.\n"\
          "##################################\n")
    params = {"stockId": "600000", "dataType": 6, "detailType": ["price", "low", "board"]}
    #params = {"stockId": "000059", "dataType": 10}
    #params = {"dataType": 0}
    #params = {}
    #params = {"stockId": "000059", "dataType": 9}
    #params = {"stockId": "000059", "dataType": 1, "startTime":"2017-02-27", "endTime":"2017-05-27"}
    #params = {"stockId": "600000", "dataType": 1,}
    #params = {"stockId": "000059", "dataType": 1, "relatedTime":"2017-03-27"}
    #params = {"stockId": "000059", "dataType": 6, "detailType": "high"}
    sd = SingleStock(params)
    sd.return_data()
    sd.resp_code()
    ##sd.available_data(sd.id_stock)
    ##print(sd.available_list)
    #
    print(sd.results)
    #sd.latest_sql()
    #print(sd.results['timeRelatedData']['data'])


