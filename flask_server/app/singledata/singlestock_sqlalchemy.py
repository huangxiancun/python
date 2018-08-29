# -*- coding: utf-8 -*-

import json
import sys
sys.path.append('../..')
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


from config import SQLConfig, SPARQLConfig

class SingleStock:
    """
    dataType: 1, 2, 5, 6, 7, 10 are OK

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

        self.schema = {}
        self.schema_d = {}
        self.get_schema()

        self.mysql_user = SQLConfig.USER
        self.mysql_passpwd = SQLConfig.PASSWD
        self.mysql_db = SQLConfig.DB
        #self._connect_sql()
        if self.stockId:
            self.id_stock = self.attribute_id("StockCode", "stock_code", self.stockId)

        self.results = {}


    def get_schema(self, schema_file="../tosparql/external_dict/kg_schema.json", schema_dict="../tosparql/external_dict/kg_schema_class_map.json"):
        try:
            with open(schema_file) as ff:
                schema = json.load(ff)
            with open(schema_dict) as fd:
                schema_d = json.load(fd)
        except:
            schema = None
            schema_d = None
            print("json files are not exist")

        if schema:
            self.schema = schema
        if schema_d:
            self.schema_d = schema_d
        return

    def _get_schema_tuple(self, classname):
        schema_tuple = []
        for cn in self.schema[classname]:
            if isinstance(cn, dict):
                t1 = cn["column"]
                t2 = cn["foreign_table"]
                t3 = self.schema_d[t2]
                schema_tuple.append((t1, t2, t3))

        return schema_tuple

    def attribute_id(self, table_name, attribute_name, value):
        id_ = None
        s_sql = 'select id from %s where %s="%s"'%(table_name, attribute_name, value)
        print(s_sql)
        values = self.mysql_db_execute(s_sql)
        if len(values) >= 1:
            id_ = values[0][0]

        return id_

    def _get_property(self, table_name):
        s_sql = 'select * from %s limit 1'%table_name
        column_name = self.mysql_db_execute(s_sql, get_column_name=True)
        return column_name

    def _connect_sql(self):
        from sqlalchemy import create_engine

        engine = create_engine('mysql://newuser:password@localhost/kg_schema?charset=utf8',echo=True,encoding='utf-8',convert_unicode=True)
        try:
            self.conn = engine.connect(close_with_result=True)
            self.cursor = self.conn.cursor(buffered=True)
            print("MYSQL connect success!")
        except:
            print("MYSQL connect error!")

    def mysql_db_execute(self, query_statement, get_column_name=False):

        db_engine = create_engine('mysql://%s:%s@localhost/%s?charset=utf8'%(self.mysql_user, self.mysql_passpwd, self.mysql_db))
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

        #cnx.close()

    def resp_code(self):
        if self.results:
            self.results["respCode"] = "1000"
            return self.results
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
            

    def brief_sql(self, id_stock, table_name):
        s_sql = self._base_sql(id_stock, table_name)
        if self.relatedTime:
            id_ = self.attribute_id("DayDate", "day_date", self.relatedTime)
            s_sql += ' and %s.created_at=%s '%(table_name, id_)
        elif self.startTime and self.endTime:
            start_id = self.attribute_id("DayDate", "day_date", self.startTime)
            end_id = self.attribute_id("DayDate", "day_date", self.endTime)
            s_sql += ' and created_at > %s and created_at < %s'%(start_id, end_id)
        else:
            s_sql += ' limit 1'
        print(s_sql)
        return s_sql
    def diagnose_sql(self, id_stock, table_name):
        return self._base_sql(id_stock, table_name)

    def k_sql(self, stockId):
        pass

    def day_sql(self, stockId):
        pass
    def fundamental_sql(self, id_stock, table_name):
        return self._base_sql(id_stock, table_name)
    def _special_sql(self, attr_name):
        from mapping_to_json import ToJson
        from min_path import MinPath
        mapping_file = 'sql_rdf_new_mapping.ttl'
        mapping = ToJson(mapping_file)
        mapping.to_json()

        m = MinPath(mapping.data)
        mapping_dict = mapping.mapping_class_attribute
        m.get_mapping_file(mapping_dict)

        sparql_s = m.sparql_sentence(self.stockId, "stock_code", attr_name)
        return sparql_s
    def special_sparql_data(self, attr_name, sparql_p=""):
        if sparql_p:
            sparql_s = """
                prefix :  <http://www.kg.com/kg_schema#>
                #PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                %s
            """ %sparql_p
        else:
            sparql_s = """
                prefix :  <http://www.kg.com/kg_schema#>
                #PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                %s
            """ % self._special_sql(attr_name)

        return sparql_s
    def return_sparql_data(self):
        from SPARQLWrapper import SPARQLWrapper, JSON
        sparql = SPARQLWrapper("http://localhost:2020/sparql")

        results = []
        if isinstance(self.detailType, list):
            for i in self.detailType:
                sparql.setQuery(self.special_sparql_data(i))
                sparql.setReturnFormat(JSON)
                result = sparql.query().convert()
                results.append(result)
            return results

        else:
            sparql.setQuery(self.special_sparql_data(self.detailType))
            sparql.setReturnFormat(JSON)
            result = sparql.query().convert()
            results.append(result)
            return results

    def _format_sparql_result(self, results):
        end_results = {"title": [], "data": []}
        if isinstance(self.detailType, list):
            end_results["title"].extend(self.detailType)
        else:
            end_results["title"].append(self.detailType)

        for result in results:
            tmp_list = []
            for r in result["results"]["bindings"]:
                tmp_list.append(r["l"]["value"])
            end_results["data"].append(tmp_list)
        self.results = end_results


    def holder_sql(self, id_stock, table_name):
        s_sql = self._base_sql(id_stock, table_name)
        s_sql += ' order by pub_date desc, share_ratio desc limit 10'
        return s_sql
    def news_sql(self, stockId):
        pass
    def position_sql(self, id_stock):
        from datetime import datetime
        #current_time = datetime.now().strftime("%Y-%m-%d")
        #stock_id = self.attribute_id("StockCode", "stock_code", self.stockId)
        s_sql = "select StockCode.stock_code, StockName.stock_name,Ticks.price, Ticks.price_change  from Ticks  " \
                "left join StockCode on Ticks.stock_code=StockCode.id " \
                "left join Stock on StockCode.id=Stock.stock_code " \
                "left join StockName on StockName.id=Stock.stock_name "\
                "where Ticks.stock_code=%s order by created_at limit 1"%(id_stock)
        #print(s_sql)
        return s_sql


    def _base_sql(self, id_stock, table_name):
        select_para = ""
        join_para = ""
        schema_tuple = self._get_schema_tuple(table_name)
        t1 = [i[0] for i in schema_tuple]
        print("t1", t1)
        column_name = list(self._get_property(table_name))
        print(column_name)
        if 'id' in column_name:
            column_name.remove("id")
        for i in column_name:
            if i in t1:
                for j in schema_tuple:
                    if i == j[0]:
                        select_para += j[1]+'.' +j[2] + ','
                        #join_para += 'and (' +table_name + '.' + i + ' is null or '+ table_name + '.' +i +'=' +j[1]+ '.id) '
                        join_para += ' left join %s on %s.%s=%s.id '%(j[1], table_name, i, j[1])
            else:
                select_para += i + ', '

        select_para = select_para.strip()[:-1]
        #print(where_para)
        sql_s = "SELECT %s FROM %s %s where %s.stock_code=%s"%(select_para, table_name, join_para, table_name, id_stock)
        #print(sql_s)
        return sql_s

    def _base_sql_bak(self, id_stock, table_name):
        select_para = ""
        table_para = table_name + ', '
        where_para = ""
        schema_tuple = self._get_schema_tuple(table_name)
        t1 = [i[0] for i in schema_tuple]
        # print("t1", t1)
        column_name = list(self._get_property(table_name))
        if 'id' in column_name:
            column_name.remove("id")
        for i in column_name:
            if i in t1:
                for j in schema_tuple:
                    if i == j[0]:
                        select_para += j[1]+'.' +j[2] + ','
                        table_para += j[1] +', '
                        where_para += 'and (' +table_name + '.' + i + ' is null or '+ table_name + '.' +i +'=' +j[1]+ '.id) '
            else:
                select_para += i + ', '

        select_para = select_para.strip()[:-1]
        table_para = table_para.strip()[:-1]
        where_para = where_para.strip()
        #print(where_para)
        s_sql = "SELECT %s FROM %s WHERE %s.stock_code=%s %s"%(select_para, table_para, table_name, id_stock, where_para)
        #print(s_sql)
        return s_sql
    def unique(self, sequence):
        seen = set()
        return [x for x in sequence if not (x in seen or seen.add(x))]
    def return_data(self):
        table_name = None
        sql_sentence = None
        values = None
        if self.dataType == 1:
            table_name= 'Ticks'
            sql_sentence = self.brief_sql(self.id_stock, table_name)
        elif self.dataType == 2:
            table_name= 'StockDiagnose'
            sql_sentence = self.diagnose_sql(self.id_stock, table_name)
        elif self.dataType == 3:
            sql_sentence = self.k_sql(self.stockId)
        elif self.dataType == 4:
            sql_sentence = self.day_sql(self.stockId)
        elif self.dataType == 5:
            table_name= 'Stock'
            if self.id_stock:
                sql_sentence = self.fundamental_sql(self.id_stock, table_name)
        elif self.dataType == 6:
            results = self.return_sparql_data()
            self._format_sparql_result(results)
        elif self.dataType == 7:
            table_name= 'HasHolder'
            if self.id_stock:
                sql_sentence = self.holder_sql(self.id_stock, table_name)
        elif self.dataType == 8:
            sql_sentence = self.news_sql(self.stockId)
        elif self.dataType == 9:
            pass
        elif self.dataType == 10:
            table_name = "position_10"
            sql_sentence = self.position_sql(self.id_stock)
        else:
            return self.results
        if sql_sentence and table_name:
            values = self.mysql_db_execute(sql_sentence)
            if table_name == "position_10":
                column_name = ["stock_code", "stock_name", "price", "price_change"]
            else:
                column_name = self._get_property(table_name)
        if values:
            #values = self.unique(values)
            self._format_results(values, column_name)
            return self.results
        else:
            return self.results

    def _format_results(self, values, column_name):
        end_results = {"title":[], "data":[]}
        for t in column_name:
            end_results["title"].append(t)
        for v in values:
            tmp_list = []
            for i in v:
                tmp_list.append(i)
            end_results["data"].append(tmp_list)

        self.results = end_results
        return


if __name__ == "__main__":
    #params = {"stockId": "000059", "dataType": 6, "detailType": ["comp_name", "shareholder_name"]}
    #params = {"stockId": "000059", "dataType": 2}
    params = {"stockId": "000059", "dataType": 1, "startTime":"2017-02-27", "endTime":"2017-03-27"}
    #params = {"stockId": "000059", "dataType": 1, "relatedTime":"2017-03-27"}
    #params = {"stockId": "000059", "dataType": 6, "detailType": "comp_name"}
    sd = SingleStock(params)
    #sd.return_data()
    #sd.stock_id()
    sd.return_data()
    sd.resp_code()
    print(sd.results)
