# -*- coding: utf-8 -*-

import json

class SingleStock:
    """
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

        self._connect_sql()
        if self.stockId:
            # self.id_stock = self._stock_id()
            self.id_stock = self.attribute_id("StockCode", "stock_code", self.stockId)

        self.results = {}

    def __del__(self):

        if self.conn:
            self.conn.close()

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
    def _stock_id(self):

        id_stock = None
        self.cursor.execute('select id from StockCode where stock_code=%s', (self.stockId,))
        values = self.cursor.fetchall()
        if len(values) >= 1:
            id_stock = values[0][0]

        return id_stock
    def attribute_id(self, table_name, attribute_name, value):
        id = None
        s_sql = 'select id from %s where %s=%s'%(table_name, attribute_name, value)
        self.cursor.execute(s_sql)
        values = self.cursor.fetchall()
        if len(values) >= 1:
            id = values[0][0]

        return id

    def _get_property(self, table_name):
        column_name = None
        sql_s = 'select * from %s'%table_name
        self.cursor.execute(sql_s)
        column_name = self.cursor.column_names
        return column_name

    def _connect_sql(self):
        import mysql.connector

        config = {
          'user': 'newuser',
          'password': 'password',
          'host': '127.0.0.1',
          'database': 'kg_schema',
          'raise_on_warnings': True,
          'use_pure': False,
          'use_unicode':True,
        }

        try:
            self.conn = mysql.connector.connect(**config)
            self.cursor = self.conn.cursor(buffered=True)
            print("MYSQL connect success!")
        except:
            print("MYSQL connect error!")


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
        else:
            self.results["respCode"] = "0100"
            return self.results
            

    def brief_sql(self, id_stock, table_name):
        s_sql = self._base_sql(id_stock, table_name)
        if self.relatedTime:
            id = self.attribute_id("Daydate", "day_date", self.relatedTime)
            s_sql += ' and %s.created_at=%s'%(table_name, id)
        elif self.startTime and self.endTime:
            start_id = self.attribute_id("Daydate", "day_date", self.startTime)
            end_id = self.attribute_id("Daydate", "day_date", self.endTime)
            s_sql += ' and created_at > %s and created_at < %s'%(start_id, end_id)
        else:
            s_sql += 'limit 1'
        return s_sql
    def diagnose_sql(self, id_stock, table_name):
        return self._base_sql(id_stock, table_name)

    def k_sql(self, stockId):
        pass

    def day_sql(self, stockId):
        pass
    def fundamental_sql(self, id_stock, table_name):
        return self._base_sql(id_stock, table_name)
    def special_sql(self, stockId, detailType):
        for k, v in self.schema.items():
            if detailType in v:
                table_name = k

        pass
    def holder_sql(self, id_stock, table_name):
        s_sql = self._base_sql(id_stock, table_name)
        s_sql += ' order by pub_date desc, share_ratio desc limit 10'
        return s_sql
    def news_sql(self, stockId):
        pass


    def _base_sql(self, id_stock, table_name):
        select_para = ""
        table_para = table_name + ', '
        where_para = ""
        schema_tuple = self._get_schema_tuple(table_name)
        t1 = [i[0] for i in schema_tuple]
        print("t1", t1)
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
        sql_s = "SELECT %s FROM %s WHERE %s.stock_code=%s %s"%(select_para, table_para, table_name, id_stock, where_para)
        #print(sql_s)
        return sql_s
    def unique(self, sequence):
        seen = set()
        return [x for x in sequence if not (x in seen or seen.add(x))]
    def return_data(self):
        sql_sentence = None
        values = None
        if self.dataType == 1:
            table_name= 'Ticks'
            sql_sentence = self.brief_sql(self.id_stock, table_name)
        elif self.dataType == 2:
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
            sql_sentence = self.spacial_sql(self.stockId, self.detailType)
        elif self.dataType == 7:
            table_name= 'HasHolder'
            if self.id_stock:
                sql_sentence = self.holder_sql(self.id_stock, table_name)
        elif self.dataType == 8:
            sql_sentence = self.news_sql(self.stockId)
        elif self.dataType == 9:
            table_name= 'StockDiagnose'
            sql_sentence = self.diagnose_sql(self.id_stock, table_name)
        elif self.dataType == 10:
            pass
        elif self.dataType == 11:
            pass
        else:
            return self.results
        if sql_sentence and table_name:
            self.cursor.execute(sql_sentence)
            values = self.cursor.fetchall()
            #column_name = self.cursor.column_names
            column_name = self._get_property(table_name)
        if values:
            values = self.unique(values)
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
    params = {"stockId": "000059", "dataType": 7}
    sd = SingleStock(params)
    #sd.return_data()
    #sd.stock_id()
    sd.return_data()
    sd.resp_code()
    print(sd.results)
