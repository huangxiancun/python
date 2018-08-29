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

        self.results = {}
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
            

    def brief_sparql(self, stockId):
        pass
    def k_sparql(self, stockId):
        pass
    def day_sparql(self, stockId):
        pass
    def fundamental_sparql(self, stockId):
        sparql_sentence = """
        SELECT DISTINCT ?property ?value WHERE {
          ?s :stock_code "%s".
          ?d :stock_code ?s.
          ?d rdf:type :Stock.
          ?d ?property ?value.
        }
        """%(stockId)
        return sparql_sentence
    def special_sparql(self, stockId):
        pass
    def holder_sparql(self, stockId):
        sparql_sentence = """
        SELECT  ?property ?value WHERE {
          ?c1 :stock_code "%s".
          ?c2 :stock_code ?c1.
          ?c2 rdf:type :HasHolder.
          ?c2 ?property ?value.
          ?c2 :share_ratio ?m.
          ?c2 :pub_date ?c3.
          ?c3 :day_date ?n.
        }
        ORDER BY DESC(?n) DESC(?m)

        """%(stockId)
        return sparql_sentence
    def news_sparql(self, stockId):
        pass

    
    def return_data(self):
        if self.dataType == 1:
            sparql_sentence = self.brief_sparql(self.stockId)
        elif self.dataType == 2:
            sparql_sentence = self.k_sparql(self.stockId)
        elif self.dataType == 3:
            sparql_sentence = self.day_sparql(self.stockId)
        elif self.dataType == 4:
            sparql_sentence = self.fundamental_sparql(self.stockId)
        elif self.dataType == 5:
            sparql_sentence = self.spacial_sparql(self.stockId, self.detailType)
        elif self.dataType == 6:
            sparql_sentence = self.holder_sparql(self.stockId)
        elif self.dataType == 7:
            sparql_sentence = self.news_sparql(self.stockId)
        else:
            return self.results
        self.results = self.to_sparql(sparql_sentence)

        for b in self.results["results"]["bindings"]:
            if b["value"]["type"] == "uri":
                attribute = b["property"]["value"].split("#")[-1]
                classid = b["value"]["value"]
                classname = b["value"]["value"].split('/')[-2]
                if classname == "DayDate":
                    attribute = "day_date"
                value = self._get_class_name(attribute, classid)
                value = value["results"]["bindings"]
                if value:
                    value = value[0]["value"]["value"]
                    b["value"]["value"]=value
        self._format_results()
        return self.results

    def _format_results(self):
        end_results = {"title":[], "data":[]}
        for r in self.results["results"]["bindings"]:
            prop = r["property"]["value"].split('#')[-1]
            value = r["value"]["value"]
            if prop != 'type':
                end_results["title"].append(prop)
                end_results["data"].append(value)

        if end_results["data"].count(end_results["data"][0]) > 1:
            if len(end_results["data"]) == len(end_results["title"]):
                
                n = len(set(end_results["title"]))
                tmp_data = [end_results["data"][i:i+n] for i in range(0, len(end_results["data"]), n)]
                #print(tmp_data)
                end_results["data"] = tmp_data
                end_results["title"] = end_results["title"][:len(set(end_results["title"]))]

        self.results = end_results
        return


    def _get_class_name(self, attribute, classid):
        sparql_sentence = """
        SELECT DISTINCT ?value WHERE {
          <%s> :%s ?value.
        }
        """%(classid, attribute)
        class_attribute_value = self.to_sparql(sparql_sentence)
        return class_attribute_value


    ### @staticmethod
    def to_sparql(self, sparql_sentence):
        from SPARQLWrapper import SPARQLWrapper, JSON
        
        sparql = SPARQLWrapper("http://localhost:2020/sparql")
        
        ss = """
            PREFIX :  <http://www.kg.com/kg_schema#>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX map: <http://localhost:2020/resource/#>
            PREFIX db: <http://localhost:2020/resource/>
            %s
        """%sparql_sentence
        #print(ss)

        sparql.setQuery(ss)
        
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        return results


if __name__ == "__main__":
    params = {"stockId": "600000", "dataType": 6}
    sd = SingleStock(params)
    sd.return_data()
    sd.resp_code()
    print(sd.results)
