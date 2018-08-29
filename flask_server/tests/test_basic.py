# -*- coding: utf-8 -*-

import unittest
from flask import current_app
from app import create_app
#from test_funs import test_single
#from test_funs import test_multiplestock
import requests
import os
import json


class BasicsTestCase(unittest.TestCase):

    
    def setUp(self):
        """
        This is the preparation for a test case.
        """
        #self.url = 'http://127.0.0.1:5000'
        self.headers = {'content-type':'application/json','token':'"62ac4031f2cc03b43dfe64315aa8b642a78722a1bb4d9d14888988df2fb2d998'}
        self.app = create_app("test")
        self.app_context = self.app.app_context()
        self.app_context.push()


    
    def test_app_exists(self):
        
        self.assertFalse(current_app is None)

    def test_app_is_testing(self):
       
        self.assertTrue(current_app.config['TESTING'])


    def test_single_data(self,url = 'http://localhost:5001/singlestock/data/'):
        """
        need testing single stock data interface.
        """
        data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "data/")
        myjson_path = os.path.join(data_dir, "test_data.json")
        response_path = os.path.join(data_dir, "response.json")
        
        with open (myjson_path,'r') as fin:
		
           
            data = json.dumps(json.load(fin))
	   # print "++++++++++++++++++++"
	    #print data
            r = requests.post(url,data = data,headers =self.headers,verify = False)
            #print r.json
	   # print '=================='
	    print r.text
	   # print '==================='
	    print r
	    #r.text["respCode"]
            self.assertEqual(r.status_code,200)
	    #self.assertEqual(r.text['respCode']='1000'
            result = json.dumps(r.text)
	    #self.assertEqual(int(result['respCode']),1000)
            with  open (response_path,'w') as fo:
		json.dump(result,fo)
           
    def tearDown(self):
        """
        after testing clean up.
        """
        #print "============"
        self.app_context.pop()
        #print "end"
    
    
if __name__ == '__main__':
    
    url = "" #测试接口
    
    test_single_data = BasicsTestCase()
    test_single_data.test_single_data(url)
