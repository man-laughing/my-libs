#!/usr/bin/python

from sys import exit
from pymongo import MongoClient

class mongolib():
    def __init__(self,db_host,db_port,db_name):
        if bool(db_host) == False and bool(db_port) == False and bool(db_name) == False:
            exit()
        self.db_host = db_host  
        self.db_port = db_port  
        self.db_name = db_name
        self.db_conn = MongoClient(self.db_host,self.db_port) 

    def AddOne(self,cont,tbname):
        if bool(cont) == False and bool(tbname) == False:
            exit()
        use_db   = self.db_conn.get_database(self.db_name)
        use_coll = use_db.get_collection(tbname)
        ins_id = []
        for i in cont:
            result = use_coll.insert_one(i)
            ins_id.append(result.inserted_id)
        return ins_id
       
    def DelOne(self,cont,tbname):
        if bool(cont) == False and bool(tbname) == False:
            exit()
        use_db   = self.db_conn.get_database(self.db_name)
        use_coll = use_db.get_collection(tbname)
        result = use_coll.delete_one(cont)
        return result.deleted_count
        
    def UptOne(self,cont1,set1,tbname):
        if bool(cont1) == False and bool(set1) == False and bool(tbname) == False:
            exit()
        use_db   = self.db_conn.get_database(self.db_name)
        use_coll = use_db.get_collection(tbname)
        two = {'$set':set1}
        result = use_coll.update_one(cont1,two)
        return result.modified_count

    def QurOne(self,tbname):
        if bool(tbname) == False:
            exit()
        use_db   = self.db_conn.get_database(self.db_name)
        use_coll = use_db.get_collection(tbname)
        return use_coll.find_one()

    def QurAll(self,tbname):
        if bool(tbname) == False:
            exit()
        use_db   = self.db_conn.get_database(self.db_name)
        use_coll = use_db.get_collection(tbname)
        result = use_coll.find()
        rrr = []
        for i in result:rrr.append(i)
        return rrr

    def QurWhere(self,cont,tbname):
        if bool(cont) == False and  bool(tbname) == False:
            exit()
        use_db   = self.db_conn.get_database(self.db_name)
        use_coll = use_db.get_collection(tbname)
        rrr   = use_coll.find(cont)
        result = []
        for i in rrr:
            result.append(i)
        return result

###############################################
