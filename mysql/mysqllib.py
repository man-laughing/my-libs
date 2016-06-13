#!/usr/bin/python
###############################################
#  Description: MySQL  Python interface.      #
#  Author:      laughing                      #
#  Mail:        305835227@qq.com              #
#  Require:     Python env and  MySQLdb       #
###############################################

import MySQLdb

class mysqlpy():
    def __init__(self,db_host,db_port,db_user,db_pass,db_name):
        if not bool(db_host) or not bool(db_port) or not bool(db_user) or not bool(db_pass):
            return None
        self.__db_host = db_host
        self.__db_port = db_port
        self.__db_user = db_user
        self.__db_pass = db_pass
        self.__db_name = db_name
        self.__db_conn = MySQLdb.connect(host=self.__db_host,port=self.__db_port,user=self.__db_user,passwd=self.__db_pass,db=self.__db_name)
        self.__db_cur  = self.__db_conn.cursor(MySQLdb.cursors.DictCursor)

#  desc: Insert one record in a table.
#  para: <Dictionary> <String>
#  exam:  dic = { 
#                 'Account'  : 'zhanglei',
#                 'Password' : 'zhanglei'
#                 }
#        tb = "users"

    def addOne(self,dic,tb):
        if not bool(tb) or not bool(dic):
            return None
        if len(dic) == 1:
            aa = dic.keys()[0]
            bb = dic.values()[0]
            sql = "INSERT INTO %s (%s) values ('%s')" %(tb,dic.keys()[0],dic.values()[0])
            qur_data = self.__db_cur.execute(sql)
            self.__db_conn.commit()
            self.__db_cur.close()
            self.__db_conn.close()
            return qur_data
        else:
            aa = ','.join(dic.keys())
            bb = tuple(dic.values())
            sql = "INSERT INTO %s (%s) values %s" %(tb,aa,bb)
            qur_data = self.__db_cur.execute(sql)
            self.__db_conn.commit()
            self.__db_cur.close()
            self.__db_conn.close()
            return qur_data

#  desc: Delete one record in a table.
#  para: <Dictionary> <String>
#  exam:  dicc = { 'Account':'zhanglei'  }
#         tb = "users"

    def delOne(self,dicc,tb):
        if not bool(tb) or not bool(dicc):
            return None
        one = dicc.keys()[0]
        two = "'%s'" %dicc.values()[0]
        sql = "DELETE  FROM %s WHERE %s=%s " %(tb,one,two)
        qur_data = self.__db_cur.execute(sql)
        self.__db_conn.commit()
        self.__db_cur.close()
        self.__db_conn.close()
        return qur_data

#  desc: Update one record in a table.
#  para: <Dictionary> <Dictionary> <String>
#  exam: dic_set = { 'Password' : 999  }
#        dic_whe = { 'Account'  : 'zhanglei'  }
#        tb      = "users"

    def uptOne(self,dic_set,dic_whe,tb):
        if not bool(dic_set) or not bool(dic_whe) or not bool(tb):
            return None
        set_k = dic_set.keys()[0]
        set_v = "'%s'" %dic_set.values()[0]
        whe_k = dic_whe.keys()[0]
        whe_v = "'%s'" %dic_whe.values()[0]
        sql = "UPDATE %s SET %s=%s WHERE %s=%s " %(tb,set_k,set_v,whe_k,whe_v)
        qur_data = self.__db_cur.execute(sql)
        self.__db_conn.commit()
        self.__db_cur.close()
        self.__db_conn.close()
        return qur_data

#  desc: Query one record in a table.
#  para: <String> <String>
#  exam:  cols = "Account,Password"
#         tb   = "users"

    def qurOne(self,cols,tb):
        if not bool(cols) or not bool(tb):
            return None
        sql = "SELECT %s FROM %s LIMIT 1" %(cols,tb)
        self.__db_cur.execute(sql)
        qur_data = self.__db_cur.fetchall()
        self.__db_cur.close()
        self.__db_conn.close()
        return qur_data

#  desc: Query many record in a table.
#  para: <String>
#  exam: tb = "users"

    def qurAll(self,tb):
        if not bool(tb):
            return None
        sql = "SELECT * FROM %s" %tb
        self.__db_cur.execute(sql)
        qur_data = self.__db_cur.fetchall()
        self.__db_cur.close()
        self.__db_conn.close()
        return qur_data

#  desc: Query record  by 'where' statement  in a table.
#  para: <String> <Dictionary> <String>
#  exam:  dic_cols = "Account,Password"
#         dic_whe  = { 'Account':'admin'}
#         tb = "users"

    def qurWhere(self,dic_cols,dic_whe,tb):
        if not bool(dic_cols) or not bool(dic_whe) or not bool(tb):
            return None
        sql = "SELECT %s FROM %s WHERE %s='%s'" %(dic_cols,tb,dic_whe.keys()[0],dic_whe.values()[0])
        self.__db_cur.execute(sql)
        qur_data = self.__db_cur.fetchall()
        self.__db_cur.close()
        self.__db_conn.close()
        return qur_data

#######################################################################################
