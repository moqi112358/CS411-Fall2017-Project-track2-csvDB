# -*- coding: utf-8 -*-
"""
Created on Thu Dec 21 22:12:17 2017

@author: lycbel
"""

# -*- coding: utf-8 -*-
from pandas import DataFrame
import numpy as np
import csv
import pandas as pd
import timeit
import  os
from mx.BeeBase.BeeIndex import *
import os.path
from mx.BeeBase.BeeDict import *
from random import randint
zfillenth = 7;
sizeToDivid = 200;
prefixd = 'index/'
prefixr = 'repliction/'
keyS =25;
hashS = 500;
randomToSequetialRatio = 20

#function outside need:
#createIndex(name)
#loadTableOPIN(tableName,conditions)
#loadFinalTable(tableName,conditions,condition)

#the example pattern
conditionTest = [ ['stars',1,'1',2,'5'],['funny',2,'0',2,'16'] ] 
# conditions format [  [colname,bigthan,one,smallerthan,one]... ] n * 5 str array bigerthan is code for condition 1: < 2: <= 0: none 3: equal similar to smallerthan   
def loadTable(tableName,conditions):
    mins = -1;
    index = -1;
    start = 0;
    for i in range(0, len(conditions)):
        if(hasIndex(tableName,conditions[i][0])):
            (temstart,tempMin,temptotal) = loadTableCheck(tableName,conditions[i]);
            if(beT(mins,tempMin)):
                (mins,start) = (tempMin,temstart);
                index = i;
        elif(hasDic(tableName,conditions[i][0])):
            if(conditions[i][1]==3):
                (temstart,tempMin,temptotal) = loadTableCheckDic(tableName,conditions[i]);
                if(beT(mins,tempMin)):
                    (mins,start) = (tempMin,temstart);
                    index = i;
    if(index==-1):#means no index on any column
        data = pd.read_csv(tableName);
        return data;
    #load the real data
    data = loadPandas(tableName,conditions[index][0],start,mins)
    return data;

def loadTableBySeperateJoin(tableName,colName,equalTos):
    if(colName == None):
        data = pd.read_csv(tableName);
        return data;
    else:
        datas =loadPandasJoin2(tableName,colName,equalTos)
        return datas;
def loadTableWithOpimazedInfo(tableName,colName,start,mins):
    if(colName == None):
        data = pd.read_csv(tableName);
        return data;
    else: 
        data = loadPandas(tableName,colName,start,mins)
        return data;
def loadTableOPTINJoin(tableName,colName,equalTos):
        equalTos = np.unique(equalTos);
        number = len (equalTos);
        
        startMinsList = np.empty([len(equalTos),2],dtype='object');
        i = 0;
        total = 0;
        minr = min(number,5)
        for ind in range(0,minr):
            chosed = randint(0, number-1);
            row = equalTos[chosed];
            conditions = [[colName,3,row,0,'0']];
            result = loadTableOPIN(tableName,conditions)
            
            startMinsList[i] = (result[2],result[3]);
            total = total + result[3];
            i = i +1;
        if(minr<=0):
            return (tableName,colName,total,equalTos)
        total = total / minr;
        total = total * number *randomToSequetialRatio;
        return (tableName,colName,total,equalTos)
def loadPandasJoin2(tableName,colName,equalTos):
    title = loadPandasTitle(tableName,colName);
    title = title [0:len(title)-1];
    datas =  DataFrame( columns=title);
    for row in equalTos:
         data = loadTableReal2(title,tableName,colName,row)
         try:
             data = data[:,0:len(title)]
         except Exception as e:
             data = None;
         df = DataFrame(data=data, columns=title);
         datas = datas.append(df);
    return datas;
    
    
    
#if there is optimazed informaiton return (numberofRecord,bestconditon)
# if there is no optimazed information return (tableName,colName,start,mins)
def loadTableOPIN(tableName,conditions):
    mins = -1;
    index = -1;
    start = 0;
    total =0;
    for i in range(0, len(conditions)):
        if(hasIndex(tableName,conditions[i][0])):
            (temstart,tempMin,temptotal) = loadTableCheck(tableName,conditions[i]);
            if(beT(mins,tempMin)):
                (mins,start) = (tempMin,temstart);
                index = i;
                total  = temptotal;
        elif(hasDic(tableName,conditions[i][0])):
            if(conditions[i][1]==3):
                (temstart,tempMin,temptotal) = loadTableCheckDic(tableName,conditions[i]);
                if(beT(mins,tempMin)):
                    (mins,start) = (tempMin,temstart);
                    index = i;
                    total  =temptotal;
    if(index==-1):#means no index on any column
        total = getTotal(tableName);
        return (tableName,None,0,total);
    return (tableName,conditions[index][0],start,mins);   
def loadTableReal2(title,tableName,columnName,equals):
    realName = tableName.split('.')[0];
    rfileName = prefixr+ realName+'/'+ columnName+'.rep';
    key = equals.zfill(zfillenth)+'p';
    if(hasIndex(tableName,columnName)):
            realName = tableName.split('.')[0];
            ifileName = prefixd+ realName+'/'+ columnName+'.index';
            idx = BeeStringIndex(ifileName, keysize=keyS, dupkeys=0, filemode=3) 
            start = getEqual(key,idx)
            idx.flush();
            idx.close();
    elif(hasDic(tableName,columnName)):
            realName = tableName.split('.')[0];
            ifileName = prefixd+ realName+'/'+ columnName+'.dic';
            idx = BeeStringDict(ifileName, keysize=hashS); 
            start = geteQualDic(key,idx);
            idx.flush();
            idx.close();
    arr = None;
    i = 0;
    if(start<0):
        return arr;
    fk = open(rfileName, 'rU');
    fk.seek(start);
    rd = csv.reader(fk,delimiter=',', quotechar='"');
    inde = 0;
    for ind in range(0,len(title)-1):
        if(title[ind]==columnName):
            inde =ind;
    for rowt in rd:
        after = rowt[inde].zfill(zfillenth)+'p';
        if(after!=key):
            break;
        if(i==0):
            mins = int(rowt[len(rowt)-1].split(';')[1]);
            arr = np.empty([mins,len(rowt)],dtype='object');
        try:
            arr[i] = rowt;
        except Exception as s:
            print i;
        del rowt;
        i = i+1;
    fk.close();
    del fk;
    return arr;   
def loadTableReal(tableName,columnName,start,mins):
    realName = tableName.split('.')[0];
    rfileName = prefixr+ realName+'/'+ columnName+'.rep';
    arr = None;
    i = 0;
    fk = open(rfileName, 'rU');
    fk.seek(start);
    rd = csv.reader(fk,delimiter=',', quotechar='"');
    for rowt in rd:
        if(i>=mins):
            break;
        if(i==0):
            arr = np.empty([mins,len(rowt)],dtype='object');
        try:
            arr[i] = rowt;
        except Exception as s:
            print i;
        del rowt;
        i = i+1;
    fk.close();
    del fk;
    return arr;

def loadPandas(tableName,columnName,start,mins):
     title = loadTableReal(tableName,columnName,0,1)[0];
     lenth = len(title)-1;
     title = title[0:lenth]
     if mins > 0:  
         data = loadTableReal(tableName,columnName,start,mins);
         data = data[:,0:lenth]
         df = DataFrame(data=data, columns=title);
         del data;
         return df;
     else:
         df =  DataFrame( columns=title);
         return df;
def loadPandasTitle(tableName,columnName):
    title = loadTableReal(tableName,columnName,0,1)[0];
    
    return title;
def loadPandasWithTitle(tableName,columnName,start,mins,title):
     lenth = len(title);
     if mins > 0:  
         data = loadTableReal(tableName,columnName,start,mins);
         data = data[:,0:lenth]
         df = DataFrame(data=data, columns=title);
         del data;
         return df;
     else:
         df =  DataFrame( columns=title);
         return df;
def loadTableCheckDic(tableName,condition):
    realName = tableName.split('.')[0];
    ifileName = prefixd + realName+'/'+condition[0]+'.dic';
    colName = condition[0];
    start =0;
    key1 = condition[2].zfill(zfillenth)+'p'
    idx = BeeStringDict(ifileName, keysize=hashS); 
    total = idx['^defaultTotal'];   
    start = geteQualDic(key1,idx); 
    if(start ==-1):
        idx.flush()
        idx.close()
        return (0,-1,total);
    (small,equal) = getSE(tableName,colName,start);
    idx.close()
    return (start,equal,total);             
#0 none, 1 bigger 2 >= 3 ==
#reutrn  start  ,number of rows         condition example: ['stars',1,'5',2,'6']
def loadTableCheck(tableName,condition):
    realName = tableName.split('.')[0];
    ifileName = prefixd + realName+'/'+condition[0]+'.index';
    colName = condition[0];
    start =0;
    end = 0;
    key1 = condition[2].zfill(zfillenth)+'p'
    key2 = condition[4].zfill(zfillenth)+'p'
    o1 = condition[1]
    o2 = condition[3]
    idx = BeeStringIndex(ifileName, keysize=keyS, dupkeys=0, filemode=3)
    total = idx['^defaultTotal'];
    if o1 ==3:#only equal
       start = getEqual(key1,idx); 
       if(start ==-1):
           idx.flush()
           idx.close()
           return (0,-1,total);
       (small,equal) = getSE(tableName,colName,start);
       idx.flush()
       idx.close()
       return (start,equal,total);
    else:
       start = getPNofLeft(key1,o1,idx);
       end = getPNofRight(key2,o2,idx);
    if(start == -1 or end ==-1):
        idx.flush()
        idx.close()
        return (0,-1,total); 
    (sm1,e1) =(0,0)
    (sm2,e2) =(0,0)
    
    if(end==-2):
        sm2 = total;
        e2 = 0;
    else:
        (sm2,e2) = getSE(tableName,colName,end);
    if(start!=-2):
        (sm1,e1) = getSE(tableName,colName,start);
    idx.flush()
    idx.close() 
    return (start,e2+sm2-sm1,total)
def getTotal(tableName):
            realName = tableName.split('.')[0];
            ifileName = prefixd + realName+'/'+'total'+'.index';
            idx = BeeStringIndex(ifileName, keysize=keyS, dupkeys=0, filemode=3);
            total = idx['^defaultTotal']; 
            return total;

#(small,equal)
def getSE(fileName,colName,start):
    datat = loadTableReal(fileName,colName,start,1)
    try:
        a =len(datat);
    except Exception as e:
        print e;
        return (0,0)
    data = datat[0];
    lent = len(data);
    rdata = data[lent-1];
    srdata = rdata.split(';');
    small = int(srdata[0]);
    equal = int(srdata[1]);
    return (small,equal)
def geteQualDic(key,idx):
    try:
        value = idx[key];
        return value;
    except Exception as e:
        return -1;
#return position  
def getEqual(key,idx): 
   if(not haskey(idx,key)):
       idx.delete(key,-1)
       return -1;
   value = idx[key];
   return value;

   
       
#mod =0 None mod = 1 not include equal 2 include equal       
#return position  
def getPNofLeft(key,mod,idx):
    if(mod==0):
        up = idx.cursor(FirstKey)
        value = up.value;
        return value;
    needDelet = not haskey(idx,key);
    if( (not needDelet) and (mod==2)):
        up = idx.cursor(key);
    else:
        up = idx.cursor(key);
        if not up.next():
            return -1;
    value = up.value

    if(needDelet):
        idx.delete(key,-1)
    return value; 
def getPNofRight(key,mod,idx):
    if(mod==0):
        #up = idx.cursor(LastKey)
        #value = up.value;
        #return value;
        return -2;
    needDelet = not haskey(idx,key);
    if((not needDelet) and (mod==2)):
        up = idx.cursor(key);
    else:
        up = idx.cursor(key);
        if not up.prev():
            return -1;
    value = up.value
    if(needDelet):
        idx.delete(key,-1)
    return value;
def haskey(idx,key):
    try:
        return 4294967295 != idx[key];
    except Exception as e:
        idx[key]=-1;
        return False;
#bigger  eaual to   
def beT(a,b):
    if(a==-1):
        return True;
    return a >= b;

def hasIndex(tableName,colName):
    realName = tableName.split('.')[0];
    fileName = prefixd + realName + '/' + colName + '.index';
    return os.path.isfile(fileName)
       
def readCSV(name):
    pdd = pd.read_csv(name,index_col=False,keep_default_na=False);
    return pdd;
def storecsv(df,name):
    df.to_csv(name,index=False,index_label=False,escapechar='\r')   
def storecsv2(df,name):
    data = df.values
    title = df.columns;
    with open(name, "a") as myfile:
        for row in data:
            bu = '';
            first = True;
            for sr in row:
                sr = rewriteEl(sr);
                if first:
                    bu = bu +','+ sr ;
                    first = False;
                else:
                    bu = bu +','+ sr ;
            myfile.write(bu+'\n');
def rewriteEl(e):
    e = str.replace(e,'"','""');
    if(('"' in e) or ('\n' in e) or (',' in e)  ):
        e = "\"" + e + '"';
    return e;
#name is the table file name must be a name no folder
#the index will be index/tablename/<columnName>.index for each column
#for column longer than 100 char will not create index
#index format indexforNUmparray,small,equal
def createIndex(name):
    if(not checkIndexEixst(name)):
        realName = name.split('.')[0];
        createDir(prefixd+realName);
        createDir(prefixr+realName);
        datat = readCSV(name);
        title = datat.columns;
        totalName = prefixd + realName+'/'+'total'+'.index';
        idx1 = BeeStringIndex(totalName, keysize=keyS, dupkeys=0, filemode=2)
        idx1['^defaultTotal'] = len(datat.values);
        idx1.flush();
        idx1.close();
        #datat['sizInfo'] = '0000000;0000000';
        i = -1;
        for tele in title:
            datad = datat.copy();
            datad['sizInfo'] = '0000000;0000000';
            i= i + 1;
            ifileName = prefixd + realName+'/'+tele+'.index';
            rfileName = 'repliction/'+ realName+'/'+ tele+'.rep';
            idx = BeeStringIndex(ifileName, keysize=keyS, dupkeys=0, filemode=2)
            datad = datad.sort_values(by=[tele])
            datadt = datad.astype(str)
            
            try:
                createIndexR(idx,tele,i,ifileName,rfileName,realName,datadt,title);
                idx.flush()
                idx.close()
                del datad
            except Exception as Ex:
                 print Ex
                 print('did not create index on ' + tele + " as string longer than 30, trying create dictionary") 
                 idx.flush()
                 idx.close()
                 remove(ifileName); 
                 datadt = datad.astype(str)
                 createDicR(name,tele,i,rfileName,datadt,title);
                 del datad
    else:
        print ('index already exists')
def createIndexR(idx,tele,i,ifileName,rfileName,realName,datat,title):
        equal = 1;
        previous = None;
        sp = None;
        startLenth = 0;
        lenth = 0;
        data = datat.values
        lenOfRow = len(title);
        lenth = getlen(datat.columns);
        startLenth = lenth;
        start = True;
        small = 0;
        end = timeit.default_timer()
        total = len(data);
        tempT = 0;
        tlen = len(title);
        for da in data:
            temp = str(da[i]).zfill(zfillenth);
            if(temp==previous):
                equal = equal + 1;
            else:
                if start :
                    start = False;
                else:
                    small = tempT - equal;
                    idx[previous+'p'] = startLenth;
                    stemp = str(small).zfill(7) + ';' + str(equal).zfill(7);
                    data[small,tlen] = stemp;
                    startLenth = lenth;
                    equal = 1;
                previous = temp;
            lenth = lenth + getlen(da);
            tempT = tempT + 1;
        small = tempT - equal;
        idx[previous+'p'] = startLenth;
        stemp = str(small).zfill(7) + ';' + str(equal).zfill(7);
        data[small][lenOfRow] = stemp;
        idx['^defaultTotal'] = total;
        starts = timeit.default_timer()
        print "index time" + str(starts - end)
        storecsv(datat,rfileName)  
        end = timeit.default_timer()
        print 'store rep file time:' + str(end - starts)   
        del datat;
def createDicR(name,tele,i,rfileName,datat,title):
        realName = name.split('.')[0];
        ifileName = prefixd  + realName + '/' + tele + '.dic';
        idx = BeeStringDict(ifileName, autocommit=1, keysize=hashS);
        try:
            equal = 1;
            previous = '';
            sp = None;
            startLenth = 0;
            lenth = 0;
            datat = datat.sort_values(by=[tele])
            data = datat.values
            lenOfRow = len(title);
            lenth = getlen(datat.columns);
            startLenth = lenth;
            start = True;
            small = 0;
            end = timeit.default_timer()
            total = len(data);
            tempT = 0;
            tlen = len(title);
            for da in data:
                temp = str(da[i]).zfill(zfillenth);
                if(temp==previous):
                    equal = equal + 1;
                else:
                    if start :
                        start = False;
                    else:
                        small = tempT - equal;
                        idx[previous+'p'] = startLenth;
                        stemp = str(small).zfill(7) + ';' + str(equal).zfill(7);
                        data[small,tlen] = stemp;
                        startLenth = lenth;
                        equal = 1;
                    previous = temp;
                lenth = lenth + getlen(da);
                tempT = tempT + 1;
            small = tempT - equal;
            idx[previous+'p'] = startLenth;
            stemp = str(small).zfill(7) + ';' + str(equal).zfill(7);
            data[small][lenOfRow] = stemp;
            idx['^defaultTotal'] = total;
            idx.commit();
            idx.close()
            starts = timeit.default_timer()
            print "index time" + str(starts - end)
            storecsv(datat,rfileName)  
            end = timeit.default_timer()
            print 'store rep file time:' + str(end - starts) 
            del datat;
        except Exception as e:
            print e;
            print 'did not create dictionary on '  +tele;
            idx.close();
            (file1,file2) = getDicPath(name,tele);
            remove(file1)
            remove(file2)
            del datat;
def createDir(name):
    try:
        os.makedirs(name)
    except Exception as e:
        pass
#check is index already 
def checkIndexEixst(name):
    realName = name.split('.')[0];
    for files in os.walk(prefixd):
        for fil in (files[1]):
            if(fil==realName):
                return True;
    return False;   
def remove(filename):
    try:
        os.remove(filename)
    except OSError:
        pass
def getIndex(name):
    idx = BeeStringIndex('index/photos/label.index', keysize=keyS, dupkeys=0, filemode=3)
    temp = idx[name]
    idx.flush()
    idx.close()
    return temp
def testSeek(sek):
    end = timeit.default_timer()
    f = open('repliction/photos/label.rep', 'rU');
    f.seek(sek)
    rd = csv.reader(f,delimiter=',', quotechar='"');
    i =0;
    trow = None;
    try:
        for row in rd:
           if(i==3):
               print row[3].count('\n')
           if i < 6:
               print str(i)+ ": "
               print row;
           else:
               break;
           i = i + 1;
    except Exception as E:
        print E
        print i;       
    start = timeit.default_timer()
    print start - end
    return start - end
#return M
def getSize(name):
    return os.stat(name).st_size/1024/1024
def getlen(arr):
    lent = 0;
    for e in arr:
        e =str(e)
        qns = e.count('"');
        ns = e.count('\n');
        lent = lent + ns
        lent = lent + qns;
        if (',' in e) or (qns!=0) or (ns!=0) or ('\r' in e):
            lent = lent + 2;
        lent = lent + len(e) + 1;
    return lent + 1;

# retunr two files of the dic
def getDicPath(fileName,colName):
    realName = fileName.split('.')[0];
    prefixtemp = prefixd  + realName + '/' + colName + '.dic';
    return (prefixtemp + '.dat' , prefixtemp + '.idx' );
def hasDic(fileName,colName):
    return os.path.isfile(getDicPath(fileName,colName)[0]);  
#v2WhjAB3PIBA8J8VxG3wEg  
def testLTC():
    start = timeit.default_timer();
    condition = ['business_id',3,'v2WhjAB3PIBA8J8VxG3wEg',2,'4'];
    condition = ['user_id',3,'zypGaZmq7QhyV6TZfQrayg',2,'4'];
    data = loadTableCheck('rs.csv',condition);
    end = timeit.default_timer();
    print data;
    print 'time used for LTC ' + str(end - start)
    del data
def testLTCDic():
    start = timeit.default_timer();
    condition = ['stars',3,'1',0,'4'];
    data = loadTableCheckDic('rs.csv',condition);
    end = timeit.default_timer();
    print data;
    print 'time used for LTCDic ' + str(end - start)
    del data
def testLoadPandas():
    data = loadPandas('rs.csv','user_id',2986099, 1);
    print data.values;
    del data

def testDic():
    s = BeeStringDict('index/rs/user_id.dic', keysize=hashS); 
    print s['oU2SSOmsp_A8JYI7Z2JJ5w'];
    s.close();
def testloadTableReal():
    data = loadTableReal('business.csv','stars',0,12081800);
    print data;
    #tableName,conditions[index][0],start,mins
tCondition = [['stars',1,'-1',2,'2'],['useful',2,'0',2,'100']] 
mOCNdition =    [['postal_code',3,'61803',0,'']]
def testloadTable():
    data = loadTable('business.csv',mOCNdition);
    print data;
def testloadTableOPIN():
    data = loadTableOPIN('rs.csv',tCondition);
    print data;
def testseperateLOad(i):
    pdd = pd.read_csv('business.csv',dtype=str);
    start = timeit.default_timer();
    j = 0;
    for row in pdd['business_id']:
        if(j==i):
            break;
        j = j+1;
        d= loadTableOPIN('r.csv',[['business_id',3,row,0,'0']]);
    end = timeit.default_timer();
    print str(i) + ' time:' + str(end - start)
#testloadTableOPIN();
#testloadTable();
#d= loadTable('r.csv',[['business_id',3,'v2WhjAB3PIBA8J8VxG3wEg',0,'0']]);
#testseperateLOad(1000)
#testLTCDic();
#testSeperateLoad();
#testloadTableReal();   
#testLoadPandas()    
#testLTC()
createIndex('rs.csv')
#testDic();
#testSeek(getIndex('foodp'));
#print 'ddd'
#data = tread('rs.csv',500000,0);
#print data
#print (data[1][0]['3'])
#del data;

#(tableName,conditions[index][0],start,mins) result of loadTableOPIN
def testDirectLoadTableRoutin():
    #first detect:
    opresult1 = loadTableOPIN('rs.csv',rsCondition);
    opresult2 = loadTableOPIN('business.csv',businessCondition);
    total1 = opresult1[3];
    total2 =  opresult2[3]; 
    if(total2>total1):
        print opresult1;
        result = loadTableWithOpimazedInfo(opresult1[0],opresult1[1],opresult1[2],opresult1[3]);

    else:
        result = loadTableWithOpimazedInfo(opresult2[0],opresult2[1],opresult2[2],opresult2[3]);
    print result
#testDirectLoadTableRoutin()    
rsCondition = [['stars',2,'3',2,'3'],['user_id',3,'asdasdsad',0,'']] 
businessCondition = [['postal_code',2,'0',0,'']]
def testJoinLoadTableRoutinNew ():
    tableName1 = 'r.csv'
    tableName2='business.csv'
    joinColName = 'business_id'
    #fisrt load all tables' opt information
    opresult1 = loadTableOPIN(tableName1,rsCondition);
    opresult2 = loadTableOPIN(tableName2,businessCondition);
    total1 = opresult1[3];
    total2 =  opresult2[3]; 
    
    
   
    if(total2>total1):
        opresultChose = opresult1;
        opresultLeft = opresult2;
        tableNameChose =tableName2;
        totalByCondition  = total2;
        
    
        result = loadTableWithOpimazedInfo(opresultChose[0],opresultChose[1],opresultChose[2],opresultChose[3]);
        # assumne join on R.businessId == B.businessId
        #need to use   loadTableOPTINJoin(tableName,colName,equalTos)
        #  then  loadTableBySeperateJoin(tableName,colName,startMinsList)
        JoinOptInfo = loadTableOPTINJoin(tableNameChose,joinColName,result[joinColName]); #result is (tableName,colName,total,startMinsList)
        totalBySeperateJoin = JoinOptInfo[2];
        
        if(totalBySeperateJoin>totalByCondition):
            result2 = loadTableWithOpimazedInfo(opresultLeft[0],opresultLeft[1],opresultLeft[2],opresultLeft[3]);
        else:
            result2 =  loadTableBySeperateJoin(JoinOptInfo[0],JoinOptInfo[1],JoinOptInfo[3]);
        print 'done';
        #then merge result and result 2
    if(total2<=total1):
        opresultChose = opresult2;
        opresultLeft = opresult1;
        tableNameChose =tableName1;
        totalByCondition  = total1;
    
    
        result = loadTableWithOpimazedInfo(opresultChose[0],opresultChose[1],opresultChose[2],opresultChose[3]);
        # assumne join on R.businessId == B.businessId
        #need to use   loadTableOPTINJoin(tableName,colName,equalTos)
        #  then  loadTableBySeperateJoin(tableName,colName,startMinsList)
        JoinOptInfo = loadTableOPTINJoin(tableNameChose,joinColName,result[joinColName]); #result is (tableName,colName,total,startMinsList)
        
        totalBySeperateJoin = JoinOptInfo[2];
        
        
        if(totalBySeperateJoin>totalByCondition):
            result2 = loadTableWithOpimazedInfo(opresultLeft[0],opresultLeft[1],opresultLeft[2],opresultLeft[3]);
        else:
            result2 =  loadTableBySeperateJoin(JoinOptInfo[0],JoinOptInfo[1],JoinOptInfo[3]);
        print 'done';
        #then merge result and result 2    
    
def chooseBaseTable2(tableClause1, c1, tableClause2, c2):
    #first detect:
    opresult1 = loadTableOPIN(tableClause1[0], c1);
    opresult2 = loadTableOPIN(tableClause2[0], c2);
    total1 = opresult1[3];
    total2 = opresult2[3];

    if(total2 > total1):
        result = loadTableWithOpimazedInfo(opresult1[0],opresult1[1],opresult1[2],opresult1[3]);
        rsTable = tableClause1[1]
    else:
        result = loadTableWithOpimazedInfo(opresult2[0],opresult2[1],opresult2[2],opresult2[3]);
        rsTable = tableClause2[1]
    return result, rsTable

def chooseBaseTable3(tableClause1, c1, tableClause2, c2, tableClause3, c3):
    #first detect:
    opresult1 = loadTableOPIN(tableClause1[0], c1);
    opresult2 = loadTableOPIN(tableClause2[0], c2);
    opresult3 = loadTableOPIN(tableClause3[0], c3);
    total1 = opresult1[3];
    total2 = opresult2[3];
    total3 = opresult3[3];

    if total1 < total2 and total1 < total3:
        result = loadTableWithOpimazedInfo(opresult1[0],opresult1[1],opresult1[2],opresult1[3]);
        rsTable = tableClause1[1]
    elif total2 < total1 and total2 < total3: 
        result = loadTableWithOpimazedInfo(opresult2[0],opresult2[1],opresult2[2],opresult2[3]);
        rsTable = tableClause2[1]
    else:
        result = loadTableWithOpimazedInfo(opresult3[0],opresult3[1],opresult3[2],opresult3[3]);
        rsTable = tableClause3[1]
    return result, rsTable

def loadJoinTable(base, joinTableClause, joinTableCondition, joinColName, baseColName):
    opresult = loadTableOPIN(joinTableClause[0], joinTableCondition);
    total =  opresult[3]; 
    JoinOptInfo = loadTableOPTINJoin(joinTableClause[0],joinColName,base[baseColName]); #result is (tableName,colName,total,startMinsList)
    
    totalBySeperateJoin = JoinOptInfo[2];
    
    if(totalBySeperateJoin > total):
        result2 = loadTableWithOpimazedInfo(opresult[0],opresult[1],opresult[2],opresult[3]);
    else:
        result2 =  loadTableBySeperateJoin(JoinOptInfo[0],JoinOptInfo[1],JoinOptInfo[3]);
    return result2, joinTableClause[1]
testJoinLoadTableRoutinNew();


# -*- coding: utf-8 -*-

