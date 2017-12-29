import numpy as np
from rewrite import decomposeOr, rewrite, isPushable, getPushupCondition
import pandas as pd
from functools import reduce
import timeit
import sqlparse
import re
from copy import copy
from indexV1 import createIndex, loadTable, chooseBaseTable2, chooseBaseTable3, loadJoinTable
import glob

def mem_usage(pandas_obj):
    if isinstance(pandas_obj,pd.DataFrame):
        usage_b = pandas_obj.memory_usage(deep=True).sum()
    else: # we assume if not a df it's a series
        usage_b = pandas_obj.memory_usage(deep=True)
    usage_mb = usage_b / 1024 ** 2 # convert bytes to megabytes
    return "{:03.2f} MB".format(usage_mb)

def renameCols(table, prefix):
    cols = list(table)
    cols = [prefix+'.'+attr for attr in cols]
    table.columns = [cols]
    return table

class csvDB:
    def __init__(self):
        self.data = []
        self.tables = []
        self.tempData = []
        self.tempTables = []
        self.optimize = False
    ############################################################################
    # load multiple csv files
    def indexData(self, filenames):
        for f in filenames:
            if f:
                createIndex(f)

    ############################################################################
    def join(self, product, joinTable, joinCondition, rename):
        product_ = product
        df2 = joinTable
        onList = []
        attrList = [i.strip() for i in joinCondition.split(" = ")]
        if attrList[0] in product_.columns:
            df2 = df2.rename(columns={attrList[1]: attrList[0]})
            onList.append(attrList[0])
            elimed = attrList[1]
        else:
            df2 = df2.rename(columns={attrList[0]: attrList[1]})
            onList.append(attrList[1])
            elimed = attrList[0]
        product = pd.merge(product, df2, on=onList[0])
        return product, (elimed, onList[0])

    # return the cartesian product of multiple tables
    def cartesian(self, tablesList, rename):
        if len(tablesList) == 2:
            t1 = tablesList[0].split(' ')
            t2 = tablesList[1].split(' ')
            df1 = copy(self.data[self.tables.index(t1[0])])
            df2 = copy(self.data[self.tables.index(t2[0])])
            if rename:
                df1 = renameCols(df1, t1[1])
                df2 = renameCols(df2, t2[1])
            df1['key'] = 1
            df2['key'] = 1
            product = pd.merge(df1, df2, on='key')
            product = product.drop(['key'], axis=1)
            return product
        if len(tablesList) == 3:
            df1 = self.data[self.tables.index(tablesList[0])]
            df2 = self.data[self.tables.index(tablesList[1])]
            df3 = self.data[self.tables.index(tablesList[2])]
            df1['key'] = 1
            df2['key'] = 1
            df3['key'] = 1
            product = pd.merge(df1, df2, df3, on='key')
            product = product.drop(['key'], axis=1)
            return product

    ############################################################################
    # query optimization methods
    def selectionPushUp(self, product, tName, condition):
        product_ = product
        conditionList = re.split(" and | AND ", condition)
        for c in conditionList:
            pushable, c_ = isPushable(c)
            if pushable and c_.split('.')[0] == tName:
                newc= rewrite(c_)
                product_ = product_[eval(newc)]
        return product_

    ############################################################################
    # execute query here
    def executeSearch(self, condition, tables, rename):
        distinctFlag = False
        tablesList = tables.split(',')
        tablesList = [i.strip() for i in tablesList]
        
        joinCondition = ''
        if "DISTINCT" in condition:
            condition = condition.replace('DISTINCT', '')
            distinctFlag = True
        if ' ON ' in condition:
            joinCondition = condition.split(' ON ')[1].strip()[1:-1]
            condition = condition.split(' ON ')[0]
            
        if len(tablesList) == 1:
            tableClause = tables.split(' ')
            # print(tableClause[1])
            # print(condition)
            pushupCondition = getPushupCondition(tableClause[1], condition)
            # print(pushupCondition)
            product = loadTable(tableClause[0], pushupCondition)
            product = product.apply(pd.to_numeric, errors='ignore')
            # print(product)
            if rename:
                product = renameCols(product, tableClause[1])
            new = rewrite(condition)
            # print(new)
            rs = product[eval(new)]
        elif len(tablesList) == 2:
            #choose the base table
            tableClause1 = tablesList[0].split(' ')
            tableClause2 = tablesList[1].split(' ')
            pushupCondition1 = getPushupCondition(tableClause1[1], condition)
            pushupCondition2 = getPushupCondition(tableClause2[1], condition)
            
            product, tName = chooseBaseTable2(tableClause1, pushupCondition1, tableClause2, pushupCondition2)
            product = product.apply(pd.to_numeric, errors='ignore')
            if rename:
                product = renameCols(product, tName)
            product = self.selectionPushUp(product, tName, condition)
            joinCols = joinCondition.split(" = ")
            if tName == tableClause1[1]:
                joinTableClause = tableClause2
                joinTableCondition = pushupCondition2
                if joinCols[0].split(".")[0] == tName:
                    baseColName = joinCols[0]
                    joinColName = joinCols[1].split(".")[1]
                else:
                    baseColName = joinCols[1]
                    joinColName = joinCols[0].split(".")[1]
            else:
                joinTableClause = tableClause1
                joinTableCondition = pushupCondition1
                if joinCols[0].split(".")[0] == tName:
                    baseColName = joinCols[0]
                    joinColName = joinCols[1].split(".")[1]
                else:
                    baseColName = joinCols[1]
                    joinColName = joinCols[0].split(".")[1]

            joinTable, joinTName  = loadJoinTable(product, joinTableClause, joinTableCondition, joinColName, baseColName)
            joinTable = joinTable.apply(pd.to_numeric, errors='ignore')
            if rename:
                joinTable = renameCols(joinTable, joinTableClause[1])
            joinTable = self.selectionPushUp(joinTable, joinTName, condition)
            print(joinTable.shape)
            print(product.shape)
            product, log = self.join(product, joinTable, joinCondition, rename)
            
            new = rewrite(condition)
            rs = product[eval(new)]
        elif len(tablesList) == 3:
            #choose the base table
            tableClause1 = tablesList[0].split(' ')
            tableClause2 = tablesList[1].split(' ')
            tableClause3 = tablesList[2].split(' ')
            tableClauseList = [tableClause1, tableClause2, tableClause3]
            pushupCondition1 = getPushupCondition(tableClause1[1], condition)
            pushupCondition2 = getPushupCondition(tableClause2[1], condition)
            pushupCondition3 = getPushupCondition(tableClause3[1], condition)
            pushupConditionList = [pushupCondition1, pushupCondition2, pushupCondition3]
            
            product, tName = chooseBaseTable3(tableClause1, pushupCondition1, tableClause2, pushupCondition2, tableClause3, pushupCondition3)
            product = product.apply(pd.to_numeric, errors='ignore')
            if rename:
                product = renameCols(product, tName)
            product = self.selectionPushUp(product, tName, condition)
            joinConditions = joinCondition.split(",")
            for jc in joinConditions:
                if tName+'.' in jc:
                    firstJoinCondition = jc
            if firstJoinCondition == joinConditions[0]:
                secondJoinCondition = joinConditions[1]
            else:
                secondJoinCondition = joinConditions[0]
            
            #process first join condition
            joinCols = [i.strip() for i in firstJoinCondition.split(" = ")]
            if tName+'.' in joinCols[0]:
                joinTableName = joinCols[1].split('.')[0]
                for t in tableClauseList:
                    if joinTableName in t:
                        joinTableClause = t
                joinTableCondition = pushupConditionList[tableClauseList.index(joinTableClause)]
                if tName+'.' in joinCols[0]:
                    baseColName = joinCols[0]
                    joinColName = joinCols[1].split(".")[1]
                else:
                    baseColName = joinCols[1]
                    joinColName = joinCols[0].split(".")[1]
            else:
                joinTableName = joinCols[0].split('.')[0]
                for t in tableClauseList:
                    if joinTableName in t:
                        joinTableClause = t
                joinTableCondition = pushupConditionList[tableClauseList.index(joinTableClause)]
                if tName+'.' in joinCols[0]:
                    baseColName = joinCols[0]
                    joinColName = joinCols[1].split(".")[1]
                else:
                    baseColName = joinCols[1]
                    joinColName = joinCols[0].split(".")[1]
            joinTable, joinTName  = loadJoinTable(product, joinTableClause, joinTableCondition, joinColName, baseColName)
            joinTable = joinTable.apply(pd.to_numeric, errors='ignore')
            if rename:
                joinTable = renameCols(joinTable, joinTableClause[1])
            joinTable = self.selectionPushUp(joinTable, joinTName, condition)
            product, log = self.join(product, joinTable, firstJoinCondition, rename)
            print(product.shape)
            
            #process second join condition
            if log[0] in secondJoinCondition:
                secondJoinCondition = secondJoinCondition.replace(log[0], log[1])
            print(secondJoinCondition)
            joinCols = [i.strip() for i in secondJoinCondition.split(" = ")]
            if joinCols[0] in product.columns:
                joinTableName = joinCols[1].split('.')[0]
            else:
                joinTableName = joinCols[0].split('.')[0]
            for t in tableClauseList:
                if joinTableName in t:
                    joinTableClause = t
            joinTableCondition = pushupConditionList[tableClauseList.index(joinTableClause)]
            if tName+'.' in joinCols[0]:
                baseColName = joinCols[0]
                joinColName = joinCols[1].split(".")[1]
            else:
                baseColName = joinCols[1]
                joinColName = joinCols[0].split(".")[1]
            print(joinTableClause)
            print(joinTableCondition)
            print(joinColName)
            print(baseColName)
            joinTable, joinTName  = loadJoinTable(product, joinTableClause, joinTableCondition, joinColName, baseColName)
            joinTable = joinTable.apply(pd.to_numeric, errors='ignore')
            if rename:
                joinTable = renameCols(joinTable, joinTableClause[1])
            joinTable = self.selectionPushUp(joinTable, joinTName, condition)
            product, onAttr = self.join(product, joinTable, secondJoinCondition, rename)
            
            new = rewrite(condition)
            rs = product[eval(new)]
        return rs, distinctFlag

    # check the existance of all tables
    def CheckCSV(self, tables, orFlag):
        tablesList = tables.split(',')
        renameFlag = False
        csvList = glob.glob(('*.csv'))
        for t in tablesList:
            if t:
                t = t.strip()
                if len(t.split(' ')) == 2:
                    renameFlag = True
                if t.split(' ')[0] not in csvList:
                    print("csv file "+ t +" not found.")
                    return False, renameFlag
        return True, renameFlag

    # main method to process sql
    def executeSQL(self, sql):
        if sql == "show tables":
            self.showTables()
        elif "index " in sql:
            source = sql.split(" ")[1:]
            self.indexData(source)
        else:
            #try:
                sqlList = decomposeOr(sql)
                rsList = []
                orFlag = len(sqlList)> 1
                start = timeit.default_timer()
                for sql_ in sqlList:
                    # get all the clauses from the sql statement
                    stmt = sqlparse.parse(sql_)[0]
                    tables = str(stmt.tokens[6])
                    selection = str(stmt.tokens[-1])

                    #check the select statement
                    if not str(stmt.tokens[2]) == "*":
                        attributes = str(stmt.tokens[2]).split(',')
                        attributes = [i.strip() for i in attributes]
                    else:
                        attributes = None

                    checkTables, rename = self.CheckCSV(tables, orFlag)
                    if checkTables:
                        # if no where clause, print out the whole table/s
                        if selection == tables:
                            tablesList = tables.split(',')
                            if len(tablesList) == 1:
                                idx = self.tables.index(tables)
                                print(self.data[idx])
                            else:
                                tablesList = [i.strip() for i in tablesList]
                                print(self.cartesian(tablesList))
                        else:
                            # get rid of "where" clause
                            condition = selection[6:]
                            rs, dFlag= self.executeSearch(condition, tables, rename)
                            # projection
                            if attributes:
                                if dFlag:
                                    rsList.append(rs[attributes].drop_duplicates())
                                else:
                                    rsList.append(rs[attributes])
                            else:
                                rsList.append(rs)
                finalRs = reduce((lambda df, df2: df.append(df2)), rsList)
                print(finalRs)
                end = timeit.default_timer()
                print("Runtime: " + str(end - start)[:6] + "s")
                self.tables = []
                self.data = []
            #except Exception:
            #    print("Invalid sql statement")

    # show all the tables loaded
    def showTables(self):
        print(self.tables)

# testing
if __name__ == "__main__" :
    db = csvDB()
    #
    # db.loadData("movies.csv oscars.csv")
    # sql1 = "SELECT m.movie_title FROM movies.csv m WHERE m.title_year=1999"
    # sql2 = "SELECT movie_title, imdb_score FROM movies.csv WHERE movie_title like '%Harry Potter%'"
    # sql3 = "SELECT m1.movie_title, m1.imdb_score FROM movies.csv m1 WHERE m1.movie_title like '%Harry Potter%' AND m1.title_year = 2001"
    # sql4 = "SELECT M.title_year, M.movie_title, A.Award, M.imdb_score FROM movies.csv M, oscars.csv A WHERE M.imdb_score < 7 ON (M.movie_title = A.Film)"
    # sql5 = "SELECT M.imdb_score FROM movies.csv M, oscars.csv A WHERE M.imdb_score < 7 ON (M.movie_title = A.Film)"
    # sql6 = "SELECT M.imdb_score, A.Winner FROM movies.csv M, oscars.csv A WHERE M.imdb_score = (3.1 + A.Winner)*2 AND (M.language like 'S%' OR A.Winner = 1) ON (M.movie_title = A.Film)"
    # sql7 = "SELECT M1.imdb_score, M2.imdb_score FROM movies.csv M1, movies.csv M2 WHERE M1.imdb_score > M2.imdb_score and M1.language like 'U%' ON (M1.movie_title = M2.movie_title, M1.title_year = M2.title_year)"
    # sql8 = "SELECT  M1.director_name, M1.title_year, M1.movie_title, M2.title_year, M2.movie_title, M3.title_year, M3.movie_title FROM movies.csv M1, movies.csv M2, movies.csv M3 WHERE M1.movie_title <> M2.movie_title AND M2.movie_title <> M3.movie_title AND M1.movie_title <> M3.movie_title AND M1.title_year < M2.title_year-10 AND M2.title_year < M3.title_year-10 ON (M1.director_name = M2.director_name, M1.director_name = M3.director_name)"
    #
    # db.executeSQL(sql7)
    # columns:
    # review: ['funny' 'user_id' 'review_id' 'text' 'business_id' 'stars' 'date' 'useful' 'cool']
    # business: ['address' 'attributes_AcceptsInsurance' 'attributes_AgesAllowed'
    #  'attributes_Alcohol' 'attributes_Ambience' 'attributes_BYOB'
    # 'attributes_BYOBCorkage' 'attributes_BestNights' 'attributes_BikeParking'
    # 'attributes_BusinessAcceptsBitcoin'
    # 'attributes_BusinessAcceptsCreditCards' 'attributes_BusinessParking'
    # 'attributes_ByAppointmentOnly' 'attributes_Caters' 'attributes_CoatCheck'
    # 'attributes_Corkage' 'attributes_DietaryRestrictions'
    # 'attributes_DogsAllowed' 'attributes_DriveThru'
    # 'attributes_GoodForDancing' 'attributes_GoodForKids'
    # 'attributes_GoodForMeal' 'attributes_HairSpecializesIn'
    # 'attributes_HappyHour' 'attributes_HasTV' 'attributes_Music'
    # 'attributes_NoiseLevel' 'attributes_Open24Hours'
    # 'attributes_OutdoorSeating' 'attributes_RestaurantsAttire'
    # 'attributes_RestaurantsCounterService' 'attributes_RestaurantsDelivery'
    # 'attributes_RestaurantsGoodForGroups' 'attributes_RestaurantsPriceRange2'
    # 'attributes_RestaurantsReservations' 'attributes_RestaurantsTableService'
    # 'attributes_RestaurantsTakeOut' 'attributes_Smoking'
    # 'attributes_WheelchairAccessible' 'attributes_WiFi' 'business_id'
    # 'categories' 'city' 'hours_Friday' 'hours_Monday' 'hours_Saturday'
    # 'hours_Sunday' 'hours_Thursday' 'hours_Tuesday' 'hours_Wednesday'
    # 'is_open' 'latitude' 'longitude' 'name' 'neighborhood' 'postal_code'
    # 'review_count' 'stars' 'state']
    # photos: ['business_id' 'caption' 'label' 'photo_id']

    # test
    #db.convertData("review-1m.csv business.csv photos.csv")
    sample1 = "SELECT R.review_id, R.stars, R.useful FROM r.csv R WHERE R.stars >= 4 AND R.useful > 20"
    sample2 = "SELECT B.name, B.postal_code, R.review_id, R.stars, R.useful FROM business.csv B, r.csv R WHERE B.city = 'Champaign' AND B.state = 'IL' ON (B.business_id = R.business_id)"
    sample3 = "SELECT B.name, B.city, B.state, R.stars, P.label FROM business.csv B, r.csv R, photos.csv P WHERE B.city = 'Champaign' AND B.state = 'IL' AND R.stars = 5 AND P.label = 'inside' ON (B.business_id = R.business_id, B.business_id = P.business_id) "
    a1 = "SELECT R.review_id, R.funny, R.useful FROM r.csv R WHERE R.funny >= 20 AND R.useful > 30"
    a2 = "SELECT B.name, B.city, B.state FROM business.csv B WHERE B.city = 'Champaign' AND B.state = 'IL'"
    b1 = "SELECT B.business_id, B.name, B.postal_code, R.stars, R.useful FROM business.csv B, r.csv R WHERE B.name = 'Sushi Ichiban' AND B.postal_code = '61820' ON (B.business_id = R.business_id)"
    b2 = "SELECT R1.user_id, R2.user_id, R1.stars, R2.stars FROM r.csv R1, r.csv R2 WHERE R1.stars = 5 AND R2.stars = 1 AND R1.useful > 50 AND R2.useful > 50 ON (R1.business_id = R2.business_id)"
    c3 = "SELECT B.name, R1.user_id, R2.user_id FROM business.csv B, r.csv R1, r.csv R2 WHERE R1.stars = 5 AND R2.stars = 1 AND R1.useful > 50 AND R2.useful > 50 ON (B.business_id = R1.business_id, B.business_id = R2.business_id) "
    c4 = "SELECT B.name, R1.user_id, R2.user_id FROM business.csv B, r.csv R1, r.csv R2 WHERE R1.stars = 5 AND R2.stars = 1 AND B.city = 'Champaign' ON (B.business_id = R1.business_id, B.business_id = R2.business_id) DISTINCT"
    testor1 = "SELECT B.business_id, B.name, B.postal_code, R.stars, R.useful FROM business.csv B, review-1m.csv R WHERE (B.postal_code = '44114' OR B.postal_code = '61820') ON (B.business_id = R.business_id)"
    testor2 = "SELECT B.business_id, B.name, B.postal_code, R.stars, R.useful FROM business.csv B, review-1m.csv R WHERE (B.postal_code = '44114' OR R.stars = 5) ON (B.business_id = R.business_id)"
    testor3 = "SELECT B.business_id, B.name, B.postal_code, R.stars, R.useful FROM business.csv B, review-1m.csv R WHERE R.useful > 50 AND (B.postal_code = '44114' OR R.stars = 5) ON (B.business_id = R.business_id)"
    sql4 = "SELECT B.name, B.postal_code, R.review_id, R.stars, R.useful FROM business.csv B, r.csv R, photos.csv P WHERE P.label = 'outside' AND R.useful > 20 ON (B.business_id = R.business_id, B.business_id = P.business_id)"
    tests = [sample2]
    # tests = [testor1, testor2, testor3]
    for s in tests:
        db.executeSQL(s)
    #db.executeSQL(sql4)
