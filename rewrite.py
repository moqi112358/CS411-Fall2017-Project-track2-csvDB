import re
from copy import copy

def rewrite(condition):
    smap = ["<", ">", "(", ")", " ", "+", "-", "*", "/", "=", "!"];
    smap2 = ["and", "or", "not"];
    condition = condition + " ";
    condition = condition.replace("=", "==");
    condition = condition.replace("<>", " != ");
    condition = condition.replace("like\"", "like ");
    condition = condition.replace("like'", "like ");
    percentAppend = True;
    likeAppend = True;
    result = "(";
    name = "";
    like = False;
    colin = 0;
    dex = 0;
    lent = len(condition)
    before = "";
    haveNot = False;

    for i in range(0, lent):
        likeAppend = True;
        a = condition[i];
        if ((a == "'" or a == "\"") and condition[i - 1] != "\\"):
            likeAppend = False;
            colin = colin + 1;
        if (a in smap) and not like and (colin == 0 or colin == 3):
            if (a != " "):
                haveNot = False;
            if (name != ""):
                if (name.lower() == "like"):
                    like = True;
                    if (haveNot):
                        lenresult = len(result) - 1;
                        empty = True;

                        while (empty):
                            lenresult = len(result) - 1;
                            ch = result[lenresult]
                            result = result[:-1];
                            if (ch != " "):
                                for atmp in before:
                                    result = result[:-1];
                                result = result + " ~ " + before;
                                haveNot = False;
                                break;
                if (not like):
                    haveNot = False
                    isNumber = True
                    try:
                        float(name)
                    except:
                        isNumber = False
                    if (name.lower() in smap2):
                        if (name.lower() == "and"):
                            result = result + ") & ("
                        if (name.lower() == "or"):
                            result = result + ") | ("
                        if (name.lower() == "not"):
                            haveNot = True;
                            result = result + "~"
                    elif (not isNumber and (colin != 3)):
                        before = " product['" + name + "']";
                        result = result + "  product['" + name + "']";
                    else:  # if digit or string
                        name = name.replace("==", "=");
                        result = result + name;
            result = result + a;
            name = "";  # reset name
        else:
            if (like):
                if (colin == 1 and not likeAppend and condition[i + 1] == "%"):
                    percentAppend = False;
                    likeAppend = False;
                    dex = dex + 1;
                    continue
                if (colin == 2 and condition[i - 1] == "%"):
                    likeAppend = False;
                    dex = dex + 2;
                    name = name[:-1];
                if (colin == 2):
                    name = name.replace("==", "=");
                    if (dex == 1):
                        result = result + ".str.endswith('" + name + "')==True";
                    if (dex == 2):
                        result = result + ".str.startswith('" + name + "')==True";
                    if (dex == 3):
                        result = result + ".str.contains('" + name + "')==True";
                    if (dex == 0):
                        result = result + " == '" + name + "' ";
                    name = "";
                    dex = 0;
                    like = False;

                if (likeAppend and percentAppend):
                    percentAppend = True;
                    name = name + a;
                    likeAppend = False;
                percentAppend = True;
            else:
                name = name + a;
        i = i + 1;
        if (colin == 3):
            colin = 0;
        if (colin == 2):
            colin = colin + 1;
    result = result + ")"
    result = result.replace(">==", ">=")
    result = result.replace("<==", "<=")
    result = re.sub("\(~|\( ~", "~(", result)
    return result;

def isfloat(value):
  try:
    float(value)
    return True
  except ValueError:
    return False

def isPushable(c):
    c_ = c
    if c[0] == '(':
        c_ = c[1:]
    if c[-1] == ')':
        c_ = c[:-1]
    if 'like' in c or 'LIKE' in c:
        return True, c_
    else:
        attrList = [i.strip() for i in re.split("=|>|<|<>|>=|<=", c_)]
        if attrList[-1][-1]=="'":
            return True, c_
        numAttr = 0
        for attr in attrList:
            incFlag = False
            # check if the attr has been renamed or is an expression
            for symbol in ['.','+','-','*','/']:
                if symbol in attr:
                    incFlag = True
                    break
            # check if attr is an un-renamed attr
            if not isfloat(attr):
                incFlag = True
            if incFlag:
                numAttr += 1
        if numAttr < 2:
            return True, c_

        return False, 0

def findOrClause(sql):
    return 0

def decomposeOr(sql):
    if not " OR " in sql:
        return [sql]
    else:
        sqlList = sql.split(" ")
        condition1 = sqlList[sqlList.index("OR")-1]
        condition2 = sqlList[sqlList.index("OR")+1]

        sqlList = ['AND' if x=='OR' else x for x in sqlList]
        sqlListC = copy(sqlList)
        sqlList1 = ["NOT "+condition2 if x == condition2 else x for x in sqlListC]
        for i in range(100):
            if condition1[0] == '(':
                break
            else:
                condition1 = sqlList[sqlList.index(condition1)-1]
        sqlList2 = [condition1.replace('(','( NOT ') if x == condition1 else x for x in sqlListC]

        subSql1 = sql.replace('OR', 'AND')
        subSql2 = ' '.join(sqlList1)
        subSql3 = ' '.join(sqlList2)
        return [subSql1, subSql2, subSql3]

def getPushupCondition(tableName, condition):
    conditionList =[i.strip() for i in condition.split(" AND ")]
    operatorList = ['>=','>','<=','<','=']
    codeList = [2, 1, 2, 1, 3]
    rsList = []
    for c in conditionList:
        if isPushable(c) and not 'LIKE' in c and not '<>' in c:
            for operator in operatorList:
                if operator in c:
                    [attr1, attr2] = [i.strip() for i in c.split(operator)]
                    if attr1.split('.')[0] == tableName:
                        if operator != '<' and operator != '<=':
                            if attr2[-1] == "'":
                                attr2 = attr2[1:-1]
                            rs = [attr1.split('.')[1], codeList[operatorList.index(operator)], str(attr2),0,'']
                        else:
                            if attr2[-1] == "'":
                                attr2 = attr2[1:-1]
                            rs = [attr1.split('.')[1], 0,'',codeList[operatorList.index(operator)], str(attr2)]
                        rsList.append(rs)
                        break
    return rsList
                
        

if __name__ == "__main__" :
    # print(isPushable("m.a = 1"))
    # print(rewrite( ""));
    # sql = "NOT B.name = 'Sushi Ichiban' AND B.postal_code = '61820'"
    # c = "B.name = 4 )"
    # tf, rs = isPushable(c)
    # print(rs)
    # print(rewrite(sql))
    #t = "SELECT B.business_id, B.name, B.postal_code, R.stars, R.useful FROM business.csv B, review-1m.csv R WHERE (B.postal_code = '44114' OR B.postal_code = '61820') ON (B.business_id = R.business_id)"
    #l = decomposeOr(t)
    #print("")
    #print("=======")
    #for i in l:
    #    print(i)
    b2 = "SELECT R1.user_id, R2.user_id, R1.stars, R2.stars FROM review-1m.csv R1, review-1m.csv R2 WHERE R1.stars = 5 AND R2.stars = 1 AND R1.useful > 50 AND R2.useful > 50 ON (R1.business_id = R2.business_id)"
    condition = "R.stars >= 4 AND R.useful > 20"
    print(getPushupCondition("R", condition))