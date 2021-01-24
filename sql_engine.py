import sys

METADATA_FILE = 'metadata.txt'
db = dict()
reqd_table = []
reqd_cols = []
selected_cols = []
reqd_rows = set()
col_attr = dict()
col_vals = dict()
colToTable = dict()
hasDistinct = False
aggr = ''
aggrs = {"sum", "average", "max", "min", "count", "avg"}

def read_metadata():
    newTable = False
    curTable = ""

    with open(METADATA_FILE,"r") as f:
        for line in f:
            line = line.strip()
            if(len(line) == 0):
                continue
            if(line == "<begin_table>"):
                newTable = True
            elif(newTable == True):
                db[line] = dict()
                curTable = line
                newTable = False
            elif(line != "<end_table>"):
                db[curTable][line] = list()
                colToTable[line] = curTable

def read_csv():
    for table in db:
        with open(table+".csv", "r") as f:
           for line in f:
               vals = line.split(",")
               i = 0
               for col in db[table]:
                   db[table][col].append(int(vals[i].replace('\"', '').strip()))
                   i += 1

def process_cols(cols):
    colnames = []

    for col in cols:
        if(col.lower() == "distinct"):
            global hasDistinct
            hasDistinct = True
            continue
        parts = col.replace('(', ' ').replace(')', ' ').split()
        col_attr[parts[-1]] = "none"
        if(parts[0].lower() in aggrs):
            col_attr[parts[-1]] = parts[0]
            global aggr
            aggr = parts[0].lower()
        colnames.append(parts[-1])

    return colnames

def setTable(tableList):
    setup_table = []

    for i in range(len(tableList)):
        table = tableList[i]
        if table in db: 
            reqd_cols.extend(list(db[table].keys()))
            rowCount = len(db[table][next(iter(db[table]))])
            temp_table = []
            for j in range(rowCount):
                rowvals = []
                for col in db[table]:
                    rowvals.append(db[table][col][j])
                temp_table.append(rowvals)
            setup_table.append(temp_table)
        else:
            print("Error: Table not found")
            sys.exit(1)
    
    itr = 0
    import itertools
    for element in itertools.product(*setup_table):
        reqd_table.append(sum(list((element)), []))
        reqd_rows.add(itr)
        itr += 1
      
def applyWhere(where):
    
    isAnd = False
    if 'and' in where.lower():
        isAnd = True
    parts = where.replace('and','or').replace('AND', 'or').replace('OR', 'or').split('or')
    
    condOutput = []

    for cond in parts:
        cond = cond.strip()
        col = ""
        val = 0
        op = ""
        for i in range(len(cond)):
            if(cond[i] == '='):
                col = cond[:i]
                val = int(cond[i+1:])
                op = cond[i]
                break
            if(cond[i] == '<'):
                if(cond[i+1] == '='):
                    col = cond[:i]
                    val = int(cond[i+2:])
                    op = "<="
                    break
                col = cond[:i]
                val = int(cond[i+1:])
                op = "<"
                break
            if(cond[i] == '>'):
                if(cond[i+1] == '='):
                    col = cond[:i]
                    val = int(cond[i+2:])
                    op = ">="
                    break
                col = cond[:i]
                val = int(cond[i+1:])
                op = ">"
                break
        
        colIndex = 0
        try:
            colIndex = reqd_cols.index(col.strip())
        except:
            print("Error: Column not found in WHERE Clause")
            sys.exit()

        rowsToDel = []

        for row in range(len(reqd_table)):
            for col in range(len(reqd_table[row])):
                if(col == colIndex):
                    delRow = False
                    if((op == "=") and (reqd_table[row][col] != val)):
                        delRow = True
                    if((op == ">") and (reqd_table[row][col] <= val)):
                        delRow = True
                    if((op == "<") and (reqd_table[row][col] >= val)):
                        delRow = True
                    if((op == ">=") and (reqd_table[row][col] < val)):
                        delRow = True
                    if((op == "<=") and (reqd_table[row][col] > val)):
                        delRow = True
                    if(delRow):
                        rowsToDel.append(row)
        condOutput.append(rowsToDel)
    
    if(isAnd):
        for i in condOutput:
            for j in i:
                if j in reqd_rows:
                    reqd_rows.remove(j)
    else:
        toDel = set(condOutput[0])
        for s in condOutput[1:]:
            toDel.intersection_update(s)
        for i in toDel:
            if i in reqd_rows:
                reqd_rows.remove(i)
 
def applyGroup(col_list,group_col,orderBy):
    aggr_cols = dict()
    group_col = group_col.split(',')[0].strip()
    order = []

    if(group_col not in reqd_cols):
        print("Group by column not found")
        exit()

    header = colToTable[group_col].lower() + '.' + group_col.lower() + ','
    for i in col_list:
        cur = i.replace('(',' ').replace(')', '').split()
        if cur[0].lower() in aggrs:
            aggr_cols[cur[1]] = cur[0]
            order.append(cur[1])
        else:
            order.append(cur[0])
    
    groupDict = dict()
    resultDict = dict()
    for i in range(len(reqd_table)):
        if i in reqd_rows:
            groupDict[reqd_table[i][reqd_cols.index(group_col)]] = []
    for i in range(len(reqd_table)):
        if i in reqd_rows:
            groupDict[reqd_table[i][reqd_cols.index(group_col)]].append(reqd_table[i])

    for i in groupDict:
        tmpList = [[] for i in range(len(reqd_cols))]    
        for j in range(len(groupDict[i])):
            for k in range(len(groupDict[i][j])):
                tmpList[k].append(groupDict[i][j][k])
        resultDict[i] = tmpList
    
    finalDict = dict()
    headerRem = []
    for i in resultDict:
        finalDict[i] = []
        for j in range(len(resultDict[i])):
            if reqd_cols[j] in aggr_cols:
                if(aggr_cols[reqd_cols[j]].lower() == "sum"):
                    finalDict[i].append(sum(resultDict[i][j]))
                if(aggr_cols[reqd_cols[j]].lower() == "max"):
                    finalDict[i].append(max(resultDict[i][j]))
                if(aggr_cols[reqd_cols[j]].lower() == "min"):
                    finalDict[i].append(min(resultDict[i][j]))
                if(aggr_cols[reqd_cols[j]].lower() == "count"):
                    finalDict[i].append(len(resultDict[i][j]))
                if(aggr_cols[reqd_cols[j]].lower() == "average" or aggr_cols[reqd_cols[j]].lower() == "avg"):
                    finalDict[i].append(int(sum(resultDict[i][j])/len(resultDict[i][j])))
                tmp = colToTable[reqd_cols[j]].lower() + '.' + reqd_cols[j].lower()
                if tmp not in headerRem:
                    headerRem.append(tmp)
                    header +=  tmp + ','

    header = header[:-1]
    print(header)
    if(len(orderBy) == 0):
        for i in finalDict:
            row_vals = []
            row_vals.append(str(i))
            for j in finalDict[i]:
                row_vals.append(str(j))
            print(','.join(row_vals))
    else:
        orderBy = orderBy.split()[-1].lower()
        if(orderBy == "desc"):
            for i in sorted(finalDict,reverse=True):
                row_vals = []
                row_vals.append(str(i))
                for j in finalDict[i]:
                    row_vals.append(str(j))
                print(','.join(row_vals))
        else:
            for i in sorted(finalDict):
                row_vals = []
                row_vals.append(str(i))
                for j in finalDict[i]:
                    row_vals.append(str(j))
                print(','.join(row_vals))

    sys.exit()

def handleAggr(selectedColumn):
    vals = []
    for i in range(len(reqd_table)):
        if i in reqd_rows:
            vals.append(reqd_table[i][selectedColumn])

    global selected_cols
    selected_cols = reqd_cols[selectedColumn]
    printHeader()
    if(aggr == "average" or aggr == "avg"):
        print(sum(vals)/len(vals))
    if(aggr == "min"):
        print(min(vals))
    if(aggr == "max"):
        print(max(vals))
    if(aggr == "sum"):
        print(sum(vals))
    if(aggr == "count"):
        print(len(vals))
    sys.exit()

def getQueryResult(cols, tables):
    colnames = process_cols(cols)
    selectedColIndex = []
    global reqd_cols
    
    for i in range(len(reqd_cols)):
        if((reqd_cols[i] in colnames) or (colnames[0] == '*')):
            selectedColIndex.append(i)

    if(len(selectedColIndex) < len(colnames)):
        print("Error: Selected column(s) not found")
        sys.exit(1)

    if(len(aggr) == 0):
        res_table = []
        for i in range(len(reqd_table)):
            if i in reqd_rows:
                row_vals = []
                for j in range(len(reqd_table[i])):
                    if j in selectedColIndex:
                        if reqd_cols[j] not in selected_cols:
                            selected_cols.append(reqd_cols[j])
                        row_vals.append(str(reqd_table[i][j]))
                res_table.append(row_vals)
        
        reqd_cols = selected_cols

        if(hasDistinct):
            checked = []
            for e in res_table:
                if e not in checked:
                    checked.append(e)
            res_table = checked
        return res_table
    else:
        handleAggr(selectedColIndex[0])

def printHeader():
    header = ""
    for i in selected_cols:
        header += colToTable[i].lower() + '.' + i.lower() + ','
    header = header[:-1]
    print(header)

def applyOrder(table, order):
    parts = order.split()
    order_col = parts[0].strip()
    order_type = parts[1].lower().strip() if (len(parts) > 1) else "asc"
    selectedColIndex = -1

    for i in range(len(reqd_cols)):
        if (reqd_cols[i] == order_col):
            selectedColIndex = i 

    if(selectedColIndex == -1):
        print("Error: Order by column not found")
        sys.exit()

    for i in range(len(table)):
        table[i] = list(map(int, table[i]))

    if(order_type == "asc"):
        return sorted(table, key = lambda x: x[selectedColIndex])
    else:
        return sorted(table, key = lambda x: x[selectedColIndex], reverse = True)

def parse_query(query):
    
    if(query[-1] == ';'):
        query = query[0:-1]
    else:
        print("Error: Semicolon missing in SQL query")
        return
    tokens = query.split()

    if(tokens[0].lower() != "select"):
        print("Only SELECT statements are supported")
        return
    
    if(len(tokens) < 4):
        print("Invalid SQL syntax")

    cols = ""
    tables = ""
    where = ""
    group = ""
    order = ""
    colsRead = False
    tablesRead = False
    readWhere = False
    readGroup = False
    readOrder = False

    for token in tokens:
        if(token.lower() == "select"):
            continue
        if(token.lower() == "from"):
            colsRead = True
            continue
        if(colsRead == False):
            cols += token + " "
            continue
        if(token.lower() == "where" or token.lower() == "group" or token.lower() == "order"):
            tablesRead = True
            if(token.lower() == "where"):
                readWhere = True
            elif(token.lower() == "group"):
                readGroup = True
                readWhere = False
            else:
                readOrder = True
                readWhere = False
                readGroup = False
            continue
        if(tablesRead == False):
            tables += token + " "
            continue
        if(readWhere == True):
            where += token + " "
            continue
        if(readGroup == True):
            if(token.lower() != "by"):
                group += token + " "
            continue
        if(readOrder == True):
            if(token.lower() != "by"):
                order += token + " "
    
    col_list = cols.replace(',', ' ').split()
    table_list = tables.replace(',', ' ').split()

    if((readWhere == True and len(where) == 0) or (readGroup == True and len(group) == 0)
            or (readOrder == True and len(order) == 0) or len(table_list) == 0 or len(col_list) == 0):
        print("Error: Invalid SQL Syntax")
        sys.exit(1)

    setTable(table_list)
    if(len(where)):
        applyWhere(where)
    if(len(group)):
        applyGroup(col_list,group,order)

    res_table = getQueryResult(col_list, table_list)
    if(len(order)):
        res_table = applyOrder(res_table, order)
        for i in range(len(res_table)):
            res_table[i] = list(map(str, res_table[i]))

    printHeader()

    for row in res_table:
        row_vals = []
        for col in row:
            row_vals.append(col)
        print(','.join(row_vals))

def main():
    read_metadata()
    read_csv()

    if(len(sys.argv)<2):
        print("Provide query in the argument")
    
    parse_query(sys.argv[1])

main()
