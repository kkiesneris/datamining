import oracledb
import random
import time

connection = oracledb.connect(
    user="user",
    password="password",
    dsn="//dbserver:1521/PDB1")

def getPanList():
    query = """
        select pan
        from pan_set n
        where
        n.set_id = 'BI0'
    """
    # and not exists (select * from pan_set e where n.pan = e.pan and e.set_id = 'BI0S')
    panList = []
    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
        panList = [row[0] for row in rows]
    return panList

def getRandomPans(panList, number_to_select):
    if number_to_select > len(panList):
        raise ValueError("Number to select exceeds the number of available identifiers.")
    
    selected = random.sample(panList, number_to_select)
    for id in selected:
        panList.remove(id)
    return selected

def insertPans(pans):
    insert_sql = f"INSERT INTO pan_set (pan,set_id) VALUES (:1,'BI0S')"
    with connection.cursor() as cursor:
        cursor.executemany(insert_sql, [(pan,) for pan in pans])
        connection.commit()

def getTranCount(pans):
    sum = 0
    placeholders = ', '.join([':pan' + str(i) for i, _ in enumerate(pans)])
    params = {f'pan{i}': pan for i, pan in enumerate(pans)}
    query = f"SELECT sum(tran_count_iss) FROM pan_list WHERE pan IN ({placeholders})"
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        data = cursor.fetchone()
        sum = data[0]
    return sum

allPanList = getPanList()
maxTranCount = 25150 - 3733
tranCount = 0
totalTranCount = 0
maxReached = 0
while totalTranCount < maxTranCount:
    panList = getRandomPans(allPanList,500)
    tranCount = getTranCount(panList)
    while (totalTranCount + tranCount) > maxTranCount:
        del panList[-1]
        tranCount = getTranCount(panList)
        maxReached = 1
    insertPans(panList)
    totalTranCount = totalTranCount + tranCount
    print("Total tran count: "+str(totalTranCount))
    if maxReached:
        break
