import ssl
import oracledb
from datetime import datetime

#1 - matched fraud
#1 - one transaction for one fraud by amount and card number / 2 - multiple transaction matches for single fraud pair in transaction data / 3 multiple pan/amount pairs in fraud data, must use additional attributes
#0 - automatic matching / 1 - manual matching
#0 - same amount / 1 - amount differs - different currency (can't be directly calculated)

f = open("match_mc_iss_atm.log", "w")

connection = oracledb.connect(
    user="user",
    password="password",
    dsn="//dbserver:1521/PDB1")

fraudCursor = connection.cursor()

getFraudData = """
    select degraded_pan, tran_date, to_char(round(amount/0.0513)), term_id, merchant_name, merchant_city, arn
    from mc_iss
    where fraud is null and mcc = '6011'
    order by degraded_pan, amount_num, tran_date ASC
"""
match = 0
check = 0
no_match = 0

def getString(data):
    if data is None:
        return ""
    else:
        return data.strip()


for fRow in fraudCursor.execute(getFraudData):
    fPAN = getString(fRow[0])
    fTranDate = fRow[1].strftime("%Y%m%d%H%M%S")
    fAmount = getString(fRow[2])
    fTerminalId = getString(fRow[3])
    fMerchantName = getString(fRow[4])
    fMerchantCity = getString(fRow[5])
    fARN = getString(fRow[6])

    tranCursor = connection.cursor()
    getAtmTrans = """
            select crd_pan, tran_datetime, amt_1, term_term_id, term_owner_name, term_city, tran_dat, tran_tim
            from atm_trans where
            fraud is null
            and crd_pan = :1
            AND trunc(tran_datetime,'DAY') + 1 > trunc(to_date(:2,'YYYYMMDDHH24MISS'),'DAY')
            AND trunc(tran_datetime,'DAY') - 1 < trunc(to_date(:3,'YYYYMMDDHH24MISS'),'DAY')
            and (amt_1 * 0.9) < to_number(:4)
            and (amt_1 * 1.1) > to_number(:5)
    """

    tranCursor.execute(getAtmTrans,(fPAN,fTranDate,fTranDate,fAmount,fAmount))
    tRows = tranCursor.fetchall()
    print("PAN|TRAN_DATE|AMOUNT|TERMINAL_ID|MERCHANT_NAME|MERCHANT_CITY|ARN")
    f.write("PAN|TRAN_DATE|AMOUNT|TERMINAL_ID|MERCHANT_NAME|MERCHANT_CITY|ARN\n")
    print("%s|%s|%s|%s|%s|%s|%s"%(fPAN,fTranDate,fAmount,fTerminalId,fMerchantName,fMerchantCity,fARN))
    f.write("%s|%s|%s|%s|%s|%s|%s\n"%(fPAN,fTranDate,fAmount,fTerminalId,fMerchantName,fMerchantCity,fARN))
    if tRows is None or len(tRows) == 0:
        no_match+=1
        print("NO MATCH")
        f.write("NO MATCH\n")
    elif len(tRows)==1:
        match+=1
        print("MATCH")
        f.write("MATCH\n")
    elif (len(tRows)>1):
        check+=1
        print("CHECK")
        f.write("CHECK\n")
    for tRow in tRows:
        tPAN = getString(tRow[0])
        tTranDate = tRow[1].strftime("%Y%m%d%H%M%S")
        tAmount = getString(tRow[2])
        tTerminalId = getString(tRow[3])
        tMerchantName = getString(tRow[4])
        tMerchantCity = getString(tRow[5])
        tTranDate = getString(tRow[6])
        tTranTime = getString(tRow[7])

        print("%s|%s|%s|%s|%s|%s"%(tPAN,tTranDate,tAmount,tTerminalId,tMerchantName,tMerchantCity))
        f.write("%s|%s|%s|%s|%s|%s\n"%(tPAN,tTranDate,tAmount,tTerminalId,tMerchantName,tMerchantCity))

        if len(tRows)==1:
            updateCursor = connection.cursor()
            updateAtmTrans = """
                update atm_trans set arn = :1 , fraud = '1100' where crd_pan = :2 and amt_1 = :3 and tran_dat = :4 and tran_tim = :5
            """
            #updateCursor.execute(updateAtmTrans,(fARN,tPAN,tAmount,tTranDate,tTranTime))
            #updateCursor.execute("update mc_iss set fraud = '1100' where arn = :1",(fARN,))
            #connection.commit()

print("Match="+str(match))
print("Check="+str(check))
print("No match="+str(no_match))
f.close()
