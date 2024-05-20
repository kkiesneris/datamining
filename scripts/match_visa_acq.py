import ssl
import oracledb
from datetime import datetime

#1 - matched fraud
#1 - one transaction for one fraud by amount and card number / 2 - multiple transaction matches for single fraud pair in transaction data / 3 multiple pan/amount pairs in fraud data, must use additional attributes
#0 - automatic matching / 1 - manual matching
#0 - same amount / 1 - amount differs - different currency (can't be directly calculated) / 2 - tran date differs / 3 - no PAN available

f = open("match_visa_acq.log", "w")

connection = oracledb.connect(
    user="user",
    password="password",
    dsn="//dbserver:1521/PDB1")

fraudCursor = connection.cursor()

getFraudData = """
    select degraded_pan, tran_date, amount, term_id, merchant_id, merchant_name, merchant_city, mcc, arn
    from visa_acq
    where fraud is null
    and length(degraded_pan) > 15
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
    fMerchantId = getString(fRow[4])
    fMerchantName = getString(fRow[5])
    fMerchantCity = getString(fRow[6])
    fMCC = getString(fRow[7])
    fARN = getString(fRow[8])

    tranCursor = connection.cursor()
    getPosTrans = """
            select crd_card_crd_num, tran_datetime, amt_1, tkey_term_id, tkey_rkey_retailer_id, term_owner_name, term_city, retl_sic_cde, resp_cde, tran_dat, tran_tim
            from pos_trans where 
            fraud is null
            and amt_1 = :1
            and retl_sic_cde = :2
            and retl_term_id = :3
            and retl_ky_rdfkey_id = :4
            and term_city = :5
            and crd_card_crd_num = :6
    """
    tranCursor.execute(getPosTrans,(fAmount,fMCC,fTerminalId,fMerchantId,fMerchantCity,fPAN))
    tRows = tranCursor.fetchall()
    print("PAN|TRAN_DATE|AMOUNT|TERMINAL_ID|MERCHANT_ID|MERCHANT_NAME|MERCHANT_CITY|MCC|ARN")
    f.write("PAN|TRAN_DATE|AMOUNT|TERMINAL_ID|MERCHANT_ID|MERCHANT_NAME|MERCHANT_CITY|MCC|ARN\n")
    print("%s|%s|%s|%s|%s|%s|%s|%s|%s"%(fPAN,fTranDate,fAmount,fTerminalId,fMerchantId,fMerchantName,fMerchantCity,fMCC,fARN))
    f.write("%s|%s|%s|%s|%s|%s|%s|%s|%s\n"%(fPAN,fTranDate,fAmount,fTerminalId,fMerchantId,fMerchantName,fMerchantCity,fMCC,fARN))
    if tRows is None:
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
        tMerchantId = getString(tRow[4])
        tMerchantName = getString(tRow[5])
        tMerchantCity = getString(tRow[6])
        tMCC = getString(tRow[7])
        tResponseCode = getString(tRow[8])
        tTranDate = getString(tRow[9])
        tTranTime = getString(tRow[10])

        print("%s|%s|%s|%s|%s|%s|%s|%s|%s"%(tPAN,tTranDate,tAmount,tTerminalId,tMerchantId,tMerchantName,tMerchantCity,tMCC,tResponseCode))
        f.write("%s|%s|%s|%s|%s|%s|%s|%s|%s\n"%(tPAN,tTranDate,tAmount,tTerminalId,tMerchantId,tMerchantName,tMerchantCity,tMCC,tResponseCode))

        if len(tRows)==1:
            updateCursor = connection.cursor()
            updatePosTrans = """
                update pos_trans set arn = :1 , fraud = '1102' where crd_card_crd_num = :2 and amt_1 = :3 and tran_dat = :4 and tran_tim = :5
            """
            updateCursor.execute(updatePosTrans,(fARN,tPAN,tAmount,tTranDate,tTranTime))

            updateCursor.execute("update visa_acq set fraud = '1102' where arn = :1",(fARN,))
            connection.commit()

print("Match="+str(match))
print("Check="+str(check))
print("No match="+str(no_match))
f.close()
