import xlrd
import datetime
import oracledb

book = xlrd.open_workbook("fraud_data.xls")

connection = oracledb.connect(
    user="user",
    password="password",
    dsn="//dbserver:1521/PDB1")

cursor = connection.cursor()

print("Get MC ISS data")
sh = book.sheet_by_index(0)
print("{0} {1} {2}".format(sh.name, sh.nrows, sh.ncols))
#[text:'DEGRADED PAN', text:'TRAN_DATE', text:'AMOUNT ', text:'currency', text:'TERM_ID', text:'MERCHANT ID', text:'MERCHANT NAME', text:'MERCHANT CITY', text:'CNTRY', text:'MCC', text:'ENTRY MODE', text:'ACQ_ICA/BIN']

arn_num = 10000000000000000000000
for rx in range(sh.nrows):
    arn_num+=1
    if rx > 0:
        pan = sh.cell_value(rx,0)
        tran_date = datetime.datetime(*xlrd.xldate_as_tuple(sh.cell_value(rx,1), book.datemode)).strftime('%Y%m%d%H%M%S')
        amnt_num = sh.cell_value(rx,2)
        amnt = str(int(amnt_num*100))
        ccy = sh.cell_value(rx,3)
        term_id = sh.cell_value(rx,4)
        merchant_id = sh.cell_value(rx,5)
        merchant_name = sh.cell_value(rx,6)
        merchant_city = sh.cell_value(rx,7)
        country = sh.cell_value(rx,8)
        mcc = sh.cell_value(rx,9)
        entry_mode = sh.cell_value(rx,10)
        bin = sh.cell_value(rx,11)
        arn = str(arn_num)
        
        cursor.execute("""INSERT INTO mc_iss (
                        degraded_pan, tran_date, amount_num, currency, term_id, merchant_id, 
                        merchant_name, merchant_city, country, mcc, entry_mode, bin, arn, amount)
                    VALUES (:1, to_date(:2,'YYYYMMDDHH24MISS'), :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14)""",
                    (pan, tran_date, amnt_num, ccy, term_id, merchant_id, merchant_name, merchant_city, country, mcc, entry_mode, bin, arn, amnt))

connection.commit()

print("Get MC ACQ data")
sh = book.sheet_by_index(2)
print("{0} {1} {2}".format(sh.name, sh.nrows, sh.ncols))
#[text:'DEGRADED PAN', text:'ENTRY MODE', text:'MERCHANT ID', text:'TERM ID', text:'MERCHANT NAME', text:'MERCHANT CITY', text:'MCC', text:'FRAUD AMOUNT IN CZK', text:'TRAN_DATE and TIME (yymmdd hhmmss)', text:'ARN']

for rx in range(sh.nrows):
    if rx > 0:
        pan = sh.cell_value(rx,0)
        entry_mode = sh.cell_value(rx,1)
        merchant_id = sh.cell_value(rx,2)
        term_id = sh.cell_value(rx,3)
        merchant_name = sh.cell_value(rx,4)
        merchant_city = sh.cell_value(rx,5)
        mcc = sh.cell_value(rx,6)
        amnt_num = sh.cell_value(rx,7)
        amnt = str(int(amnt_num*100))
        tran_date = "20"+str(int(sh.cell_value(rx,8)))
        arn = sh.cell_value(rx,9)

        cursor.execute("""INSERT INTO mc_acq (
                        degraded_pan, tran_date, amount_num, term_id, merchant_id, 
                        merchant_name, merchant_city, mcc, entry_mode, arn, amount)
                    VALUES (:1, to_date(:2,'YYYYMMDDHH24MISS'), :3, :4, :5, :6, :7, :8, :9, :10, :11)""",
                    (pan, tran_date, amnt_num, term_id, merchant_id, merchant_name, merchant_city, mcc, entry_mode, arn, amnt))

connection.commit()



print("Get VISA ISS data")
sh = book.sheet_by_index(1)
print("{0} {1} {2}".format(sh.name, sh.nrows, sh.ncols))
#[text:'DEGRADED PAN', text:'TRAN_DATE', text:'AMOUNT ', text:'currency', text:'TERM_ID', text:'MERCHANT ID', text:'MERCHANT NAME', text:'MERCHANT CITY', text:'CNTRY', text:'MCC', text:'ENTRY MODE', text:'ACQ_ICA/BIN', text:'ARN']

for rx in range(sh.nrows):
    if rx > 0:
        pan = sh.cell_value(rx,0)
        tran_date = datetime.datetime(*xlrd.xldate_as_tuple(sh.cell_value(rx,1), book.datemode)).strftime('%Y%m%d%H%M%S')
        amnt_num = sh.cell_value(rx,2)
        amnt = str(int(amnt_num*100))
        ccy = sh.cell_value(rx,3)
        term_id = sh.cell_value(rx,4)
        merchant_id = sh.cell_value(rx,5)
        merchant_name = sh.cell_value(rx,6)
        merchant_city = sh.cell_value(rx,7)
        country = sh.cell_value(rx,8)
        mcc = sh.cell_value(rx,9)
        entry_mode = sh.cell_value(rx,10)
        bin = sh.cell_value(rx,11)
        arn = sh.cell_value(rx,12)

        cursor.execute("""INSERT INTO visa_iss (
                        degraded_pan, tran_date, amount_num, currency, term_id, merchant_id, 
                        merchant_name, merchant_city, country, mcc, entry_mode, bin, arn, amount)
                    VALUES (:1, to_date(:2,'YYYYMMDDHH24MISS'), :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14)""",
                    (pan, tran_date, amnt_num, ccy, term_id, merchant_id, merchant_name, merchant_city, country, mcc, entry_mode, bin, arn, amnt))

connection.commit()


print("Get VISA ACQ data")
sh = book.sheet_by_index(3)
#print(sh.row(0))
print("{0} {1} {2}".format(sh.name, sh.nrows, sh.ncols))
#[text:'DEGRADED PAN', text:'MERCHANT ID', text:'TERM ID', text:'MERCHANT NAME', text:'MERCHANT CITY', text:'MCC', text:'FRAUD AMOUNT IN CZK', text:'TRAN_DATE (yymmdd)', text:'TRAN_TIME(HHMMSS)', text:'ARN']

for rx in range(sh.nrows):
    if rx > 0:
        pan = sh.cell_value(rx,0)
        merchant_id = sh.cell_value(rx,1)
        term_id = sh.cell_value(rx,2)
        merchant_name = sh.cell_value(rx,3)
        merchant_city = sh.cell_value(rx,4)
        mcc = sh.cell_value(rx,5)
        amnt_num = sh.cell_value(rx,6)
        amnt = str(int(amnt_num*100))
        tran_date = "20"+(str(int(sh.cell_value(rx,7)))).ljust(6,'0')+(str(int(sh.cell_value(rx,8)))).rjust(6,'0')
        arn = sh.cell_value(rx,9)

        cursor.execute("""INSERT INTO visa_acq (
                        degraded_pan, tran_date, amount_num, term_id, merchant_id, 
                        merchant_name, merchant_city, mcc, arn, amount)
                    VALUES (:1, to_date(:2,'YYYYMMDDHH24MISS'), :3, :4, :5, :6, :7, :8, :9, :10)""",
                    (pan, tran_date, amnt_num, term_id, merchant_id, merchant_name, merchant_city, mcc, arn, amnt))

connection.commit()