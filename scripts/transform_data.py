import oracledb

connection = oracledb.connect(
    user="user",
    password="password",
    dsn="//dbserver:1521/PDB1")

getPanData = """
    select pan
    from pan_list
    where (processed is null or processed = 0)
    order by pan ASC
"""

updatePanList = """
    update pan_list set processed = 1
    where pan = :1
    """

getTransformedData = """
select t.*,(select count(*) from country_borders where country = t.previous_country and border_country = t.COUNTRY_CODE) as neighbouring_country from (
SELECT
    amount,
    ARN,
    AUTHORIZER,
    crd_card_crd_num as pan,
    CRD_FIID as CARD_ISSUER,
    CRD_LN as CARD_NETWORK,
    CRD_TYP as CARD_BRAND,
    DEST as DESTINATION,
    EXP_DAT as EXPIRY_DATE,
    FRAUD_FLG as FRAUD,
    MSG_TYPE,
    PT_SRV_COND_CDE as CONDITION_CODE,
    PT_SRV_ENTRY_MDE as ENTRY_MODE,
    RESP_CDE as RESPONSE_CODE,
    RETL_SIC_CDE as MCC,
    RVRL_CDE as REVERSAL_CODE,
    TERM_CITY as TERMINAL_CITY,
    TERM_CNTRY_CDE as COUNTRY_CODE,
    TERM_TERM_ID as TERMINAL_ID,
    TKEY_RKEY_RETAILER_ID as MERCHANT_ID,
    TRAN_CDE_C as TRAN_CATEGORY,
    TRAN_CDE_TC as TRAN_CODE,
    TRAN_CDE_T as CARD_TYPE,
    tran_datetime as TRAN_DATE,
    stddev(amount) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as amount_dev,
    count(*) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW) AS tran_count_1,
    count(*) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW) AS tran_count_7,
    count(*) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND CURRENT ROW) AS tran_count_30,
    round(AVG(approved_amount) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW),2) AS avg_amount_tran_1,
    round(AVG(approved_amount) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW),2) AS avg_amount_tran_7,
    round(AVG(approved_amount) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND CURRENT ROW),2) AS avg_amount_tran_30,
    round((SUM(approved_amount) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW)) / 1,2) AS avg_amount_day_1,
    round((SUM(approved_amount) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW)) / 7,2) AS avg_amount_day_7,
    round((SUM(approved_amount) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND CURRENT ROW)) / 30,2) AS avg_amount_day_30,
    decode(PIN_IND,'1',1,0) as pin_present,
    decode(PIN_TRIES,'Z',1,to_number(PIN_TRIES)) as PIN_TRIES,
    DECODE(substr(PT_SRV_ENTRY_MDE,3,1),'1',1,0) as ABLE_TO_ENTER_PIN,
    DECODE(substr(PT_SRV_ENTRY_MDE,1,2),'05',1,0) as PAN_FROM_CHIP,
    lag(TERM_CNTRY_CDE) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime) as PREVIOUS_COUNTRY,--not included
    decode(lag(TERM_CNTRY_CDE) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime),TERM_CNTRY_CDE,1,0) as SAME_COUNTRY,
    (select count(*) from country_codes where alpha2 = TERM_CNTRY_CDE and sepa_flag = 1) as SEPA_COUNTRY,
    decode(lag(TKEY_RKEY_RETAILER_ID) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime),TKEY_RKEY_RETAILER_ID,1,0) as SAME_MERCHANT,
    count(*) OVER(PARTITION BY RETL_SIC_CDE ORDER BY tran_datetime RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW) AS tran_count_mcc_1,
    count(*) OVER(PARTITION BY RETL_SIC_CDE ORDER BY tran_datetime RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW) AS tran_count_mcc_7,
    count(*) OVER(PARTITION BY RETL_SIC_CDE ORDER BY tran_datetime RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND CURRENT ROW) AS tran_count_mcc_30,
    round(AVG(approved_amount) OVER(PARTITION BY RETL_SIC_CDE ORDER BY tran_datetime RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW),2) AS avg_amount_tran_mcc_1,
    round(AVG(approved_amount) OVER(PARTITION BY RETL_SIC_CDE ORDER BY tran_datetime RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW),2) AS avg_amount_tran_mcc_7,
    round(AVG(approved_amount) OVER(PARTITION BY RETL_SIC_CDE ORDER BY tran_datetime RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND CURRENT ROW),2) AS avg_amount_tran_mcc_30,
    round((SUM(approved_amount) OVER(PARTITION BY RETL_SIC_CDE ORDER BY tran_datetime RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW)) / 1,2) AS avg_amount_day_mcc_1,
    round((SUM(approved_amount) OVER(PARTITION BY RETL_SIC_CDE ORDER BY tran_datetime RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW)) / 7,2) AS avg_amount_day_mcc_7,
    round((SUM(approved_amount) OVER(PARTITION BY RETL_SIC_CDE ORDER BY tran_datetime RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND CURRENT ROW)) / 30,2) AS avg_amount_day_mcc_30,
    DECODE(TRAN_CDE_TC,'13',1,0) as CARD_NOT_PRESENT,
    (tran_datetime - (lag(tran_datetime) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime))) as time_since_prev_tran,
    case when to_char(tran_datetime,'D') > 5 then 1 else 0 end as tran_on_weekend,
    case when to_char(tran_datetime,'HH24') < 7 then 1 else 0 end as tran_at_night,
    decode(substr(lag(RESP_CDE) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime),1,2),'00',0,1) as PREVIOUS_TRAN_DECLINED,
    decode(substr(RESP_CDE,1,2),'00',0,1) as TRAN_DECLINED
FROM
    tran_data
    WHERE crd_card_crd_num = :1
    order by tran_datetime asc) t
"""

insertTransformedData = """
    INSERT INTO TRANSACTIONS 
        (amount, arn, authorizer, pan, card_issuer, card_network, card_brand, destination, expiry_date, fraud, 
        msg_type, condition_code, entry_mode, response_code, mcc, reversal_code, terminal_city, country_code, 
        terminal_id, merchant_id, tran_category, tran_code, card_type, tran_date, amount_dev, 
        tran_count_1, tran_count_7, tran_count_30, avg_amount_tran_1, avg_amount_tran_7, avg_amount_tran_30, 
        avg_amount_day_1, avg_amount_day_7, avg_amount_day_30, pin_present, pin_tries, able_to_enter_pin, 
        pan_from_chip, same_country, sepa_country, same_merchant, 
        tran_count_mcc_1, tran_count_mcc_7, tran_count_mcc_30, avg_amount_tran_mcc_1, avg_amount_tran_mcc_7, avg_amount_tran_mcc_30, 
        avg_amount_day_mcc_1, avg_amount_day_mcc_7, avg_amount_day_mcc_30, card_not_present, 
        time_since_prev_tran, tran_on_weekend, tran_at_night, previous_tran_declined, tran_declined, neighbouring_country, transaction_id)
    VALUES
        (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10,
        :11, :12, :13, :14, :15, :16, :17, :18, :19, :20,
        :21, :22, :23, :24, :25, :26, :27, :28, :29, :30,
        :31, :32, :33, :34, :35, :36, :37, :38, :39, :40,
        :41, :42, :43, :44, :45, :46, :47, :48, :49, :50,
        :51, :52, :53, :54, :55, :56, :57, :58)
"""

f = open("transform_data.log", "w")
seq=10000000000
panCursor = connection.cursor()
for pRow in panCursor.execute(getPanData):
    pan = pRow[0]
    f.write("Start processing of PAN [%s]"%(pan,))
    tranCursor = connection.cursor()
    insertCursor = connection.cursor()
    for row in tranCursor.execute(getTransformedData,(pan,)):
        amount = row[0]
        arn = row[1]
        authorizer = row[2]
        pan = row[3]
        card_issuer = row[4]
        card_network = row[5]
        card_brand = row[6]
        destination = row[7]
        expiry_date = row[8]
        fraud = row[9]
        msg_type = row[10]
        condition_code = row[11]
        entry_mode = row[12]
        response_code = row[13]
        mcc = row[14]
        reversal_code = row[15]
        terminal_city = row[16]
        country_code = row[17]
        terminal_id = row[18]
        merchant_id = row[19]
        tran_category = row[20]
        tran_code = row[21]
        card_type = row[22]
        tran_date = row[23]
        amount_dev = row[24]
        tran_count_1 = row[25]
        tran_count_7 = row[26]
        tran_count_30 = row[27]
        avg_amount_tran_1 = row[28]
        avg_amount_tran_7 = row[29]
        avg_amount_tran_30 = row[30]
        avg_amount_day_1 = row[31]
        avg_amount_day_7 = row[32]
        avg_amount_day_30 = row[33]
        pin_present = row[34]
        pin_tries = row[35]
        able_to_enter_pin = row[36]
        pan_from_chip = row[37]
        previous_country = row[38]
        same_country = row[39]
        sepa_country = row[40]
        same_merchant = row[41]
        tran_count_mcc_1 = row[42]
        tran_count_mcc_7 = row[43]
        tran_count_mcc_30 = row[44]
        avg_amount_tran_mcc_1 = row[45]
        avg_amount_tran_mcc_7 = row[46]
        avg_amount_tran_mcc_30 = row[47]
        avg_amount_day_mcc_1 = row[48]
        avg_amount_day_mcc_7 = row[49]
        avg_amount_day_mcc_30 = row[50]
        card_not_present = row[51]
        time_since_prev_tran = row[52]
        tran_on_weekend = row[53]
        tran_at_night = row[54]
        previous_tran_declined = row[55]
        tran_declined = row[56]
        neighbouring_country = row[57]
        seq = seq+1
        transaction_id = str(seq)
        insertCursor.execute(insertTransformedData,
                    (amount, arn, authorizer, pan, card_issuer, card_network, card_brand, destination, expiry_date, fraud, 
                        msg_type, condition_code, entry_mode, response_code, mcc, reversal_code, terminal_city, country_code, 
                        terminal_id, merchant_id, tran_category, tran_code, card_type, tran_date, amount_dev, 
                        tran_count_1, tran_count_7, tran_count_30, avg_amount_tran_1, avg_amount_tran_7, avg_amount_tran_30, 
                        avg_amount_day_1, avg_amount_day_7, avg_amount_day_30, pin_present, pin_tries, able_to_enter_pin, 
                        pan_from_chip, same_country, sepa_country, same_merchant, 
                        tran_count_mcc_1, tran_count_mcc_7, tran_count_mcc_30, avg_amount_tran_mcc_1, avg_amount_tran_mcc_7, avg_amount_tran_mcc_30, 
                        avg_amount_day_mcc_1, avg_amount_day_mcc_7, avg_amount_day_mcc_30, card_not_present, 
                        time_since_prev_tran, tran_on_weekend, tran_at_night, previous_tran_declined, tran_declined, neighbouring_country, transaction_id))
    updateCursor = connection.cursor()
    updateCursor.execute(updatePanList,(pan,))
    connection.commit()
    f.write("|Completed processing of PAN [%s]\n"%(pan,))
