import oracledb
import time

connection = oracledb.connect(
    user="miner",
    password="miner",
    dsn="//dell:1521/PDB1")

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
SELECT amount, arn, authorizer, pan, card_issuer, card_network, card_brand,destination,
    expiry_date, fraud,msg_type, condition_code, entry_mode, response_code, mcc, reversal_code,
    terminal_city, country_code, terminal_id, merchant_id, tran_category, tran_code, card_type,
    tran_date, pin_present, pin_tries, able_to_enter_pin, pan_from_chip, same_country,
    sepa_country, same_merchant, high_risk_mcc, card_not_present, tran_at_night,
    previous_tran_declined, tran_declined,
    decode(amount_diff,null,100,amount_diff) AS amount_diff,
    (SELECT count(*) FROM country_borders WHERE country = t.previous_country AND border_country = t.country_code) AS neighbouring_country,
    count_10m, count_60m, count_1d, count_7d, count_30d, count_60d, sum_1d, sum_7d, sum_30d, sum_60d,
    round(decode(count_1d,0,0,sum_1d/count_1d),2) AS avg_tran_amount_1d,
    round(decode(count_7d,0,0,sum_7d/count_7d),2) AS avg_tran_amount_7d,
    round(decode(count_30d,0,0,sum_30d/count_30d),2) AS avg_tran_amount_30d,
    round(decode(count_60d,0,0,sum_60d/count_60d),2) AS avg_tran_amount_60d,
    round(sum_1d/1,2) AS avg_day_amount_1d,
    round(sum_7d/7,2) AS avg_day_amount_7d,
    round(sum_30d/30,2) AS avg_day_amount_30d,
    round(sum_60d/60,2) AS avg_day_amount_60d,
    count_mcc_10m, count_mcc_60m, count_mcc_1d, count_mcc_7d, count_mcc_30d, count_mcc_60d, sum_mcc_1d, sum_mcc_7d, sum_mcc_30d, sum_mcc_60d,
    round(decode(count_mcc_1d,0,0,sum_mcc_1d/count_mcc_1d),2) AS avg_mcc_tran_amount_1d,
    round(decode(count_mcc_7d,0,0,sum_mcc_7d/count_mcc_7d),2) AS avg_mcc_tran_amount_7d,
    round(decode(count_mcc_30d,0,0,sum_mcc_30d/count_mcc_30d),2) AS avg_mcc_tran_amount_30d,
    round(decode(count_mcc_60d,0,0,sum_mcc_60d/count_mcc_60d),2) AS avg_mcc_tran_amount_60d,
    round(sum_mcc_1d/1,2) AS avg_mcc_day_amount_1d,
    round(sum_mcc_7d/7,2) AS avg_mcc_day_amount_7d,
    round(sum_mcc_30d/30,2) AS avg_mcc_day_amount_30d,
    round(sum_mcc_60d/60,2) AS avg_mcc_day_amount_60d,
    count_merchant_10m, count_merchant_60m, count_merchant_1d, count_merchant_7d, count_merchant_30d, count_merchant_60d, sum_merchant_1d, sum_merchant_7d, sum_merchant_30d, sum_merchant_60d,
    round(decode(count_merchant_1d,0,0,sum_merchant_1d/count_merchant_1d),2) AS avg_merchant_tran_amount_1d,
    round(decode(count_merchant_7d,0,0,sum_merchant_7d/count_merchant_7d),2) AS avg_merchant_tran_amount_7d,
    round(decode(count_merchant_30d,0,0,sum_merchant_30d/count_merchant_30d),2) AS avg_merchant_tran_amount_30d,
    round(decode(count_merchant_60d,0,0,sum_merchant_60d/count_merchant_60d),2) AS avg_merchant_tran_amount_60d,
    round(sum_merchant_1d/1,2) AS avg_merchant_day_amount_1d,
    round(sum_merchant_7d/7,2) AS avg_merchant_day_amount_7d,
    round(sum_merchant_30d/30,2) AS avg_merchant_day_amount_30d,
    round(sum_merchant_60d/60,2) AS avg_merchant_day_amount_60d,
    count_country_10m, count_country_60m, count_country_1d, count_country_7d, count_country_30d, count_country_60d, sum_country_1d, sum_country_7d, sum_country_30d, sum_country_60d,
    round(decode(count_country_1d,0,0,sum_country_1d/count_country_1d),2) AS avg_country_tran_amount_1d,
    round(decode(count_country_7d,0,0,sum_country_7d/count_country_7d),2) AS avg_country_tran_amount_7d,
    round(decode(count_country_30d,0,0,sum_country_30d/count_country_30d),2) AS avg_country_tran_amount_30d,
    round(decode(count_country_60d,0,0,sum_country_60d/count_country_60d),2) AS avg_country_tran_amount_60d,
    round(sum_country_1d/1,2) AS avg_country_day_amount_1d,
    round(sum_country_7d/7,2) AS avg_country_day_amount_7d,
    round(sum_country_30d/30,2) AS avg_country_day_amount_30d,
    round(sum_country_60d/60,2) AS avg_country_day_amount_60d,
    count_cnp_10m, count_cnp_60m, count_cnp_1d, count_cnp_7d, count_cnp_30d, count_cnp_60d, sum_cnp_1d, sum_cnp_7d, sum_cnp_30d, sum_cnp_60d,
    round(decode(count_cnp_1d,0,0,sum_cnp_1d/count_cnp_1d),2) AS avg_cnp_tran_amount_1d,
    round(decode(count_cnp_7d,0,0,sum_cnp_7d/count_cnp_7d),2) AS avg_cnp_tran_amount_7d,
    round(decode(count_cnp_30d,0,0,sum_cnp_30d/count_cnp_30d),2) AS avg_cnp_tran_amount_30d,
    round(decode(count_cnp_60d,0,0,sum_cnp_60d/count_cnp_60d),2) AS avg_cnp_tran_amount_60d,
    round(sum_cnp_1d/1,2) AS avg_cnp_day_amount_1d,
    round(sum_cnp_7d/7,2) AS avg_cnp_day_amount_7d,
    round(sum_cnp_30d/30,2) AS avg_cnp_day_amount_30d,
    round(sum_cnp_60d/60,2) AS avg_cnp_day_amount_60d,
    count_nochip_10m, count_nochip_60m, count_nochip_1d, count_nochip_7d, count_nochip_30d, count_nochip_60d, sum_nochip_1d, sum_nochip_7d, sum_nochip_30d, sum_nochip_60d,
    round(decode(count_nochip_1d,0,0,sum_nochip_1d/count_nochip_1d),2) AS avg_nochip_tran_amount_1d,
    round(decode(count_nochip_7d,0,0,sum_nochip_7d/count_nochip_7d),2) AS avg_nochip_tran_amount_7d,
    round(decode(count_nochip_30d,0,0,sum_nochip_30d/count_nochip_30d),2) AS avg_nochip_tran_amount_30d,
    round(decode(count_nochip_60d,0,0,sum_nochip_60d/count_nochip_60d),2) AS avg_nochip_tran_amount_60d,
    round(sum_nochip_1d/1,2) AS avg_nochip_day_amount_1d,
    round(sum_nochip_7d/7,2) AS avg_nochip_day_amount_7d,
    round(sum_nochip_30d/30,2) AS avg_nochip_day_amount_30d,
    round(sum_nochip_60d/60,2) AS avg_nochip_day_amount_60d,
    count_declined_10m, count_declined_60m, count_declined_1d, count_declined_7d, count_declined_30d, count_declined_60d,
    decode(time_since_prev_tran,null,365,time_since_prev_tran) AS time_since_prev_tran,
    decode(time_since_prev_mcc_tran,null,365,time_since_prev_mcc_tran) AS time_since_prev_mcc_tran,
    decode(time_since_prev_merchant_tran,null,365,time_since_prev_merchant_tran) AS time_since_prev_merchant_tran,
    decode(time_since_prev_country_tran,null,365,time_since_prev_country_tran) AS time_since_prev_country_tran,
    first_tran, first_mcc_tran, first_merchant_tran, first_country_tran, first_cnp_tran, first_nochip_tran, first_declined_tran
FROM (
SELECT
    amount,
    arn,
    authorizer,
    crd_card_crd_num AS pan,
    crd_fiid AS card_issuer,
    crd_ln AS card_network,
    crd_typ AS card_brand,
    dest AS destination,
    exp_dat AS expiry_date,
    fraud_flg AS fraud,
    msg_type,
    pt_srv_cond_cde AS condition_code,
    pt_srv_entry_mde AS entry_mode,
    resp_cde AS response_code,
    retl_sic_cde AS mcc,
    rvrl_cde AS reversal_code,
    term_city AS terminal_city,
    term_cntry_cde AS country_code,
    term_term_id AS terminal_id,
    tkey_rkey_retailer_id AS merchant_id,
    tran_cde_c AS tran_category,
    tran_cde_tc AS tran_code,
    tran_cde_t AS card_type,
    tran_datetime AS tran_date,
    decode(pin_ind,'1',1,0) AS pin_present,
    decode(pin_tries,'Z',1,to_number(pin_tries)) AS pin_tries,
    decode(substr(pt_srv_entry_mde,3,1),'1',1,0) AS able_to_enter_pin,
    decode(substr(pt_srv_entry_mde,1,2),'05',1,0) AS pan_from_chip,
    lag(term_cntry_cde) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime) AS previous_country,--not included
    decode(lag(term_cntry_cde) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime),term_cntry_cde,1,0) AS same_country,
    (SELECT count(*) FROM country_codes WHERE alpha2 = term_cntry_cde AND sepa_flag = 1) AS sepa_country,
    decode(lag(tkey_rkey_retailer_id) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime),tkey_rkey_retailer_id,1,0) AS same_merchant,
    case when retl_sic_cde in ('5122', '5912', '5962', '5966', '5967', '5933', '7273', '7995', '4816', '5816', '6051') then 1 else 0 end as high_risk_mcc,
    decode(tran_cde_tc,'13',1,0) AS card_not_present,
    case when to_number(to_char(tran_datetime,'HH24')) < 7 OR to_number(to_char(tran_datetime,'HH24')) >= 21 then 1 else 0 end AS tran_at_night,
    decode(lag(tran_declined) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime),1,1,0) AS previous_tran_declined,
    tran_declined,
    decode(amount,0,100,abs(lag(amount) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime)-amount)/amount*100) AS amount_diff,
    count(*) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime RANGE BETWEEN interval '10' MINUTE PRECEDING AND current ROW)-1 AS count_10m,
    count(*) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime RANGE BETWEEN interval '60' MINUTE PRECEDING AND current ROW)-1 AS count_60m,
    count(*) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime RANGE BETWEEN interval '1' DAY PRECEDING AND current ROW)-1 AS count_1d,
    count(*) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime RANGE BETWEEN interval '7' DAY PRECEDING AND current ROW)-1 AS count_7d,
    count(*) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime RANGE BETWEEN interval '30' DAY PRECEDING AND current ROW)-1 AS count_30d,
    count(*) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime RANGE BETWEEN interval '60' DAY PRECEDING AND current ROW)-1 AS count_60d,
    sum(approved_amount) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime RANGE BETWEEN interval '1' DAY PRECEDING AND current ROW)-approved_amount AS sum_1d,
    sum(approved_amount) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime RANGE BETWEEN interval '7' DAY PRECEDING AND current ROW)-approved_amount AS sum_7d,
    sum(approved_amount) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime RANGE BETWEEN interval '30' DAY PRECEDING AND current ROW)-approved_amount AS sum_30d,
    sum(approved_amount) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime RANGE BETWEEN interval '60' DAY PRECEDING AND current ROW)-approved_amount AS sum_60d,
    count(*) OVER(PARTITION BY retl_sic_cde ORDER BY tran_datetime RANGE BETWEEN interval '10' MINUTE PRECEDING AND current ROW)-1 AS count_mcc_10m,
    count(*) OVER(PARTITION BY retl_sic_cde ORDER BY tran_datetime RANGE BETWEEN interval '60' MINUTE PRECEDING AND current ROW)-1 AS count_mcc_60m,
    count(*) OVER(PARTITION BY retl_sic_cde ORDER BY tran_datetime RANGE BETWEEN interval '1' DAY PRECEDING AND current ROW)-1 AS count_mcc_1d,
    count(*) OVER(PARTITION BY retl_sic_cde ORDER BY tran_datetime RANGE BETWEEN interval '7' DAY PRECEDING AND current ROW)-1 AS count_mcc_7d,
    count(*) OVER(PARTITION BY retl_sic_cde ORDER BY tran_datetime RANGE BETWEEN interval '30' DAY PRECEDING AND current ROW)-1 AS count_mcc_30d,
    count(*) OVER(PARTITION BY retl_sic_cde ORDER BY tran_datetime RANGE BETWEEN interval '60' DAY PRECEDING AND current ROW)-1 AS count_mcc_60d,
    sum(approved_amount) OVER(PARTITION BY retl_sic_cde ORDER BY tran_datetime RANGE BETWEEN interval '1' DAY PRECEDING AND current ROW)-approved_amount AS sum_mcc_1d,
    sum(approved_amount) OVER(PARTITION BY retl_sic_cde ORDER BY tran_datetime RANGE BETWEEN interval '7' DAY PRECEDING AND current ROW)-approved_amount AS sum_mcc_7d,
    sum(approved_amount) OVER(PARTITION BY retl_sic_cde ORDER BY tran_datetime RANGE BETWEEN interval '30' DAY PRECEDING AND current ROW)-approved_amount AS sum_mcc_30d,
    sum(approved_amount) OVER(PARTITION BY retl_sic_cde ORDER BY tran_datetime RANGE BETWEEN interval '60' DAY PRECEDING AND current ROW)-approved_amount AS sum_mcc_60d,
    count(*) OVER(PARTITION BY tkey_rkey_retailer_id ORDER BY tran_datetime RANGE BETWEEN interval '10' MINUTE PRECEDING AND current ROW)-1 AS count_merchant_10m,
    count(*) OVER(PARTITION BY tkey_rkey_retailer_id ORDER BY tran_datetime RANGE BETWEEN interval '60' MINUTE PRECEDING AND current ROW)-1 AS count_merchant_60m,
    count(*) OVER(PARTITION BY tkey_rkey_retailer_id ORDER BY tran_datetime RANGE BETWEEN interval '1' DAY PRECEDING AND current ROW)-1 AS count_merchant_1d,
    count(*) OVER(PARTITION BY tkey_rkey_retailer_id ORDER BY tran_datetime RANGE BETWEEN interval '7' DAY PRECEDING AND current ROW)-1 AS count_merchant_7d,
    count(*) OVER(PARTITION BY tkey_rkey_retailer_id ORDER BY tran_datetime RANGE BETWEEN interval '30' DAY PRECEDING AND current ROW)-1 AS count_merchant_30d,
    count(*) OVER(PARTITION BY tkey_rkey_retailer_id ORDER BY tran_datetime RANGE BETWEEN interval '60' DAY PRECEDING AND current ROW)-1 AS count_merchant_60d,
    sum(approved_amount) OVER(PARTITION BY tkey_rkey_retailer_id ORDER BY tran_datetime RANGE BETWEEN interval '1' DAY PRECEDING AND current ROW)-approved_amount AS sum_merchant_1d,
    sum(approved_amount) OVER(PARTITION BY tkey_rkey_retailer_id ORDER BY tran_datetime RANGE BETWEEN interval '7' DAY PRECEDING AND current ROW)-approved_amount AS sum_merchant_7d,
    sum(approved_amount) OVER(PARTITION BY tkey_rkey_retailer_id ORDER BY tran_datetime RANGE BETWEEN interval '30' DAY PRECEDING AND current ROW)-approved_amount AS sum_merchant_30d,
    sum(approved_amount) OVER(PARTITION BY tkey_rkey_retailer_id ORDER BY tran_datetime RANGE BETWEEN interval '60' DAY PRECEDING AND current ROW)-approved_amount AS sum_merchant_60d,
    count(*) OVER(PARTITION BY term_cntry_cde ORDER BY tran_datetime RANGE BETWEEN interval '10' MINUTE PRECEDING AND current ROW)-1 AS count_country_10m,
    count(*) OVER(PARTITION BY term_cntry_cde ORDER BY tran_datetime RANGE BETWEEN interval '60' MINUTE PRECEDING AND current ROW)-1 AS count_country_60m,
    count(*) OVER(PARTITION BY term_cntry_cde ORDER BY tran_datetime RANGE BETWEEN interval '1' DAY PRECEDING AND current ROW)-1 AS count_country_1d,
    count(*) OVER(PARTITION BY term_cntry_cde ORDER BY tran_datetime RANGE BETWEEN interval '7' DAY PRECEDING AND current ROW)-1 AS count_country_7d,
    count(*) OVER(PARTITION BY term_cntry_cde ORDER BY tran_datetime RANGE BETWEEN interval '30' DAY PRECEDING AND current ROW)-1 AS count_country_30d,
    count(*) OVER(PARTITION BY term_cntry_cde ORDER BY tran_datetime RANGE BETWEEN interval '60' DAY PRECEDING AND current ROW)-1 AS count_country_60d,
    sum(approved_amount) OVER(PARTITION BY term_cntry_cde ORDER BY tran_datetime RANGE BETWEEN interval '1' DAY PRECEDING AND current ROW)-approved_amount AS sum_country_1d,
    sum(approved_amount) OVER(PARTITION BY term_cntry_cde ORDER BY tran_datetime RANGE BETWEEN interval '7' DAY PRECEDING AND current ROW)-approved_amount AS sum_country_7d,
    sum(approved_amount) OVER(PARTITION BY term_cntry_cde ORDER BY tran_datetime RANGE BETWEEN interval '30' DAY PRECEDING AND current ROW)-approved_amount AS sum_country_30d,
    sum(approved_amount) OVER(PARTITION BY term_cntry_cde ORDER BY tran_datetime RANGE BETWEEN interval '60' DAY PRECEDING AND current ROW)-approved_amount AS sum_country_60d,
    sum(card_not_present) OVER(PARTITION BY card_not_present ORDER BY tran_datetime RANGE BETWEEN interval '10' MINUTE PRECEDING AND current ROW) - decode(card_not_present,0,0,1) AS count_cnp_10m,
    sum(card_not_present) OVER(PARTITION BY card_not_present ORDER BY tran_datetime RANGE BETWEEN interval '60' MINUTE PRECEDING AND current ROW) - decode(card_not_present,0,0,1) AS count_cnp_60m,
    sum(card_not_present) OVER(PARTITION BY card_not_present ORDER BY tran_datetime RANGE BETWEEN interval '1' DAY PRECEDING AND current ROW) - decode(card_not_present,0,0,1) AS count_cnp_1d,
    sum(card_not_present) OVER(PARTITION BY card_not_present ORDER BY tran_datetime RANGE BETWEEN interval '7' DAY PRECEDING AND current ROW) - decode(card_not_present,0,0,1) AS count_cnp_7d,
    sum(card_not_present) OVER(PARTITION BY card_not_present ORDER BY tran_datetime RANGE BETWEEN interval '30' DAY PRECEDING AND current ROW) - decode(card_not_present,0,0,1) AS count_cnp_30d,
    sum(card_not_present) OVER(PARTITION BY card_not_present ORDER BY tran_datetime RANGE BETWEEN interval '60' DAY PRECEDING AND current ROW) - decode(card_not_present,0,0,1) AS count_cnp_60d,
    sum(decode(card_not_present,0,0,approved_amount)) OVER(PARTITION BY card_not_present ORDER BY tran_datetime RANGE BETWEEN interval '1' DAY PRECEDING AND current ROW) - decode(card_not_present,0,0,approved_amount) AS sum_cnp_1d,
    sum(decode(card_not_present,0,0,approved_amount)) OVER(PARTITION BY card_not_present ORDER BY tran_datetime RANGE BETWEEN interval '7' DAY PRECEDING AND current ROW) - decode(card_not_present,0,0,approved_amount) AS sum_cnp_7d,
    sum(decode(card_not_present,0,0,approved_amount)) OVER(PARTITION BY card_not_present ORDER BY tran_datetime RANGE BETWEEN interval '30' DAY PRECEDING AND current ROW) - decode(card_not_present,0,0,approved_amount) AS sum_cnp_30d,
    sum(decode(card_not_present,0,0,approved_amount)) OVER(PARTITION BY card_not_present ORDER BY tran_datetime RANGE BETWEEN interval '60' DAY PRECEDING AND current ROW) - decode(card_not_present,0,0,approved_amount) AS sum_cnp_60d,
    sum(decode(pan_from_chip,1,0,1)) OVER(PARTITION BY pan_from_chip ORDER BY tran_datetime RANGE BETWEEN interval '10' MINUTE PRECEDING AND current ROW) - decode(pan_from_chip,1,0,1) AS count_nochip_10m,
    sum(decode(pan_from_chip,1,0,1)) OVER(PARTITION BY pan_from_chip ORDER BY tran_datetime RANGE BETWEEN interval '60' MINUTE PRECEDING AND current ROW) - decode(pan_from_chip,1,0,1) AS count_nochip_60m,
    sum(decode(pan_from_chip,1,0,1)) OVER(PARTITION BY pan_from_chip ORDER BY tran_datetime RANGE BETWEEN interval '1' DAY PRECEDING AND current ROW) - decode(pan_from_chip,1,0,1) AS count_nochip_1d,
    sum(decode(pan_from_chip,1,0,1)) OVER(PARTITION BY pan_from_chip ORDER BY tran_datetime RANGE BETWEEN interval '7' DAY PRECEDING AND current ROW) - decode(pan_from_chip,1,0,1) AS count_nochip_7d,
    sum(decode(pan_from_chip,1,0,1)) OVER(PARTITION BY pan_from_chip ORDER BY tran_datetime RANGE BETWEEN interval '30' DAY PRECEDING AND current ROW) - decode(pan_from_chip,1,0,1) AS count_nochip_30d,
    sum(decode(pan_from_chip,1,0,1)) OVER(PARTITION BY pan_from_chip ORDER BY tran_datetime RANGE BETWEEN interval '60' DAY PRECEDING AND current ROW) - decode(pan_from_chip,1,0,1) AS count_nochip_60d,
    sum(decode(pan_from_chip,1,0,approved_amount)) OVER(PARTITION BY pan_from_chip ORDER BY tran_datetime RANGE BETWEEN interval '1' DAY PRECEDING AND current ROW) - decode(pan_from_chip,1,0,approved_amount) AS sum_nochip_1d,
    sum(decode(pan_from_chip,1,0,approved_amount)) OVER(PARTITION BY pan_from_chip ORDER BY tran_datetime RANGE BETWEEN interval '7' DAY PRECEDING AND current ROW) - decode(pan_from_chip,1,0,approved_amount) AS sum_nochip_7d,
    sum(decode(pan_from_chip,1,0,approved_amount)) OVER(PARTITION BY pan_from_chip ORDER BY tran_datetime RANGE BETWEEN interval '30' DAY PRECEDING AND current ROW) - decode(pan_from_chip,1,0,approved_amount) AS sum_nochip_30d,
    sum(decode(pan_from_chip,1,0,approved_amount)) OVER(PARTITION BY pan_from_chip ORDER BY tran_datetime RANGE BETWEEN interval '60' DAY PRECEDING AND current ROW) - decode(pan_from_chip,1,0,approved_amount) AS sum_nochip_60d,
    sum(tran_declined) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime RANGE BETWEEN interval '10' MINUTE PRECEDING AND current ROW) - decode(tran_declined,1,1,0) AS count_declined_10m,
    sum(tran_declined) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime RANGE BETWEEN interval '60' MINUTE PRECEDING AND current ROW) - decode(tran_declined,1,1,0) AS count_declined_60m,
    sum(tran_declined) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime RANGE BETWEEN interval '1' DAY PRECEDING AND current ROW) - decode(tran_declined,1,1,0) AS count_declined_1d,
    sum(tran_declined) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime RANGE BETWEEN interval '7' DAY PRECEDING AND current ROW) - decode(tran_declined,1,1,0) AS count_declined_7d,
    sum(tran_declined) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime RANGE BETWEEN interval '30' DAY PRECEDING AND current ROW) - decode(tran_declined,1,1,0) AS count_declined_30d,
    sum(tran_declined) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime RANGE BETWEEN interval '60' DAY PRECEDING AND current ROW) - decode(tran_declined,1,1,0) AS count_declined_60d,
    (tran_datetime - (lag(tran_datetime) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime))) AS time_since_prev_tran,
    (tran_datetime - (lag(tran_datetime) OVER(PARTITION BY retl_sic_cde ORDER BY tran_datetime))) AS time_since_prev_mcc_tran,
    (tran_datetime - (lag(tran_datetime) OVER(PARTITION BY tkey_rkey_retailer_id ORDER BY tran_datetime))) AS time_since_prev_merchant_tran,
    (tran_datetime - (lag(tran_datetime) OVER(PARTITION BY term_cntry_cde ORDER BY tran_datetime))) AS time_since_prev_country_tran,
    decode(lag(tran_datetime) OVER(PARTITION BY crd_card_crd_num ORDER BY tran_datetime),null,1,0) AS first_tran,
    decode(lag(tran_datetime) OVER(PARTITION BY retl_sic_cde ORDER BY tran_datetime),null,1,0) AS first_mcc_tran,
    decode(lag(tran_datetime) OVER(PARTITION BY tkey_rkey_retailer_id ORDER BY tran_datetime),null,1,0) AS first_merchant_tran,
    decode(lag(tran_datetime) OVER(PARTITION BY term_cntry_cde ORDER BY tran_datetime),null,1,0) AS first_country_tran,
    decode(card_not_present,0,0,decode(lag(tran_datetime) OVER(PARTITION BY card_not_present ORDER BY tran_datetime),null,1,0)) AS first_cnp_tran,
    decode(pan_from_chip,1,0,decode(lag(tran_datetime) OVER(PARTITION BY pan_from_chip ORDER BY tran_datetime),null,1,0)) AS first_nochip_tran,
    decode(tran_declined,0,0,decode(lag(tran_datetime) OVER(PARTITION BY tran_declined ORDER BY tran_datetime),null,1,0)) AS first_declined_tran
FROM
    tran_source_2
    WHERE crd_card_crd_num = :1
    ORDER BY tran_datetime asc) t
"""

insertTransformedData = """
    INSERT INTO TRANSACTIONS_2
        (amount,arn,authorizer,pan,card_issuer,card_network,card_brand,destination,
        expiry_date,fraud,msg_type,condition_code,entry_mode,response_code,mcc,reversal_code,
        terminal_city,country_code,terminal_id,merchant_id,tran_category,tran_code,card_type,
        tran_date,pin_present,pin_tries,able_to_enter_pin,pan_from_chip,same_country,
        sepa_country,same_merchant,high_risk_mcc,card_not_present,tran_at_night,
        previous_tran_declined,tran_declined,amount_diff,neighbouring_country,
        count_10m,count_60m,count_1d,count_7d,count_30d,count_60d,sum_1d,sum_7d,sum_30d,sum_60d,
        avg_tran_amount_1d,avg_tran_amount_7d,avg_tran_amount_30d,avg_tran_amount_60d,
        avg_day_amount_1d,avg_day_amount_7d,avg_day_amount_30d,avg_day_amount_60d,
        count_mcc_10m,count_mcc_60m,count_mcc_1d,count_mcc_7d,count_mcc_30d,count_mcc_60d,sum_mcc_1d,sum_mcc_7d,sum_mcc_30d,sum_mcc_60d,
        avg_mcc_tran_amount_1d,avg_mcc_tran_amount_7d,avg_mcc_tran_amount_30d,avg_mcc_tran_amount_60d,
        avg_mcc_day_amount_1d,avg_mcc_day_amount_7d,avg_mcc_day_amount_30d,avg_mcc_day_amount_60d,
        count_merchant_10m,count_merchant_60m,count_merchant_1d,count_merchant_7d,count_merchant_30d,count_merchant_60d,sum_merchant_1d,sum_merchant_7d,sum_merchant_30d,sum_merchant_60d,
        avg_merchant_tran_amount_1d,avg_merchant_tran_amount_7d,avg_merchant_tran_amount_30d,avg_merchant_tran_amount_60d,
        avg_merchant_day_amount_1d,avg_merchant_day_amount_7d,avg_merchant_day_amount_30d,avg_merchant_day_amount_60d,
        count_country_10m,count_country_60m,count_country_1d,count_country_7d,count_country_30d,count_country_60d,sum_country_1d,sum_country_7d,sum_country_30d,sum_country_60d,
        avg_country_tran_amount_1d,avg_country_tran_amount_7d,avg_country_tran_amount_30d,avg_country_tran_amount_60d,
        avg_country_day_amount_1d,avg_country_day_amount_7d,avg_country_day_amount_30d,avg_country_day_amount_60d,
        count_cnp_10m,count_cnp_60m,count_cnp_1d,count_cnp_7d,count_cnp_30d,count_cnp_60d,sum_cnp_1d,sum_cnp_7d,sum_cnp_30d,sum_cnp_60d,
        avg_cnp_tran_amount_1d,avg_cnp_tran_amount_7d,avg_cnp_tran_amount_30d,avg_cnp_tran_amount_60d,
        avg_cnp_day_amount_1d,avg_cnp_day_amount_7d,avg_cnp_day_amount_30d,avg_cnp_day_amount_60d,
        count_nochip_10m,count_nochip_60m,count_nochip_1d,count_nochip_7d,count_nochip_30d,count_nochip_60d,sum_nochip_1d,sum_nochip_7d,sum_nochip_30d,sum_nochip_60d,
        avg_nochip_tran_amount_1d,avg_nochip_tran_amount_7d,avg_nochip_tran_amount_30d,avg_nochip_tran_amount_60d,
        avg_nochip_day_amount_1d,avg_nochip_day_amount_7d,avg_nochip_day_amount_30d,avg_nochip_day_amount_60d,
        count_declined_10m,count_declined_60m,count_declined_1d,count_declined_7d,count_declined_30d,count_declined_60d,
        time_since_prev_tran,time_since_prev_mcc_tran,time_since_prev_merchant_tran,time_since_prev_country_tran,
        first_tran,first_mcc_tran,first_merchant_tran,first_country_tran,first_cnp_tran,first_nochip_tran,first_declined_tran,transaction_id)
    VALUES
        (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10,
        :11, :12, :13, :14, :15, :16, :17, :18, :19, :20,
        :21, :22, :23, :24, :25, :26, :27, :28, :29, :30,
        :31, :32, :33, :34, :35, :36, :37, :38, :39, :40,
        :41, :42, :43, :44, :45, :46, :47, :48, :49, :50,
        :51, :52, :53, :54, :55, :56, :57, :58, :59, :60,
        :61, :62, :63, :64, :65, :66, :67, :68, :69, :70,
        :71, :72, :73, :74, :75, :76, :77, :78, :79, :80,
        :81, :82, :83, :84, :85, :86, :87, :88, :89, :90,
        :91, :92, :93, :94, :95, :96, :97, :98, :99, :100,
        :101, :102, :103, :104, :105, :106, :107, :108, :109, :110,
        :111, :112, :113, :114, :115, :116, :117, :118, :119, :120,
        :121, :122, :123, :124, :125, :126, :127, :128, :129, :130,
        :131, :132, :133, :134, :135, :136, :137, :138, :139, :140,
        :141, :142, :143, :144, :145, :146, :147, :148, :149, :150,
        :151, :152, :153, :154, :155, :156, :157, :158, :159, :160,
        :161, :162, :163, :164)
"""

f = open("transform_data_2.log", "w")
seq=10000000000
rowsInserted = 0
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
        pin_present = row[24]
        pin_tries = row[25]
        able_to_enter_pin = row[26]
        pan_from_chip = row[27]
        same_country = row[28]
        sepa_country = row[29]
        same_merchant = row[30]
        high_risk_mcc = row[31]
        card_not_present = row[32]
        tran_at_night = row[33]
        previous_tran_declined = row[34]
        tran_declined = row[35]
        amount_diff = row[36]
        neighbouring_country = row[37]
        count_10m = row[38]
        count_60m = row[39]
        count_1d = row[40]
        count_7d = row[41]
        count_30d = row[42]
        count_60d = row[43]
        sum_1d = row[44]
        sum_7d = row[45]
        sum_30d = row[46]
        sum_60d = row[47]
        avg_tran_amount_1d = row[48]
        avg_tran_amount_7d = row[49]
        avg_tran_amount_30d = row[50]
        avg_tran_amount_60d = row[51]
        avg_day_amount_1d = row[52]
        avg_day_amount_7d = row[53]
        avg_day_amount_30d = row[54]
        avg_day_amount_60d = row[55]
        count_mcc_10m = row[56]
        count_mcc_60m = row[57]
        count_mcc_1d = row[58]
        count_mcc_7d = row[59]
        count_mcc_30d = row[60]
        count_mcc_60d = row[61]
        sum_mcc_1d = row[62]
        sum_mcc_7d = row[63]
        sum_mcc_30d = row[64]
        sum_mcc_60d = row[65]
        avg_mcc_tran_amount_1d = row[66]
        avg_mcc_tran_amount_7d = row[67]
        avg_mcc_tran_amount_30d = row[68]
        avg_mcc_tran_amount_60d = row[69]
        avg_mcc_day_amount_1d = row[70]
        avg_mcc_day_amount_7d = row[71]
        avg_mcc_day_amount_30d = row[72]
        avg_mcc_day_amount_60d = row[73]
        count_merchant_10m = row[74]
        count_merchant_60m = row[75]
        count_merchant_1d = row[76]
        count_merchant_7d = row[77]
        count_merchant_30d = row[78]
        count_merchant_60d = row[79]
        sum_merchant_1d = row[80]
        sum_merchant_7d = row[81]
        sum_merchant_30d = row[82]
        sum_merchant_60d = row[83]
        avg_merchant_tran_amount_1d = row[84]
        avg_merchant_tran_amount_7d = row[85]
        avg_merchant_tran_amount_30d = row[86]
        avg_merchant_tran_amount_60d = row[87]
        avg_merchant_day_amount_1d = row[88]
        avg_merchant_day_amount_7d = row[89]
        avg_merchant_day_amount_30d = row[90]
        avg_merchant_day_amount_60d = row[91]
        count_country_10m = row[92]
        count_country_60m = row[93]
        count_country_1d = row[94]
        count_country_7d = row[95]
        count_country_30d = row[96]
        count_country_60d = row[97]
        sum_country_1d = row[98]
        sum_country_7d = row[99]
        sum_country_30d = row[100]
        sum_country_60d = row[101]
        avg_country_tran_amount_1d = row[102]
        avg_country_tran_amount_7d = row[103]
        avg_country_tran_amount_30d = row[104]
        avg_country_tran_amount_60d = row[105]
        avg_country_day_amount_1d = row[106]
        avg_country_day_amount_7d = row[107]
        avg_country_day_amount_30d = row[108]
        avg_country_day_amount_60d = row[109]
        count_cnp_10m = row[110]
        count_cnp_60m = row[111]
        count_cnp_1d = row[112]
        count_cnp_7d = row[113]
        count_cnp_30d = row[114]
        count_cnp_60d = row[115]
        sum_cnp_1d = row[116]
        sum_cnp_7d = row[117]
        sum_cnp_30d = row[118]
        sum_cnp_60d = row[119]
        avg_cnp_tran_amount_1d = row[120]
        avg_cnp_tran_amount_7d = row[121]
        avg_cnp_tran_amount_30d = row[122]
        avg_cnp_tran_amount_60d = row[123]
        avg_cnp_day_amount_1d = row[124]
        avg_cnp_day_amount_7d = row[125]
        avg_cnp_day_amount_30d = row[126]
        avg_cnp_day_amount_60d = row[127]
        count_nochip_10m = row[128]
        count_nochip_60m = row[129]
        count_nochip_1d = row[130]
        count_nochip_7d = row[131]
        count_nochip_30d = row[132]
        count_nochip_60d = row[133]
        sum_nochip_1d = row[134]
        sum_nochip_7d = row[135]
        sum_nochip_30d = row[136]
        sum_nochip_60d = row[137]
        avg_nochip_tran_amount_1d = row[138]
        avg_nochip_tran_amount_7d = row[139]
        avg_nochip_tran_amount_30d = row[140]
        avg_nochip_tran_amount_60d = row[141]
        avg_nochip_day_amount_1d = row[142]
        avg_nochip_day_amount_7d = row[143]
        avg_nochip_day_amount_30d = row[144]
        avg_nochip_day_amount_60d = row[145]
        count_declined_10m = row[146]
        count_declined_60m = row[147]
        count_declined_1d = row[148]
        count_declined_7d = row[149]
        count_declined_30d = row[150]
        count_declined_60d = row[151]
        time_since_prev_tran = row[152]
        time_since_prev_mcc_tran = row[153]
        time_since_prev_merchant_tran = row[154]
        time_since_prev_country_tran = row[155]
        first_tran = row[156]
        first_mcc_tran = row[157]
        first_merchant_tran = row[158]
        first_country_tran = row[159]
        first_cnp_tran = row[160]
        first_nochip_tran = row[161]
        first_declined_tran = row[162]
        seq = seq+1
        transaction_id = str(seq)
        insertCursor.execute(insertTransformedData,
                    (amount,arn,authorizer,pan,card_issuer,card_network,card_brand,destination,
        expiry_date,fraud,msg_type,condition_code,entry_mode,response_code,mcc,reversal_code,
        terminal_city,country_code,terminal_id,merchant_id,tran_category,tran_code,card_type,
        tran_date,pin_present,pin_tries,able_to_enter_pin,pan_from_chip,same_country,
        sepa_country,same_merchant,high_risk_mcc,card_not_present,tran_at_night,
        previous_tran_declined,tran_declined,amount_diff,neighbouring_country,
        count_10m,count_60m,count_1d,count_7d,count_30d,count_60d,sum_1d,sum_7d,sum_30d,sum_60d,
        avg_tran_amount_1d,avg_tran_amount_7d,avg_tran_amount_30d,avg_tran_amount_60d,
        avg_day_amount_1d,avg_day_amount_7d,avg_day_amount_30d,avg_day_amount_60d,
        count_mcc_10m,count_mcc_60m,count_mcc_1d,count_mcc_7d,count_mcc_30d,count_mcc_60d,sum_mcc_1d,sum_mcc_7d,sum_mcc_30d,sum_mcc_60d,
        avg_mcc_tran_amount_1d,avg_mcc_tran_amount_7d,avg_mcc_tran_amount_30d,avg_mcc_tran_amount_60d,
        avg_mcc_day_amount_1d,avg_mcc_day_amount_7d,avg_mcc_day_amount_30d,avg_mcc_day_amount_60d,
        count_merchant_10m,count_merchant_60m,count_merchant_1d,count_merchant_7d,count_merchant_30d,count_merchant_60d,sum_merchant_1d,sum_merchant_7d,sum_merchant_30d,sum_merchant_60d,
        avg_merchant_tran_amount_1d,avg_merchant_tran_amount_7d,avg_merchant_tran_amount_30d,avg_merchant_tran_amount_60d,
        avg_merchant_day_amount_1d,avg_merchant_day_amount_7d,avg_merchant_day_amount_30d,avg_merchant_day_amount_60d,
        count_country_10m,count_country_60m,count_country_1d,count_country_7d,count_country_30d,count_country_60d,sum_country_1d,sum_country_7d,sum_country_30d,sum_country_60d,
        avg_country_tran_amount_1d,avg_country_tran_amount_7d,avg_country_tran_amount_30d,avg_country_tran_amount_60d,
        avg_country_day_amount_1d,avg_country_day_amount_7d,avg_country_day_amount_30d,avg_country_day_amount_60d,
        count_cnp_10m,count_cnp_60m,count_cnp_1d,count_cnp_7d,count_cnp_30d,count_cnp_60d,sum_cnp_1d,sum_cnp_7d,sum_cnp_30d,sum_cnp_60d,
        avg_cnp_tran_amount_1d,avg_cnp_tran_amount_7d,avg_cnp_tran_amount_30d,avg_cnp_tran_amount_60d,
        avg_cnp_day_amount_1d,avg_cnp_day_amount_7d,avg_cnp_day_amount_30d,avg_cnp_day_amount_60d,
        count_nochip_10m,count_nochip_60m,count_nochip_1d,count_nochip_7d,count_nochip_30d,count_nochip_60d,sum_nochip_1d,sum_nochip_7d,sum_nochip_30d,sum_nochip_60d,
        avg_nochip_tran_amount_1d,avg_nochip_tran_amount_7d,avg_nochip_tran_amount_30d,avg_nochip_tran_amount_60d,
        avg_nochip_day_amount_1d,avg_nochip_day_amount_7d,avg_nochip_day_amount_30d,avg_nochip_day_amount_60d,
        count_declined_10m,count_declined_60m,count_declined_1d,count_declined_7d,count_declined_30d,count_declined_60d,
        time_since_prev_tran,time_since_prev_mcc_tran,time_since_prev_merchant_tran,time_since_prev_country_tran,
        first_tran,first_mcc_tran,first_merchant_tran,first_country_tran,first_cnp_tran,first_nochip_tran,first_declined_tran,transaction_id))
        rowsInserted = rowsInserted + 1
    updateCursor = connection.cursor()
    updateCursor.execute(updatePanList,(pan,))
    connection.commit()
    f.write("|Completed processing of PAN [%s]\n"%(pan,))
    if (rowsInserted > 1000000):
        f.write("Sleep for 10 seconds\n")
        print("%s transactions processed"%str(rowsInserted))
        print("Sleep for 10 seconds")
        time.sleep(10)
        rowsInserted = 0
        print("Continue")
