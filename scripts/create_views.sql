CREATE OR REPLACE VIEW tran_source_2 (
    amount,
    approved_amount,
    tran_declined,
    fraud,
    fraud_flg,
    pan_from_chip,
    card_not_present,
    tran_datetime,
    crd_card_crd_num,
    term_cntry_cde,
    tkey_rkey_retailer_id,
    retl_sic_cde,
    tran_cde_tc,
    pt_srv_entry_mde,
    pin_tries,
    pin_ind,
    tran_cde_t,
    tran_cde_c,
    term_term_id,
    term_city,
    rvrl_cde,
    resp_cde,
    pt_srv_cond_cde,
    msg_type,
    exp_dat,
    dest,
    crd_typ,
    crd_ln,
    crd_fiid,
    authorizer,
    arn
) AS
    SELECT
        (decode(msg_type, '0420', to_number(coalesce(amt_1,'0')) *(- 1), to_number(coalesce(amt_1,'0'))))/100 as amount,
        decode(substr(resp_cde,1,2),'00',((decode(msg_type, '0420', to_number(coalesce(amt_1,'0')) *(- 1), to_number(coalesce(amt_1,'0'))))/100),0) as approved_amount,
        decode(substr(RESP_CDE,1,2),'00',0,1) as TRAN_DECLINED,
        fraud,
        DECODE(fraud,null,0,1) as fraud_flg,
        DECODE(substr(PT_SRV_ENTRY_MDE,1,2),'05',1,0) as PAN_FROM_CHIP,
        DECODE(TRAN_CDE_TC,'13',1,0) as CARD_NOT_PRESENT,
        tran_datetime,
        crd_card_crd_num,
        term_cntry_cde,
        tkey_rkey_retailer_id,
        retl_sic_cde,
        tran_cde_tc,
        pt_srv_entry_mde,
        pin_tries,
        pin_ind,
        tran_cde_t,
        tran_cde_c,
        term_term_id,
        term_city,
        rvrl_cde,
        resp_cde,
        pt_srv_cond_cde,
        msg_type,
        exp_dat,
        dest,
        crd_typ,
        crd_ln,
        crd_fiid,
        authorizer,
        arn
    FROM
        pos_trans
    WHERE
            tran_datetime >= TO_DATE('20091101', 'YYYYMMDD')
        AND tran_datetime <= TO_DATE('20100630', 'YYYYMMDD')
        AND amt_1 is not null;

CREATE OR REPLACE VIEW tran_data (
    tran_datetime,
    tran_cde_tc,
    tran_cde_t,
    pt_srv_cond_cde,
    pt_srv_entry_mde,
    tran_rec_type,
    RETL_SIC_CDE,
    TERM_CNTRY_CDE,
    pin_ind,
    crd_card_crd_num,
    amount,
    approved_amount,
    fraud_flg,
    fraud,
    resp_cde,
    tkey_rkey_retailer_id,
    pin_tries,
    tran_cde_c,
    term_term_id,
    term_city,
    rvrl_cde,
    msg_type,
    exp_dat,
    dest,
    crd_typ,
    crd_ln,
    crd_fiid,
    authorizer,
    arn
) AS
    SELECT
        tran_datetime,
        tran_cde_tc,
        tran_cde_t,
        pt_srv_cond_cde,
        pt_srv_entry_mde,
        tran_rec_type,
        RETL_SIC_CDE,
        TERM_CNTRY_CDE,
        pin_ind,
        crd_card_crd_num,
        (decode(msg_type, '0420', to_number(coalesce(amt_1,'0')) *(- 1), to_number(coalesce(amt_1,'0'))))/100 as amount,
        decode(substr(resp_cde,1,2),'00',((decode(msg_type, '0420', to_number(coalesce(amt_1,'0')) *(- 1), to_number(coalesce(amt_1,'0'))))/100),0) as approved_amount,
        DECODE(fraud,null,0,1) as fraud_flg,
        fraud,
        resp_cde,
        tkey_rkey_retailer_id,
        pin_tries,
        tran_cde_c,
        term_term_id,
        term_city,
        rvrl_cde,
        msg_type,
        exp_dat,
        dest,
        crd_typ,
        crd_ln,
        crd_fiid,
        authorizer,
        arn
    FROM
        pos_trans
    WHERE
            tran_datetime >= TO_DATE('20100101', 'YYYYMMDD')
        AND tran_datetime <= TO_DATE('20100630', 'YYYYMMDD')
        AND amt_1 is not null;


  CREATE OR REPLACE VIEW fraud_data (pan, amount, tran_date, merchant_id, terminal_id, mcc, arn, fraud, source) AS
  SELECT
        pan,
        amount,
        tran_date,
        merchant_id,
        terminal_id,
        mcc,
        arn,
        fraud,
        source
    FROM
        (
            ( SELECT
                mi.degraded_pan          AS pan,
                mi.amount                AS amount,
                mi.tran_date             AS tran_date,
                mi.merchant_id           AS merchant_id,
                mi.term_id               AS terminal_id,
                mi.mcc                   AS mcc,
                mi.arn                   AS arn,
                mi.fraud                 AS fraud,
                'MC_ISS'                 AS source
            FROM
                mc_iss mi
            )
            UNION ALL
            ( SELECT
                ma.degraded_pan          AS pan,
                ma.amount                AS amount,
                ma.tran_date             AS tran_date,
                ma.merchant_id           AS merchant_id,
                ma.term_id               AS terminal_id,
                ma.mcc                   AS mcc,
                ma.arn                   AS arn,
                ma.fraud                 AS fraud,
                'MC_ACQ'                 AS source
            FROM
                mc_acq ma
            )
            UNION ALL
            ( SELECT
                vi.degraded_pan          AS pan,
                vi.amount                AS amount,
                vi.tran_date             AS tran_date,
                vi.merchant_id           AS merchant_id,
                vi.term_id               AS terminal_id,
                vi.mcc                   AS mcc,
                vi.arn                   AS arn,
                vi.fraud                 AS fraud,
                'VISA_ISS'               AS source
            FROM
                visa_iss vi
            )
            UNION ALL
            ( SELECT
                va.degraded_pan          AS pan,
                va.amount                AS amount,
                va.tran_date             AS tran_date,
                va.merchant_id           AS merchant_id,
                va.term_id               AS terminal_id,
                va.mcc                   AS mcc,
                va.arn                   AS arn,
                va.fraud                 AS fraud,
                'VISA_ACQ'               AS source
            FROM
                visa_acq va
            WHERE
                length(va.degraded_pan) > 15
            )
        );

  CREATE OR REPLACE VIEW all_fraud_data (pan, amount, tran_date, merchant_id, terminal_id, mcc, arn, fraud, source) AS
  SELECT
        pan,
        amount,
        tran_date,
        merchant_id,
        terminal_id,
        mcc,
        arn,
        fraud,
        source
    FROM
        (
            ( SELECT
                mi.degraded_pan          AS pan,
                mi.amount                AS amount,
                mi.tran_date             AS tran_date,
                mi.merchant_id           AS merchant_id,
                mi.term_id               AS terminal_id,
                mi.mcc                   AS mcc,
                mi.arn                   AS arn,
                mi.fraud                 AS fraud,
                'MC_ISS'                 AS source
            FROM
                mc_iss mi
            )
            UNION ALL
            ( SELECT
                ma.degraded_pan          AS pan,
                ma.amount                AS amount,
                ma.tran_date             AS tran_date,
                ma.merchant_id           AS merchant_id,
                ma.term_id               AS terminal_id,
                ma.mcc                   AS mcc,
                ma.arn                   AS arn,
                ma.fraud                 AS fraud,
                'MC_ACQ'                 AS source
            FROM
                mc_acq ma
            )
            UNION ALL
            ( SELECT
                vi.degraded_pan          AS pan,
                vi.amount                AS amount,
                vi.tran_date             AS tran_date,
                vi.merchant_id           AS merchant_id,
                vi.term_id               AS terminal_id,
                vi.mcc                   AS mcc,
                vi.arn                   AS arn,
                vi.fraud                 AS fraud,
                'VISA_ISS'               AS source
            FROM
                visa_iss vi
            )
            UNION ALL
            ( SELECT
                va.degraded_pan          AS pan,
                va.amount                AS amount,
                va.tran_date             AS tran_date,
                va.merchant_id           AS merchant_id,
                va.term_id               AS terminal_id,
                va.mcc                   AS mcc,
                va.arn                   AS arn,
                va.fraud                 AS fraud,
                'VISA_ACQ'               AS source
            FROM
                visa_acq va
            )
        );

