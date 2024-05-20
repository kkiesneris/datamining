ALTER SESSION SET nls_length_semantics = char;

CREATE TABLE mc_iss (
    degraded_pan  VARCHAR2(19),
    tran_date     DATE,
    amount_num    NUMBER,
    amount        VARCHAR2(19),
    currency      VARCHAR(3),
    term_id       VARCHAR2(8),
    merchant_id   VARCHAR2(15),
    merchant_name VARCHAR2(22),
    merchant_city VARCHAR2(13),
    country       VARCHAR2(3),
    mcc           VARCHAR2(4),
    entry_mode    VARCHAR2(2),
    bin           VARCHAR2(5),
    arn           VARCHAR2(23)
);

CREATE TABLE mc_acq (
    degraded_pan  VARCHAR2(19),
    tran_date     DATE,
    amount_num    NUMBER,
    amount        VARCHAR2(19),
    term_id       VARCHAR2(8),
    merchant_id   VARCHAR2(10),
    merchant_name VARCHAR2(22),
    merchant_city VARCHAR2(13),
    mcc           VARCHAR2(4),
    entry_mode    VARCHAR2(2),
    arn           VARCHAR2(23)
);

CREATE TABLE visa_iss (
    degraded_pan  VARCHAR2(19),
    tran_date     DATE,
    amount_num    NUMBER,
    amount        VARCHAR2(19),
    currency      VARCHAR2(3),
    term_id       VARCHAR2(8),
    merchant_id   VARCHAR2(15),
    merchant_name VARCHAR2(25),
    merchant_city VARCHAR2(13),
    country       VARCHAR2(3),
    mcc           VARCHAR2(4),
    entry_mode    VARCHAR2(2),
    bin           VARCHAR2(6),
    arn           VARCHAR2(23)
);

CREATE TABLE visa_acq (
    degraded_pan  VARCHAR2(19),
    tran_date     DATE,
    amount_num    NUMBER,
    amount        VARCHAR2(19),
    term_id       VARCHAR2(8),
    merchant_id   VARCHAR2(10),
    merchant_name VARCHAR2(25),
    merchant_city VARCHAR2(13),
    mcc           VARCHAR2(4),
    arn           VARCHAR2(23)
);
