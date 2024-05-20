ALTER SESSION SET NLS_LENGTH_SEMANTICS=CHAR;
CREATE TABLE pan_list
(
    pan VARCHAR2(19),
    processed NUMBER,
    tran_count NUMBER,
    tran_count_iss NUMBER,
    date_min DATE,
    date_max DATE
);
CREATE INDEX pan_list ON pan_list (pan);
