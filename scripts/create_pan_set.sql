ALTER SESSION SET NLS_LENGTH_SEMANTICS=CHAR;
CREATE TABLE pan_set
(
    pan VARCHAR2(19),
    set_id VARCHAR2(4)
);
CREATE INDEX PAN_SET_IDX1 ON pan_set (pan,set_id);
CREATE INDEX PAN_SET_IDX2 ON pan_set (set_id,pan);
