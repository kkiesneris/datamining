ALTER SESSION SET NLS_LENGTH_SEMANTICS=CHAR;
CREATE TABLE country_borders
(
    country VARCHAR2(2),
    border_country VARCHAR2(2)
);
CREATE INDEX country_borders_idx ON country_borders (country, border_country);
