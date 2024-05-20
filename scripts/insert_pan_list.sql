insert into pan_list (pan) 
    select pan as pan
        from pos_trans
        where tran_datetime >= TO_DATE('20100101', 'YYYYMMDD')
        AND tran_datetime <= TO_DATE('20100630', 'YYYYMMDD')
        group by pan;
