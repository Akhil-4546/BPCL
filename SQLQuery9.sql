select * from sql_data_prectices;
select REFERENCE, p_transactionid, DATE, ALP_AMOUNT, 3_WAY_REMARKS,ACTION, count(REFERENCE) from sql_data_prectices ;

select  sum(ALP_AMOUNT) from sql_data_prectices ;

SELECT 
    REFERENCE, 
    COUNT(REFERENCE) AS REFERENCE_COUNT
FROM 
    sql_data_prectices
GROUP BY 
    REFERENCE
LIMIT 0, 1000;

SELECT 
    REFERENCE, 
    COUNT(REFERENCE) AS reference_count
FROM 
    sql_data_prectices
GROUP BY 
    REFERENCE
LIMIT 0, 1000000;







