-- query 1
SELECT CAST('a' AS CHAR(3)) LIKE 'a' AS unpadded,
       CAST('a' AS CHAR(3)) LIKE 'a__' AS padded,
       CAST('a' AS CHAR(3)) LIKE 'a__' ESCAPE '#' AS padded_with_escape;

-- query 2
SELECT CAST('a%' AS CHAR(5)) LIKE 'a#%___' ESCAPE CAST('#' AS CHAR(1)) AS fixed_width,
       CAST('a%' AS VARCHAR(5)) LIKE CAST('a#%' AS VARCHAR(3)) ESCAPE CAST('#' AS VARCHAR(2)) AS varying_width;
