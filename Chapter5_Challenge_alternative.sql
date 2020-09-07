-------ALTERNATIVE SOLUTION------


WITH avg_temp_and_difference AS
(
SELECT *,

       CAST (
       (avg(temperature) OVER (PARTITION BY species))  AS DECIMAL(5,2)) AS avg_temp,

       CAST 
(temperature - (avg(temperature) OVER (PARTITION BY species)) AS DECIMAL(5,2)) AS diff_from_avg
 
        FROM routine_checkups),

exception AS
(
SELECT *,

        CASE WHEN ABS(diff_from_avg)/avg_temp>=0.005 THEN 1 ELSE 0
       END AS is_exception
       
         FROM avg_temp_and_difference
),

Grouped AS(
          
SELECT species,
                 name,

       
                 SUM(is_exception) AS sum_exceptions,
  
               MAX(CASE WHEN is_exception>0 THEN checkup_time ELSE NULL END) AS last_check  
       
                  FROM exception
 GROUP BY species,name
),
ROWS AS(

          SELECT *,

          ROW_NUMBER() OVER (PARTITION BY species ORDER BY sum_exceptions ASC,last_check DESC  ) AS row_number
       
           FROM Grouped),

MAX_ROWS AS(

          SELECT *,
       
          CAST(MAX(row_number) OVER(PARTITION BY species) AS float) AS maxr
        
           FROM ROWS
)

SELECT *,
      
       CAST((row_number/maxr)AS DECIMAL(3,2)) AS selector
      
        FROM MAX_ROWS
  WHERE  CAST((row_number/maxr)AS DECIMAL(3,2))<=0.29  ---WHY NOT 0.25????----
   
         ORDER BY species,sum_exceptions DESC, last_check DESC
 ;