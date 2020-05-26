SELECT s1.hour, s2.datetime,  s1.ticker, s1.high_price

 
FROM(SELECT datetime, ticker, extract(hour from datetime) as hour, high
   FROM table_03) s2,
(SELECT  ticker, extract(hour from datetime) as hour, max(high) as high_price
   FROM table_03
   GROUP BY  ticker,extract(hour from datetime)
   ORDER BY ticker, extract(hour from datetime)) s1
   
WHERE s1.ticker = s2.ticker AND s1.high_price = s2.high AND s1.hour = s2.hour

ORDER BY s1.ticker, s1.hour, s2.datetime;