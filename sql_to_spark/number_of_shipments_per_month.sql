select
TO_CHAR(shipment_date,'YYYY-MM') AS year_month,
COUNT(DISTINCT(shipment_id,sub_id)) AS count
from amazon_shipment
GROUP BY 1;