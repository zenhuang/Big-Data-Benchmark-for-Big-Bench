DROP VIEW IF EXISTS q23_tmp_inv;
CREATE VIEW q23_tmp_inv AS
SELECT 	w_warehouse_name, 
	w_warehouse_sk, 
	i_item_sk, 
	d_moy, 
	stdev, mean, 
 	CASE mean WHEN 0.0 THEN NULL ELSE stdev/mean END cov
FROM (
	SELECT 	w_warehouse_name, 
		w_warehouse_sk, 
		i_item_sk,
		d_moy, 
		stddev_samp(inv_quantity_on_hand) stdev,
		avg(inv_quantity_on_hand) mean
	FROM inventory inv
	JOIN item i ON inv.inv_item_sk = i.i_item_sk
	JOIN warehouse w ON inv.inv_warehouse_sk = w.w_warehouse_sk
	JOIN date_dim d ON inv.inv_date_sk = d.d_date_sk 
	AND d.d_year = 1998
	GROUP BY w_warehouse_name, 
		w_warehouse_sk, 
		i_item_sk, 
		d_moy
	) foo
WHERE CASE mean WHEN 0.0
           THEN 0.0
           ELSE stdev/mean END > 1.0;

