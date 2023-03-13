/* Recursive_sql */

WITH RECURSIVE xyz AS (
    SELECT
        cast(stg.bk_pos_transaction_id_int as int64) as bk_pos_transaction_id_int,
        stg.dv_scrubbed_serial_num AS dv_scrubbed_serial_num,
        cast(stg.bk_detail_id_int as int64) as bk_detail_id_int,
        stg.rnk
      FROM
        `gcp-edwprddata-prd-33200`.`WORK_SALESDB`.`WI_POS_TRANS_LINE_SERIAL_NUM` AS stg
      WHERE stg.rnk = 1
    UNION ALL
    SELECT
        cast(c2.bk_pos_transaction_id_int as int64),
        substr(concat(trim(c1.dv_scrubbed_serial_num), CASE
          WHEN upper(c2.dv_scrubbed_serial_num) = ' ' THEN ''
          ELSE concat(',', trim(c2.dv_scrubbed_serial_num))
        END), 1, 2000) AS dv_scrubbed_serial_num,
        cast(c2.bk_detail_id_int as int64),
        c2.rnk
      FROM
        xyz AS c1
        INNER JOIN  `gcp-edwprddata-prd-33200`.`WORK_SALESDB`.`WI_POS_TRANS_LINE_SERIAL_NUM` AS c2 
        ON cast(c1.bk_pos_transaction_id_int as int64) = cast(c2.bk_pos_transaction_id_int as int64)
      WHERE c2.rnk = c1.rnk + 1
       AND length(c1.dv_scrubbed_serial_num) + length(c2.dv_scrubbed_serial_num) < 2000)   
  SELECT
      cast(xyz.bk_pos_transaction_id_int as int64),
      xyz.dv_scrubbed_serial_num,
      cast(xyz.bk_detail_id_int as int64),
      xyz.rnk
    FROM
      xyz
    QUALIFY rank() OVER (PARTITION BY cast(xyz.bk_pos_transaction_id_int as int64) ORDER BY cast(xyz.bk_detail_id_int as int64) DESC) = 1
	
/* Rewritten_sql */

WITH base_table_agg AS (
  SELECT BK_POS_TRANSACTION_ID_INT,
         STRING_AGG(DV_SCRUBBED_SERIAL_NUM)
           OVER (PARTITION BY BK_POS_TRANSACTION_ID_INT ORDER BY BK_DETAIL_ID_INT) AS agg_string,
		 BK_DETAIL_ID_INT,
		 RNK
  FROM `gcp-edwprddata-prd-33200`.`WORK_SALESDB`.`WI_POS_TRANS_LINE_SERIAL_NUM`
)
SELECT BK_POS_TRANSACTION_ID_INT,
       agg_string as DV_SCRUBBED_SERIAL_NUM,
       BK_DETAIL_ID_INT,
       RNK
FROM base_table_agg
where length(agg_string) < 2000
QUALIFY rank() OVER (PARTITION BY cast(BK_POS_TRANSACTION_ID_INT as int64) ORDER BY cast(BK_DETAIL_ID_INT as int64) DESC) = 1