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
      *
    FROM
      xyz