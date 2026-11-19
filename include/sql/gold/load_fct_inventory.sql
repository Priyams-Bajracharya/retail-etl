USE SCHEMA retail_etl.gold;

-- Today's snapshot with the dimension versions valid on that date.
-- The last condition makes a rerun of an older date do nothing.
CREATE OR REPLACE TEMPORARY TABLE tmp_inv_src AS
SELECT s.STORE_ID, s.ITEM_ID, s.ONHAND_QTY, s.SNAPSHOT_DT, i.ITEM_KY, l.LOCN_KY
FROM retail_etl.silver.SLV_INVENTORY_POSITION s
JOIN retail_etl.gold.DIM_ITEM i
  ON i.ITEM_ID = s.ITEM_ID AND s.SNAPSHOT_DT BETWEEN i.EFF_START_DT AND i.EFF_END_DT
JOIN retail_etl.gold.DIM_LOCN l
  ON l.LOCN_ID = s.STORE_ID AND s.SNAPSHOT_DT BETWEEN l.EFF_START_DT AND l.EFF_END_DT
WHERE s.BUSINESS_DT = %(business_date)s
  AND s.SNAPSHOT_DT >= (SELECT COALESCE(MAX(EFF_START_DT), DATE '1900-01-01')
                        FROM retail_etl.gold.FCT_INVENTORY);

-- Close open rows whose quantity changed
UPDATE retail_etl.gold.FCT_INVENTORY f
SET EFF_END_DT = DATEADD(day, -1, s.SNAPSHOT_DT), UPDATE_TS = CURRENT_TIMESTAMP()
FROM tmp_inv_src s, retail_etl.gold.DIM_ITEM di, retail_etl.gold.DIM_LOCN dl
WHERE f.EFF_END_DT = DATE '9999-12-31'
  AND di.ITEM_KY = f.ITEM_KY AND dl.LOCN_KY = f.LOCN_KY
  AND di.ITEM_ID = s.ITEM_ID AND dl.LOCN_ID = s.STORE_ID
  AND f.ONHAND_QTY <> s.ONHAND_QTY
  AND s.SNAPSHOT_DT > f.EFF_START_DT;

-- Open a row for every position that has no open row (new or just closed)
INSERT INTO retail_etl.gold.FCT_INVENTORY
    (ITEM_KY, LOCN_KY, EFF_START_DT, EFF_END_DT, ONHAND_QTY)
SELECT s.ITEM_KY, s.LOCN_KY, s.SNAPSHOT_DT, DATE '9999-12-31', s.ONHAND_QTY
FROM tmp_inv_src s
WHERE NOT EXISTS (
    SELECT 1 FROM retail_etl.gold.FCT_INVENTORY f
    JOIN retail_etl.gold.DIM_ITEM di ON di.ITEM_KY = f.ITEM_KY
    JOIN retail_etl.gold.DIM_LOCN dl ON dl.LOCN_KY = f.LOCN_KY
    WHERE f.EFF_END_DT = DATE '9999-12-31'
      AND di.ITEM_ID = s.ITEM_ID AND dl.LOCN_ID = s.STORE_ID);