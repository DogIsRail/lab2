INSERT INTO living_area (RegName, AREANAME, TERNAME, RegTypeName, TerTypeName)
SELECT DISTINCT
  RegName,
  AREANAME,
  TERNAME,
  RegTypeName,
  TerTypeName
FROM
  zno_results;

