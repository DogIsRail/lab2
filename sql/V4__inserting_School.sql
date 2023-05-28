INSERT INTO School (EONAME, EOTypeName, EORegName, EOAreaName, EOTerName, EOParent)
SELECT DISTINCT
  EONAME, 
  EOTypeName,
  EORegName,
  EOAreaName,
  EOTerName,
  EOParent
FROM
  zno_results;
