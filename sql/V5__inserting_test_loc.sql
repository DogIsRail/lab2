INSERT INTO test_loc (PTName, PTRegName, PTAreaName, PTTerName)
SELECT DISTINCT UMLPTName,
  UMLPTRegName,
  UMLPTAreaName,
  UMLPTTerName
FROM
  zno_results
UNION
SELECT DISTINCT UkrPTName,
  UkrPTRegName,
  UkrPTAreaName,
  UkrPTTerName
FROM
  zno_results
UNION
SELECT DISTINCT HistPTName,
  HistPTRegName,
  HistPTAreaName,
  HistPTTerName
FROM
  zno_results
UNION
SELECT DISTINCT MathPTName,
  MathPTRegName,
  MathPTAreaName,
  MathPTTerName
FROM
  zno_results
UNION
SELECT DISTINCT MathStPTName,
  MathStPTRegName,
  MathStPTAreaName,
  MathStPTTerName
FROM
  zno_results
UNION
SELECT DISTINCT PhysPTName,
  PhysPTRegName,
  PhysPTAreaName,
  PhysPTTerName
FROM
  zno_results
UNION
SELECT DISTINCT ChemPTName,
  ChemPTRegName,
  ChemPTAreaName,
  ChemPTTerName
FROM
  zno_results
UNION  
SELECT DISTINCT BioPTName,
  BioPTRegName,
  BioPTAreaName,
  BioPTTerName
FROM
  zno_results
UNION
SELECT DISTINCT GeoPTName,
  GeoPTRegName,
  GeoPTAreaName,
  GeoPTTerName
FROM
  zno_results
UNION
SELECT DISTINCT EngPTName,
  EngPTRegName,
  EngPTAreaName,
  EngPTTerName
FROM
  zno_results
UNION
SELECT DISTINCT FraPTName,
  FraPTRegName,
  FraPTAreaName,
  FraPTTerName
FROM
  zno_results
UNION
SELECT DISTINCT DeuPTName,
  DeuPTRegName,
  DeuPTAreaName,
  DeuPTTerName
FROM
  zno_results
UNION
SELECT DISTINCT SpaPTName,
  SpaPTRegName,
  SpaPTAreaName,
  SpaPTTerName
FROM
  zno_results
ON CONFLICT DO NOTHING;
