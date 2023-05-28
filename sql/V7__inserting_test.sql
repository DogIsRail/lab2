--umltest
INSERT INTO test (student_id, YearTest, test_name, TestStatus, Ball100, Ball12, Ball, AdaptScale, test_loc_id)
SELECT s.OUTID, s.YearTest, z.UMLTest, z.UMLTestStatus, z.UMLBall100, z.UMLBall12, z.UMLBall, z.UMLAdaptScale, tl.test_loc_id
FROM zno_results z
JOIN student s ON z.OUTID = s.OUTID
                AND z.YearTest = s.YearTest
JOIN test_loc tl ON z.UMLPTName = tl.PTName
                    AND z.UMLPTRegName = tl.PTRegName
                    AND z.UMLPTAreaName = tl.PTAreaName
WHERE z.UMLTestStatus IS NOT NULL
ON CONFLICT DO NOTHING;

--UkrTest
INSERT INTO test (student_id, YearTest, test_name, SubTest, TestStatus, Ball100, Ball12, Ball, AdaptScale, test_loc_id)
SELECT s.OUTID, s.YearTest, z.UkrTest, z.UkrSubTest, z.UkrTestStatus, z.UkrBall100, z.UkrBall12, z.UkrBall, z.UkrAdaptScale, tl.test_loc_id
FROM zno_results z
JOIN student s ON z.OUTID = s.OUTID
                AND z.YearTest = s.YearTest
JOIN test_loc tl ON z.UkrPTName = tl.PTName
                    AND z.UkrPTRegName = tl.PTRegName
                    AND z.UkrPTAreaName = tl.PTAreaName
WHERE z.UkrTestStatus IS NOT NULL
ON CONFLICT DO NOTHING;


--HistTest
INSERT INTO test (student_id, YearTest, test_name, Lang, TestStatus, Ball100, Ball12, Ball, test_loc_id)
SELECT s.OUTID, s.YearTest, z.HistTest, z.HistLang, z.HistTestStatus, z.HistBall100, z.HistBall12, z.HistBall, tl.test_loc_id
FROM zno_results z
JOIN student s ON z.OUTID = s.OUTID
                AND z.YearTest = s.YearTest
JOIN test_loc tl ON z.HistPTName = tl.PTName
                    AND z.HistPTRegName = tl.PTRegName
                    AND z.HistPTAreaName = tl.PTAreaName
WHERE z.HistTestStatus IS NOT NULL
ON CONFLICT DO NOTHING;


--MathTest
INSERT INTO test (student_id, YearTest, test_name, Lang, TestStatus, Ball100, Ball12, DPALevel, Ball, test_loc_id)
SELECT s.OUTID, s.YearTest, z.MathTest, z.MathLang, z.MathTestStatus, z.MathBall100, z.MathBall12, z.MathDPALevel, z.MathBall, tl.test_loc_id
FROM zno_results z
JOIN student s ON z.OUTID = s.OUTID
                AND z.YearTest = s.YearTest
JOIN test_loc tl ON z.MathPTName = tl.PTName
                    AND z.MathPTRegName = tl.PTRegName
                    AND z.MathPTAreaName = tl.PTAreaName
WHERE z.MathTestStatus IS NOT NULL
ON CONFLICT DO NOTHING;


--MathStTest
INSERT INTO test (student_id, YearTest, test_name, Lang, TestStatus, Ball12, Ball, test_loc_id)
SELECT s.OUTID, s.YearTest, z.MathStTest, z.MathStLang, z.MathStTestStatus, z.MathStBall12, z.MathStBall, tl.test_loc_id
FROM zno_results z
JOIN student s ON z.OUTID = s.OUTID
                AND z.YearTest = s.YearTest
JOIN test_loc tl ON z.MathStPTName = tl.PTName
                    AND z.MathStPTRegName = tl.PTRegName
                    AND z.MathStPTAreaName = tl.PTAreaName
WHERE z.MathStTestStatus IS NOT NULL
ON CONFLICT DO NOTHING;


--PhysTest
INSERT INTO test (student_id, YearTest, test_name, Lang, TestStatus, Ball100, Ball12, Ball, test_loc_id)
SELECT s.OUTID, s.YearTest, z.PhysTest, z.PhysLang, z.PhysTestStatus, z.PhysBall100, z.PhysBall12, z.PhysBall, tl.test_loc_id
FROM zno_results z
JOIN student s ON z.OUTID = s.OUTID
                AND z.YearTest = s.YearTest
JOIN test_loc tl ON z.PhysPTName = tl.PTName
                    AND z.PhysPTRegName = tl.PTRegName
                    AND z.PhysPTAreaName = tl.PTAreaName
WHERE z.PhysTestStatus IS NOT NULL
ON CONFLICT DO NOTHING;


--ChemTest
INSERT INTO test (student_id, YearTest, test_name, Lang, TestStatus, Ball100, Ball12, Ball, test_loc_id)
SELECT s.OUTID, s.YearTest, z.ChemTest, z.ChemLang, z.ChemTestStatus, z.ChemBall100, z.ChemBall12, z.ChemBall, tl.test_loc_id
FROM zno_results z
JOIN student s ON z.OUTID = s.OUTID
                AND z.YearTest = s.YearTest
JOIN test_loc tl ON z.ChemPTName = tl.PTName
                    AND z.ChemPTRegName = tl.PTRegName
                    AND z.ChemPTAreaName = tl.PTAreaName
WHERE z.ChemTestStatus IS NOT NULL
ON CONFLICT DO NOTHING;


--BioTest
INSERT INTO test (student_id, YearTest, test_name, Lang, TestStatus, Ball100, Ball12, Ball, test_loc_id)
SELECT s.OUTID, s.YearTest, z.BioTest, z.BioLang, z.BioTestStatus, z.BioBall100, z.BioBall12, z.BioBall, tl.test_loc_id
FROM zno_results z
JOIN student s ON z.OUTID = s.OUTID
                AND z.YearTest = s.YearTest
JOIN test_loc tl ON z.BioPTName = tl.PTName
                    AND z.BioPTRegName = tl.PTRegName
                    AND z.BioPTAreaName = tl.PTAreaName
WHERE z.BioTestStatus IS NOT NULL
ON CONFLICT DO NOTHING;


--GeoTest
INSERT INTO test (student_id, YearTest, test_name, Lang, TestStatus, Ball100, Ball12, Ball, test_loc_id)
SELECT s.OUTID, s.YearTest, z.GeoTest, z.GeoLang, z.GeoTestStatus, z.GeoBall100, z.GeoBall12, z.GeoBall, tl.test_loc_id
FROM zno_results z
JOIN student s ON z.OUTID = s.OUTID
                AND z.YearTest = s.YearTest
JOIN test_loc tl ON z.GeoPTName = tl.PTName
                    AND z.GeoPTRegName = tl.PTRegName
                    AND z.GeoPTAreaName = tl.PTAreaName
WHERE z.GeoTestStatus IS NOT NULL
ON CONFLICT DO NOTHING;


--EngTest
INSERT INTO test (student_id, YearTest, test_name, TestStatus, Ball100, Ball12, DPALevel, Ball, test_loc_id)
SELECT s.OUTID, s.YearTest, z.EngTest, z.EngTestStatus, z.EngBall100, z.EngBall12, z.EngDPALevel, z.EngBall, tl.test_loc_id
FROM zno_results z
JOIN student s ON z.OUTID = s.OUTID
                AND z.YearTest = s.YearTest
JOIN test_loc tl ON z.EngPTName = tl.PTName
                    AND z.EngPTRegName = tl.PTRegName
                    AND z.EngPTAreaName = tl.PTAreaName
WHERE z.EngTestStatus IS NOT NULL
ON CONFLICT DO NOTHING;

--FraTest
INSERT INTO test (student_id, YearTest, test_name, TestStatus, Ball100, Ball12, DPALevel, Ball, test_loc_id)
SELECT s.OUTID, s.YearTest, z.FraTest, z.FraTestStats, z.FraBall100, z.FraBall12, z.FraDPALevel, z.FraBall, tl.test_loc_id
FROM zno_results z
JOIN student s ON z.OUTID = s.OUTID
                AND z.YearTest = s.YearTest
JOIN test_loc tl ON z.FraPTName = tl.PTName
                    AND z.FraPTRegName = tl.PTRegName
                    AND z.FraPTAreaName = tl.PTAreaName
WHERE z.FraTestStats IS NOT NULL
ON CONFLICT DO NOTHING;


--DeuTest
INSERT INTO test (student_id, YearTest, test_name, TestStatus, Ball100, Ball12, DPALevel, Ball, test_loc_id)
SELECT s.OUTID, s.YearTest, z.DeuTest, z.DeuTestStatus, z.DeuBall100, z.DeuBall12, z.DeuDPALevel, z.DeuBall, tl.test_loc_id
FROM zno_results z
JOIN student s ON z.OUTID = s.OUTID
                AND z.YearTest = s.YearTest
JOIN test_loc tl ON z.DeuPTName = tl.PTName
                    AND z.DeuPTRegName = tl.PTRegName
                    AND z.DeuPTAreaName = tl.PTAreaName
WHERE z.DeuTestStatus IS NOT NULL
ON CONFLICT DO NOTHING;


--SpaTest
INSERT INTO test (student_id, YearTest, test_name, TestStatus, Ball100, Ball12, DPALevel, Ball, test_loc_id)
SELECT s.OUTID, s.YearTest, z.SpaTest, z.SpaTestStatus, z.SpaBall100, z.SpaBall12, z.SpaDPALevel, z.SpaBall, tl.test_loc_id
FROM zno_results z
JOIN student s ON z.OUTID = s.OUTID
                AND z.YearTest = s.YearTest
JOIN test_loc tl ON z.SpaPTName = tl.PTName
                    AND z.SpaPTRegName = tl.PTRegName
                    AND z.SpaPTAreaName = tl.PTAreaName
WHERE z.SpaTestStatus IS NOT NULL
ON CONFLICT DO NOTHING;
