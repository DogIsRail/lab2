INSERT INTO student (OUTID, YearTest, Birth, SexTypeName, ClassProfileNAME, ClassLangName, living_area_id, eo_id)
SELECT z.OUTID, z.YearTest, z.Birth, z.SexTypeName, z.ClassProfileNAME, z.ClassLangName, living_area_id, eo_id
FROM zno_results z
JOIN living_area la ON z.Regname = la.Regname
                   AND z.AREANAME = la.AREANAME
                   AND z.TERNAME = la.TERNAME
                   AND z.RegTypeName = la.RegTypeName
JOIN School eo ON z.EOName = eo.EONAME
                                 AND z.EOTypeName = eo.EOTypeName
                                 AND z.EORegName = eo.EORegName
ON CONFLICT DO NOTHING;

