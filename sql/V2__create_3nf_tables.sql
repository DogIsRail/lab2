CREATE TABLE IF NOT EXISTS living_area (
  living_area_id SERIAL PRIMARY KEY,
  RegName VARCHAR,
  AREANAME VARCHAR,
  TERNAME VARCHAR,
  RegTypeName VARCHAR,
  TerTypeName VARCHAR
);

CREATE TABLE IF NOT EXISTS School (
  eo_id SERIAL PRIMARY KEY,
  EONAME VARCHAR,
  EOTypeName VARCHAR,
  EORegName VARCHAR,
  EOAreaName VARCHAR,
  EOTerName VARCHAR,
  EOParent VARCHAR
);

CREATE TABLE IF NOT EXISTS test_loc (
  test_loc_id SERIAL PRIMARY KEY,
  PTName VARCHAR,
  PTRegName VARCHAR,
  PTAreaName VARCHAR,
  PTTerName VARCHAR
);

CREATE TABLE IF NOT EXISTS student(
  OUTID VARCHAR,
  YearTest INTEGER,
  Birth INTEGER,
  SexTypeName VARCHAR,
  ClassProfileNAME VARCHAR,
  ClassLangName VARCHAR,
  living_area_id INTEGER,
  eo_id INTEGER,
  CONSTRAINT student_id PRIMARY KEY (OUTID, YearTest)
);

CREATE TABLE IF NOT EXISTS test (
  student_id VARCHAR,
  YearTest INTEGER,
  test_name VARCHAR,
  Lang VARCHAR,
  SubTest VARCHAR,
  TestStatus VARCHAR,
  Ball100 FLOAT,
  Ball12 FLOAT,
  DPALevel VARCHAR,
  Ball FLOAT,
  AdaptScale VARCHAR,
  test_loc_id INTEGER,
  CONSTRAINT test_id PRIMARY KEY (student_id, YearTest, test_name)
);


ALTER TABLE student
ADD FOREIGN KEY (living_area_id) REFERENCES living_area (living_area_id);

ALTER TABLE student
ADD FOREIGN KEY (eo_id) REFERENCES School (eo_id);

ALTER TABLE test
ADD FOREIGN KEY (student_id, YearTest) REFERENCES student (OUTID, YearTest);

ALTER TABLE test
ADD FOREIGN KEY (test_loc_id) REFERENCES test_loc (test_loc_id);
