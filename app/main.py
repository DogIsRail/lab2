import psycopg2
import pandas as pd
import psycopg2.extras as extras
import csv
import time 

print("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
#function for inserting data
def execute_values_with_index(conn, df, num_rows, start_ind_database=0, start_ind=0, step=5000):
    cursor = conn.cursor()
    
    index_currently = start_ind_database
    
    while start_ind_database < num_rows:
        # Get the current batch of rows
        batch_end_index = min(start_ind + step, num_rows)
        batch_df = df.iloc[start_ind:batch_end_index].copy()
        tuples = [tuple(x) for x in batch_df.to_numpy()]
        
        # Create the INSERT query with values for the values
        cols = ','.join(list(batch_df.columns))
        query = "INSERT INTO zno_results (%s) VALUES %%s ON CONFLICT (OUTID, YearTest) DO NOTHING;" % (cols)
        
        # Execute the INSERT query with the current batch of rows
        extras.execute_values(cursor, query, tuples)
        try:
            conn.commit()
        except:
            print("can't execute")
        # Update the batch start index and index
        start_ind += step
        start_ind_database += step
        index_currently += len(batch_df)
        
        # Print progress message
        print(f"Inserted {index_currently} rows.")
        
    # Commit the transaction and clean up
    cursor.close()

conn = psycopg2.connect(dbname="database", user="postgres", password="postgres", host="db")

while True:
    try:
        #checking connection
        cursor = conn.cursor()

        #creating table
        cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS zno_results(
            OUTID varchar NOT NULL, 
            Birth integer, 
            SexTypeName varchar, 
            RegName varchar, 
            AREANAME varchar, 
            TERNAME varchar, 
            RegTypeName varchar, 
            TerTypeName varchar, 
            ClassProfileNAME varchar, 
            ClassLangName varchar, 
            EONAME varchar, 
            EOTypeName varchar, 
            EORegName varchar, 
            EOAreaName varchar, 
            EOTerName varchar, 
            EOParent varchar, 
            UMLTest varchar, 
            UMLTestStatus varchar, 
            UMLBall100 float, 
            UMLBall12 float, 
            UMLBall float, 
            UMLAdaptScale varchar, 
            UMLPTName varchar, 
            UMLPTRegName varchar, 
            UMLPTAreaName varchar, 
            UMLPTTerName varchar, 
            UkrTest varchar, 
            UkrSubTest varchar, 
            UkrTestStatus varchar, 
            UkrBall100 float, 
            UkrBall12 float, 
            UkrBall float, 
            UkrAdaptScale varchar, 
            UkrPTName varchar, 
            UkrPTRegName varchar, 
            UkrPTAreaName varchar, 
            UkrPTTerName varchar, 
            HistTest varchar, 
            HistLang varchar, 
            HistTestStatus varchar, 
            HistBall100 float, 
            HistBall12 float, 
            HistBall float, 
            HistPTName varchar, 
            HistPTRegName varchar, 
            HistPTAreaName varchar, 
            HistPTTerName varchar, 
            MathTest varchar, 
            MathLang varchar, 
            MathTestStatus varchar, 
            MathBall100 float, 
            MathBall12 float, 
            MathDPALevel varchar, 
            MathBall float, 
            MathPTName varchar, 
            MathPTRegName varchar, 
            MathPTAreaName varchar, 
            MathPTTerName varchar, 
            MathStTest varchar, 
            MathStLang varchar, 
            MathStTestStatus varchar, 
            MathStBall12 float, 
            MathStBall float, 
            MathStPTName varchar, 
            MathStPTRegName varchar, 
            MathStPTAreaName varchar, 
            MathStPTTerName varchar, 
            PhysTest varchar, 
            PhysLang varchar, 
            PhysTestStatus varchar, 
            PhysBall100 float, 
            PhysBall12 float, 
            PhysBall float, 
            PhysPTName varchar, 
            PhysPTRegName varchar, 
            PhysPTAreaName varchar, 
            PhysPTTerName varchar, 
            ChemTest varchar, 
            ChemLang varchar, 
            ChemTestStatus varchar, 
            ChemBall100 float, 
            ChemBall12 float, 
            ChemBall float, 
            ChemPTName varchar,
            ChemPTRegName varchar, 
            ChemPTAreaName varchar, 
            ChemPTTerName varchar, 
            BioTest varchar, 
            BioLang varchar, 
            BioTestStatus varchar, 
            BioBall100 float, 
            BioBall12 float, 
            BioBall float, 
            BioPTName varchar, 
            BioPTRegName varchar, 
            BioPTAreaName varchar, 
            BioPTTerName varchar, 
            GeoTest varchar, 
            GeoLang varchar, 
            GeoTestStatus varchar, 
            GeoBall100 float, 
            GeoBall12 float, 
            GeoBall float, 
            GeoPTName varchar, 
            GeoPTRegName varchar, 
            GeoPTAreaName varchar,
            GeoPTTerName varchar, 
            EngTest varchar, 
            EngTestStatus varchar, 
            EngBall100 float, 
            EngBall12 float, 
            EngDPALevel varchar, 
            EngBall float, 
            EngPTName varchar, 
            EngPTRegName varchar, 
            EngPTAreaName varchar, 
            EngPTTerName varchar, 
            FraTest varchar, 
            FraTestStats varchar, 
            FraBall100 float, 
            FraBall12 float, 
            FraDPALevel varchar, 
            FraBall float, 
            FraPTName varchar, 
            FraPTRegName varchar, 
            FraPTAreaName varchar, 
            FraPTTerName varchar, 
            DeuTest varchar,
            DeuTestStatus varchar, 
            DeuBall100 float, 
            DeuBall12 float, 
            DeuDPALevel varchar, 
            DeuBall float, 
            DeuPTName varchar, 
            DeuPTRegName varchar, 
            DeuPTAreaName varchar, 
            DeuPTTerName varchar, 
            SpaTest varchar, 
            SpaTestStatus varchar, 
            SpaBall100 float, 
            SpaBall12 float, 
            SpaDPALevel varchar, 
            SpaBall float, 
            SpaPTName varchar, 
            SpaPTRegName varchar, 
            SpaPTAreaName varchar, 
            SpaPTTerName varchar,
            YearTest integer
            );"""
        )

        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_OUTID_zno_results ON zno_results(OUTID, YearTest);")     

        #lists of columns for evry database
        l2021 = ['OUTID', 'Birth', 'SexTypeName', 'RegName', 'AREANAME', 'TERNAME', 'RegTypeName', 'TerTypeName', 'ClassProfileNAME', 'ClassLangName', 'EONAME', 'EOTypeName', 'EORegName', 'EOAreaName', 'EOTerName', 'EOParent', 'UMLTest', 'UMLTestStatus', 'UMLBall100', 'UMLBall12', 'UMLBall', 'UMLAdaptScale', 'UMLPTName', 
            'UMLPTRegName', 'UMLPTAreaName', 'UMLPTTerName', 'UkrTest', 'UkrSubTest', 'UkrTestStatus', 'UkrBall100', 'UkrBall12', 'UkrBall', 'UkrAdaptScale', 'UkrPTName', 'UkrPTRegName', 'UkrPTAreaName', 'UkrPTTerName', 'HistTest', 'HistLang', 'HistTestStatus', 'HistBall100', 'HistBall12', 'HistBall', 'HistPTName', 
            'HistPTRegName', 'HistPTAreaName', 'HistPTTerName', 'MathTest', 'MathLang', 'MathTestStatus', 'MathBall100', 'MathBall12', 'MathDpaLevel', 'MathBall', 'MathPTName', 'MathPTRegName', 'MathPTAreaName', 'MathPTTerName', 'MathStTest', 'MathStLang', 'MathStTestStatus', 'MathStBall12', 'MathStBall', 'MathStPTName', 
            'MathStPTRegName', 'MathStPTAreaName', 'MathStPTTerName', 'PhysTest', 'PhysLang', 'PhysTestStatus', 'PhysBall100', 'PhysBall12', 'PhysBall', 'PhysPTName', 'PhysPTRegName', 'PhysPTAreaName', 'PhysPTTerName', 'ChemTest', 'ChemLang', 'ChemTestStatus', 'ChemBall100', 'ChemBall12', 'ChemBall', 'ChemPTName',
            'ChemPTRegName', 'ChemPTAreaName', 'ChemPTTerName', 'BioTest', 'BioLang', 'BioTestStatus', 'BioBall100', 'BioBall12', 'BioBall', 'BioPTName', 'BioPTRegName', 'BioPTAreaName', 'BioPTTerName', 'GeoTest', 'GeoLang', 'GeoTestStatus', 'GeoBall100', 'GeoBall12', 'GeoBall', 'GeoPTName', 'GeoPTRegName', 'GeoPTAreaName',
            'GeoPTTerName', 'EngTest', 'EngTestStatus', 'EngBall100', 'EngBall12', 'EngDPALevel', 'EngBall', 'EngPTName', 'EngPTRegName', 'EngPTAreaName', 'EngPTTerName', 'FraTest', 'FraTestStats', 'FraBall100', 'FraBall12', 'FraDPALevel', 'FraBall', 'FraPTName', 'FraPTRegName', 'FraPTAreaName', 'FraPTTerName', 'DeuTest',
            'DeuTestStatus', 'DeuBall100', 'DeuBall12', 'DeuDPALevel', 'DeuBall', 'DeuPTName', 'DeuPTRegName', 'DeuPTAreaName', 'DeuPTTerName', 'SpaTest', 'SpaTestStatus', 'SpaBall100', 'SpaBall12', 'SpaDPALevel', 'SpaBall', 'SpaPTName', 'SpaPTRegName', 'SpaPTAreaName', 'SpaPTTerName']

        l2020 = ['OUTID', 'Birth', 'SexTypeName', 'RegName', 'AREANAME', 'TERNAME', 'RegTypeName', 'TerTypeName', 'ClassProfileNAME', 'ClassLangName', 'EONAME', 'EOTypeName', 'EORegName', 'EOAreaName', 'EOTerName', 'EOParent', 'UkrTest', 'UkrTestStatus', 'UkrBall100', 'UkrBall12', 'UkrBall', 'UkrAdaptScale', 'UkrPTName', 'UkrPTRegName',
            'UkrPTAreaName', 'UkrPTTerName', 'HistTest', 'HistLang', 'HistTestStatus', 'HistBall100', 'HistBall12', 'HistBall', 'HistPTName', 'HistPTRegName', 'HistPTAreaName', 'HistPTTerName', 'MathTest', 'MathLang', 'MathTestStatus', 'MathBall100', 'MathBall12', 'MathBall', 'MathPTName', 'MathPTRegName', 'MathPTAreaName', 'MathPTTerName', 
            'PhysTest', 'PhysLang', 'PhysTestStatus', 'PhysBall100', 'PhysBall12', 'PhysBall', 'PhysPTName', 'PhysPTRegName', 'PhysPTAreaName', 'PhysPTTerName', 'ChemTest', 'ChemLang', 'ChemTestStatus', 'ChemBall100', 'ChemBall12', 'ChemBall', 'ChemPTName',
            'ChemPTRegName', 'ChemPTAreaName', 'ChemPTTerName', 'BioTest', 'BioLang', 'BioTestStatus', 'BioBall100', 'BioBall12', 'BioBall', 'BioPTName', 'BioPTRegName', 'BioPTAreaName', 'BioPTTerName', 'GeoTest', 'GeoLang', 'GeoTestStatus', 'GeoBall100', 'GeoBall12', 'GeoBall', 'GeoPTName', 'GeoPTRegName', 'GeoPTAreaName',
            'GeoPTTerName', 'EngTest', 'EngTestStatus', 'EngBall100', 'EngBall12', 'EngDPALevel', 'EngBall', 'EngPTName', 'EngPTRegName', 'EngPTAreaName', 'EngPTTerName', 'FraTest', 'FraTestStats', 'FraBall100', 'FraBall12', 'FraDPALevel', 'FraBall', 'FraPTName', 'FraPTRegName', 'FraPTAreaName', 'FraPTTerName', 'DeuTest',
            'DeuTestStatus', 'DeuBall100', 'DeuBall12', 'DeuDPALevel', 'DeuBall', 'DeuPTName', 'DeuPTRegName', 'DeuPTAreaName', 'DeuPTTerName', 'SpaTest', 'SpaTestStatus', 'SpaBall100', 'SpaBall12', 'SpaDPALevel', 'SpaBall', 'SpaPTName', 'SpaPTRegName', 'SpaPTAreaName', 'SpaPTTerName']

        #reading data
        df2021 = pd.read_csv("Odata2021File.csv", encoding='utf-8', delimiter=';', header=None, skiprows=[0], names = l2021)
        df2020 = pd.read_csv("Odata2020File.csv", encoding='windows-1251', delimiter=';', header=None, skiprows=[0], names = l2020)

        #adding year of the test to data
        df2020["YearTest"] = [2020 for _ in range(len(df2020))]
        df2021["YearTest"] = [2021 for _ in range(len(df2021))]

        #changing the type of the balls100 columns
        for col in ['UMLBall100', 'UkrBall100', 'HistBall100', 'MathBall100', 'PhysBall100', 'ChemBall100', 'BioBall100', 'GeoBall100', 'EngBall100', 'FraBall100', 'DeuBall100', 'SpaBall100']:
            if col in list(df2020.columns):
                df2020[col] = df2020[col].str.replace(',','.', regex=True).astype(float)
            df2021[col] = df2021[col].str.replace(',','.', regex=True).astype(float)

        #countimg time of inserting data
        start_time = time.time()
        execute_values_with_index(conn, df2020, len(df2020))
        execute_values_with_index(conn, df2021, len(df2021)+len(df2020), len(df2020)-1)
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        print("ready")

        #writing the file with time of inserting
        with open('time.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["The duration of inserting data is {}".format(elapsed_time)])
            print("good")
            csvfile.close()


        # the query for 6 variant
        query = """SELECT la.RegName, st.YearTest, MIN(t.Ball100) FROM student st
JOIN test t
ON t.student_id = st.OUTID
JOIN living_area la
ON la.living_area_id = st.living_area_id
                    WHERE TestStatus = 'Зараховано'
                    AND test_name = 'Історія України'
                    GROUP BY la.RegName, st.YearTest"""
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        #writing the files with results of the query
        with open('query_result.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for row in rows:
                writer.writerow(row)
            print("good")
            csvfile.close()
        conn.close()

        print("done")

    except psycopg2.OperationalError as err:
        print("error connection")
        for i in range(15):
            try:
                conn = psycopg2.connect(dbname="database", user="postgres", password="postgres", host="db")
                break
            except:
                print("no connection")
                time.sleep(20)
    else:
        break
