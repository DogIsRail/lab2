SELECT la.RegName, st.YearTest, MIN(t.Ball100) FROM student st
JOIN test t
ON t.student_id = st.OUTID
JOIN living_area la
ON la.living_area_id = st.living_area_id
                    WHERE TestStatus = 'Зараховано'
                    AND test_name = 'Історія України'
                    GROUP BY la.RegName, st.YearTest
