select *, datetime_diff(table_1.charttime, table_2.charttime, MINUTE) as datetime_diff
from `learned-vortex-290901.Coagulation.cohort_first_72h` table_1
inner join `learned-vortex-290901.Coagulation.cohort_first_72h` table_2
on (table_1.stay_id = table_2.stay_id and table_1.rownum = table_2.rownum + 1) 
order by table_1.stay_id , table_1.rownum, table_2.rownum