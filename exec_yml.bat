del .\data\*.csv
python exec_yml_to_csv.py %1
for %%s in (.\data\*) do bcp  mydb.dbo.row_analysis_history IN %%s -f c:\users\mauri\test.fmt -T -F2