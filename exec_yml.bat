del .\data\*.csv
python exec_yml_to_csv.py %*
for %%s in (.\data\*) do bcp  mydb.dbo.row_analysis_history IN "%%s" -f dq_result.fmt -T -F2 -S localhost
sqlcmd -S localhost -i .\update_summary.sql -d mydb
