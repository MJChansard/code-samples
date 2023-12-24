@echo off
c:
cd C:\Users\MCHANS\Repos\MTQT
git pull
xcopy /y /d "C:\Users\scrubbed\Repos\MTQT\automation\SCRIPT CMC Absence Loader.py" "C:\Users\Public\ANES ETL\MTQT"
xcopy /y /d "C:\Users\scrubbed\Repos\MTQT\automation\QUERY Automated MTQT.sql" "C:\Users\Public\ANES ETL\MTQT"
xcopy /y /d "C:\Users\scrubbed\Repos\MTQT\automation\SCRIPT Pre-Migration Processing.sql" "C:\Users\Public\ANES ETL\MTQT"
xcopy /y /d "C:\Users\scrubbed\Repos\MTQT\automation\QUERY Migration File Builder.sql" "C:\Users\Public\ANES ETL\MTQT"

"C:\Users\scrubbed\AppData\Local\Programs\Python\Python310\python.exe" "C:\Users\Public\ANES ETL\MTQT\SCRIPT CMC Absence Loader.py"
echo %ERRORLEVEL%
if %ERRORLEVEL% equ 10 (
	echo Terminating due to UTSW Person Number error
	exit
)

sqlcmd -S ANESCore.swmed.org -d Reports -i "C:\Users\Public\ANES ETL\MTQT\QUERY Automated MTQT.sql"
sqlcmd -S ANESCore.swmed.org -d Reports -i "C:\Users\Public\ANES ETL\MTQT\SCRIPT Pre-Migration Processing.sql"
sqlcmd -S ANESCore.swmed.org -d Reports -i "C:\Users\Public\ANES ETL\MTQT\QUERY Migration File Builder.sql" -o "C:\Users\Public\ANES ETL\MTQT\KronosLeaveRequests.csv" -s , -W -h -1

copy "C:\Users\Public\ANES ETL\MTQT\KronosLeaveRequests.csv" "C:\Users\Public\ANES ETL\MTQT\transmit\KronosLeaveRequests_%date:~-4,4%%date:~-10,2%%date:~-7,2%.csv"
copy "C:\Users\Public\ANES ETL\MTQT\KronosLeaveRequests.csv" "C:\Users\Public\ANES ETL\MTQT\archive\KronosLeaveRequests_%date:~-4,4%%date:~-10,2%%date:~-7,2%.csv"

winscp /console /script="C:\Users\Public\ANES ETL\MTQT\mtqt-winscp-command.txt" /log="C:\Users\Public\ANES ETL\winscp-log.txt" /loglevel=0

timeout /t 5

del "C:\Users\Public\ANES ETL\MTQT\transmit\KronosLeaveRequests_%date:~-4,4%%date:~-10,2%%date:~-7,2%.csv"
del "C:\Users\Public\ANES ETL\MTQT\KronosLeaveRequests.csv"