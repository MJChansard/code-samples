#	FILE HEADER
#	
#	AUTHOR:			Matt C
#	PROJECT:		Provider Case Log Pipeline
#
#	DESCRIPTION
#	 -	This is the data refresh file for transferring provider case records from Epic Clarity to Department DW
#	 -	DO NOT use this file to perform an initial load of provide case data
#	 -  This file is ONLY to be used to refresh the tables by acquiring new provider case records

import pyodbc
import os
import sys
from datetime import date, datetime, timedelta

# BLOCK 01 | Script set up

# Logging function
def CreateLogEntry(logObject: list[str], logString: str, applyTimeStamp: bool):
	from datetime import date, datetime, timedelta

	logSpacer = "                              "	# 30 spaces for logging

	try:
		logObject
	except NameError as error:
		print(f"Python Error Output: {error}\nCustom Error: Log object cannot be found.\n")
		return
	
	if applyTimeStamp:
		timestamp = datetime.today()
		logEntry = f"\n({timestamp})  {logString}\n"
		logObject.append(logEntry)
		print(logEntry)
	else:
		logEntry = f"{logSpacer}{logString}\n"
		logObject.append(logEntry)
		print(logEntry)

currentPC = os.environ['COMPUTERNAME']
if currentPC == "laptop":
	logPath = os.path.join("C:\\", "Users", "scrubbed", "Sandbox", "ProviderCaseLog", "logs", "")			# C:\Users\scrubbed\Sandbox\ProviderCaseLog\logs\
elif currentPC == "etl":
	logPath = os.path.join("C:\\", "Users", "Public", "ANES ETL", "Epic", "Provider Case Pipeline", "logs", "")
	
if os.path.exists(logPath) == False:
	os.makedirs(logPath)

logName = "ProviderCasePipelineLog_" + str(date.today()) + ".txt"
log = ["EpicProviderCasePipelineLog_" + str(date.today()) + ".txt"]

processStart = datetime.today()


## SQL Object variables
importCadenceTableName = "import.ProviderCaseLog_Cadence"
importOpTimeTableName = "import.ProviderCaseLog_OpTime"
stageTableName = "stage.ProviderCaseLog"
reportTableName = "dbo.ProviderCaseLog"

stageRecordsUspName = "import.usp_DoStagingProviderCaseLogs"


CreateLogEntry(log, "Establishing ODBC Connections", True)

if currentPC == "laptop":
	EDW = pyodbc.connect("DSN=EDW_Primary;Database=ClarityMirror;")
elif currentPC == "etl":
	EDW = pyodbc.connect("DSN=EDW;Database=ClarityMirror;;")
ETL = pyodbc.connect("DSN=ETL1;Database=StagingAPMDW;")
Core = pyodbc.connect("DSN=Core;Database=APMDW;")

cursorEDW = EDW.cursor()
cursorETL = ETL.cursor()
cursorCore = Core.cursor()

CreateLogEntry(log, "ODBC Connections Established", True)


# BLOCK 02 | Transfer Case Records

CreateLogEntry(log, "\n\nETL PHASE: Transfer OpTime Records\n", True)
CreateLogEntry(log, "Submitting Query", True)

queryStart = datetime.today()
cursorEDW.execute("""
	DECLARE @EndDate DATE = GETDATE();
	DECLARE @StartDate DATE = DATEADD(DAY, -14, @EndDate);
		
	WITH GatherCTE AS
	(
		SELECT DISTINCT
			a.log_id				AS CaseID
			, a.AN_DATE				AS CaseDate
			, g.prov_id				AS ProviderEpicID
			, ProviderType = 
				CASE
					WHEN zc1.[NAME] = 'Anesthesiologist'						THEN 1
					WHEN zc1.[NAME] = 'ANESTHESIOLOGY FELLOW'					THEN 2
					WHEN zc1.[NAME] = 'Anesthesiology Resident'					THEN 3
					WHEN zc1.[NAME] = 'CRNA'									THEN 4
					WHEN zc1.[NAME] = 'Perfusionist'							THEN 5
					WHEN zc1.[NAME] = 'RESIDENT REGISTERED NURSE ANESTHETIST'	THEN 6
				END	
			, c.AN_BEGIN_LOCAL_DTTM	AS ProviderAnesthesiaStart
			, c.AN_END_LOCAL_DTTM	AS ProviderAnesthesiaStop
			, evnt.PatientDischarged_DateTime	AS PatientDischarged
			, lg.STATUS_C			AS LogStatus
			, lg.OR_TIME_EVTS_ENT_C	AS CaseStatus
			, a.UPDATE_DATE			AS LastUpdated
		FROM anes.F_AN_Record_Summary AS a
			INNER JOIN anes.pat_enc AS b
				ON a.an_52_enc_csn_id = b.pat_enc_csn_id
			INNER JOIN dim.AN_STAFF AS c
				ON a.AN_EPISODE_ID = c.SUMMARY_BLOCK_ID
			INNER JOIN anes.identity_id AS e
				ON e.pat_id = a.an_pat_id AND identity_type_id = 10
			LEFT JOIN  anes.or_log AS lg
				ON lg.LOG_ID = a.LOG_ID
			INNER JOIN anes.vw_OR_LogTrackingTimes AS evnt
				ON evnt.LOG_ID = lg.LOG_ID
			LEFT JOIN dim.clarity_ser AS g
				ON g.prov_id = c.AN_PROV_ID
			LEFT JOIN dim.ZC_OR_ANSTAFF_TYPE AS zc1 
				ON zc1.ANEST_STAFF_REQ_C = c.AN_PROV_TYPE_C
		WHERE
			lg.surgery_date BETWEEN @StartDate AND @EndDate
			AND NOT lg.STATUS_C in (4, 6)
	)
	SELECT
		CaseID
		, ROW_NUMBER() OVER(PARTITION BY CaseID ORDER BY ProviderAnesthesiaStart, ProviderType, ProviderEpicID) AS LineNumber
		, CaseDate
		, ProviderEpicID
		, ProviderType
		, ProviderAnesthesiaStart
		, ProviderAnesthesiaStop
		, PatientDischarged
		, LogStatus
		, CaseStatus
		, LastUpdated
	FROM GatherCTE
	ORDER BY CaseDate, CaseID, LineNumber;
""")
queryEnd = datetime.today()
rowsEDW = cursorEDW.fetchall()

CreateLogEntry(log, "Case records received", True)
CreateLogEntry(log, f"Duration: {str(queryEnd - queryStart)}", False)


CreateLogEntry(log, "Writing case records", True)
CreateLogEntry(log, f"Destination: [ETLServer.StagingAPMDW.{importOpTimeTableName}]", False)

queryStart = datetime.today()
sql = f"TRUNCATE TABLE {importOpTimeTableName};"
cursorETL.execute(sql)
cursorETL.commit()
sql = f"""
	INSERT INTO {importOpTimeTableName}
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
cursorETL.executemany(sql, rowsEDW)
cursorETL.commit()
queryEnd = datetime.today()

CreateLogEntry(log, "OpTime records successfully written.", True)
CreateLogEntry(log, f"Duration: {str(queryEnd - queryStart)}", False)


# BLOCK 04 | Import records from ProdServer
#
#	TECHNICAL CONTEXT
#		SQL Server Instance:	ProdServer
#		Database:				APMDW
#		Source Table:			dbo.ProviderCaseLog
#		Target Table:			stage.ProviderCaseLog

CreateLogEntry(log, "\n\nETL PHASE: Transfer ProdServer Records", True)
CreateLogEntry(log, "Constructing Query ...", True)

sql = f"SELECT DISTINCT CaseID FROM {importOpTimeTableName}"
cursorETL.execute(sql)
rowsETL = cursorETL.fetchall()

buildCaseIdString = ""
for row in rowsETL:
	buildCaseIdString += f"{row.CaseID},"
buildCaseIdString = buildCaseIdString[:-1]

sql = f"""
	SELECT
		CaseID
		, LineNumber
		, CaseDate
		, ProviderEpicID
		, ProviderType
		, ProviderAnesthesiaStart
		, ProviderAnesthesiaStop
		, PatientDischarged
		, LogStatus
		, CaseStatus
		, LastUpdate
		, NULL AS ETLCommand
	FROM dbo.ProviderCaseLog
	WHERE CaseID IN ({buildCaseIdString})
	ORDER BY CaseID, ProviderEpicID, ProviderAnesthesiaStart;
"""
print(sql)

CreateLogEntry(log, "Submitting Query ...", True)

queryStart = datetime.today()
cursorCore.execute(sql)
rowsCore = cursorCore.fetchall()
queryEnd = datetime.today()

CreateLogEntry(log, f"Successfully retrieved {len(rowsCore)} ProdServer records in {str(queryEnd - queryStart)} seconds", True)


CreateLogEntry(log, f"Truncating {stageTableName}  ...", True)

queryStart = datetime.today()
cursorETL.execute(f"TRUNCATE TABLE {stageTableName}")
cursorETL.commit()

CreateLogEntry(log, "Writing ANESRecords ...", True)

sql = f"""
	INSERT INTO {stageTableName} (CaseID, LineNumber, CaseDate, ProviderEpicID, ProviderType, ProviderAnesthesiaStart, ProviderAnesthesiaStop, PatientDischarged, LogStatus, CaseStatus, LastUpdate, ETLCommand)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""													# 12 columns
cursorETL.executemany(sql, rowsCore)
cursorETL.commit()
queryEnd = datetime.today()

CreateLogEntry(log, f"{len(rowsCore)} ProdServer records successfully transferred in {str(queryEnd - queryStart)} seconds", True)


# BLOCK 05 | Stage records
#
#	TECHNICAL CONTEXT
#		SQL Server Instance:	ProdServer
#		Database:				APMDW
#		USP:					import.usp_DoStagingProviderCaseLogs

CreateLogEntry(log, "\n\nETL PHASE: Staging provider case records", True)
CreateLogEntry(log, f"Calling [ETLServer.StagingAPMDW.{stageRecordsUspName}] to stage provider case records", True)

cursorETL.execute("{CALL " + stageRecordsUspName + "}")
# USP is defined in \epic-pipeline\Provider Case Log\sql\USP import,stageProviderCaseLogUSP.sql
cursorETL.commit()

CreateLogEntry(log, "Execution of USP complete", True)


# BLOCK 06 | Transfer staged records to production
#
#	TARGET
#		SQL Server Instance: 	ETLServer
#		Database:				StagingAPMDW
#		Table:					dbo.ProviderCaseLog

CreateLogEntry(log, "\n\nETL PHASE: Prepare ProdServer for records", True)
CreateLogEntry(log, "Removing records with staged replacement", True)
CreateLogEntry(log, f"Target: [ProdServer.APMDW.{reportTableName}]", False)


queryStart = datetime.today()
deleteRequired = False
insertRequired = False

sql = f"SELECT DISTINCT CaseID FROM {stageTableName} WHERE ETLCommand IN ('New', 'Update', 'Delete');"
cursorETL.execute(sql)
rowsToDelete = cursorETL.fetchall()
if len(rowsToDelete) > 0:
	CreateLogEntry(log, f"{len(rowsToDelete)} records marked for deletion.", False)
	deleteRequired = True

sql = f"SELECT DISTINCT CaseID FROM {stageTableName} WHERE ETLCommand IN ('New', 'Update');"
cursorETL.execute(sql)
rowsToInsert = cursorETL.fetchall()
if len(rowsToInsert) > 0:
	CreateLogEntry(log, f"{len(rowsToInsert)} records marked for insertion.", False)
	insertRequired = True


if deleteRequired:
	buildCaseIdString = ""
	for row in rowsToDelete:
		buildCaseIdString += f"{row.CaseID},"
	buildCaseIdString = buildCaseIdString[:-1]

	sql = f"DELETE FROM {reportTableName} WHERE CaseID IN ({buildCaseIdString});"
	cursorCore.execute(sql)
	cursorCore.commit()

	queryEnd = datetime.today()

	CreateLogEntry(log, f"Succesfully removed {len(rowsETL)} records from ProdServer.", True)
	CreateLogEntry(log, f"Duration: {str(queryEnd-queryStart)}", False)

if insertRequired:
	CreateLogEntry(log, "Copying staged records to ProdServer", True)
	CreateLogEntry(log, f"Source: [ETLServer.StagingAPMDW.{stageTableName}]", False)
	CreateLogEntry(log, f"Target: [ProdServer.APMDW.{reportTableName}]", False)

	sql = f"""
		SELECT DISTINCT
			CaseID
			, LineNumber
			, CaseDate
			, ProviderEpicID
			, ProviderType
			, ProviderAnesthesiaStart
			, ProviderAnesthesiaStop
			, PatientDischarged
			, LogStatus
			, CaseStatus
			, LastUpdate
		FROM {stageTableName}
		WHERE ETLCommand IN ('New', 'Update');
	"""
	
	queryStart = datetime.today()
	cursorETL.execute(sql)
	rowsETL = cursorETL.fetchall()
	queryStop = datetime.today()

	CreateLogEntry(log, f"Retrieved {len(rowsETL)} records in {queryStop - queryStart} seconds", True)

	sql = f"""
		INSERT INTO {reportTableName}
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	"""
	queryStart = datetime.today()
	cursorCore.executemany(sql, rowsETL)
	cursorCore.commit()
	queryEnd = datetime.today()

	CreateLogEntry(log, "Staged records successfully copied", True)
	CreateLogEntry(log, f"Duration: {str(queryEnd - queryStart)}", False)


if deleteRequired == False and insertRequired == False:
	CreateLogEntry(log, f"No staged records flagged as 'New', 'Update', or 'Delete'.", True)


# BLOCK 07 | Write logs and close connections
#
#	- Write logs to file
#	- Close database connections

processEnd = datetime.today()
processDuration = processEnd - processStart
try:
	with open(logPath + logName, 'x') as logFile:
		logFile.writelines(log)
		logFile.write(f"End process timestamp: {str(processEnd)}\n")
		logFile.write(f"Process duration: {str(processDuration)}\n")
		logFile.write("\n---- PROVIDER CASE REFRESH COMPLETE ----")
except FileExistsError:
	with open(logPath + logName, 'a') as logFile:
		logFile.writelines(log)
		logFile.write("\n\n")
		logFile.write(f"Appending log from additional ETL processes\n")
		logFile.write(f"End process timestamp: {str(processEnd)}\n")
		logFile.write(f"Process duration: {str(processDuration)}\n")
		logFile.write("\n---- PROVIDER CASE REFRESH COMPLETE ----")

# Clean up
EDW.close()
ETL.close()
Core.close()

sys.exit()
#	END OF FILE