#   FILE HEADER
#       File Name:  Schedule.py
#       Author:     Matt C
#       Project:    QGenda Data Mart
#   
#   DESCRIPTION
#       This python script defines a function which acquires QGenda Schedule records via the QGenda
#       REST API.  This file can only execute successfully in environments with ODBC connections 
#       defined  that match the DSNs in the pyodbc connection strings.
#
#   QGenda REST API (https://restapi.qgenda.com/)
#   Endpoint: Schedule (https://restapi.qgenda.com/#0f9bab3f-e1a0-41dd-b743-6ca6a96435f6)
#
 
import os
import pyodbc
import requests
from datetime import date, datetime

def getSchedule(token, companyKey, startDate, endDate):
	# BLOCK 01 | Initialization
	#
	#	- Initialize ODBC connections
	#	- Configuration for connecting to QGenda REST API
	#	- Request and store access token

	processStart = datetime.today()

	importTableName = "import.qdm_Schedule"
	stageTableName = "stage.qdm_Schedule"
	prodTableName = "dbo.Schedule"
	logSpacer = "                           " #27 spaces for logging

	listLog = ["Schedule.getSchedule() commencing\n", f"Process Start Timestamp: {str(processStart)}\n"]
	listLog.append("\n\nETL PHASE: Initialization\n\n")

	ETL = pyodbc.connect('DSN=ETL1;Database=StagingQGenda;')
	Core = pyodbc.connect('DSN=Core;Database=QGenda;')
	listLog.append("ODBC connections opened\n")
	

	# BLOCK 02 | Retreving data from Schedule Endpoint
	#
	#	- Request data via QGenda API
	#	- Truncate (clear) [import.Schedule]
	#	- Store data in ETLServer

	log = "\n\nETL PHASE: Data retrieval from QGenda API\n\n"
	print(log)
	listLog.append(log)
	
	log = "(" + str(datetime.today()) + ")  Preparing API request\n"
	print(log)
	listLog.append(log)
	
	rootURL = "https://api.qgenda.com/v2"
	endPointURL = f"/schedule?companyKey={companyKey}&startDate={startDate}&endDate={endDate}&$select=ScheduleKey,TaskShiftKey,StaffKey,TaskKey,Date,StartDate,StartTime,EndDate,EndTime,TaskName,StaffFName,StaffLName,Credit,TaskIsPrintStart,TaskIsPrintEnd,IsCred,IsLocked,IsPublished,IsStruck,Notes&$orderby=Date"
	getURL = rootURL + endPointURL
	headers = {
	'Authorization': f'bearer {token}'
	}
	payload={}
	
	log = "(" + str(datetime.today()) + ")  Requesting data from QGenda API.\n"
	print(log)
	listLog.append(log)
	
	response = requests.get(getURL, headers=headers, data=payload)
	
	# C:\Users\Public\ANES ETL\QGenda Data Mart\json
	jsonPath = os.path.join("C:\\", "Users", "Public", "ANES ETL", "QGenda Data Mart", "json", "")
	jsonFile = "Schedule_" + str(date.today()) + ".json"
	with open(jsonPath + jsonFile, "w", encoding="utf-8") as file:
		file.write(response.text)
	
	log = "(" + str(datetime.today()) + ")  Received SCHEDULE data from QGenda API\n\n"
	print(log)
	listLog.append(log)

	log = "(" + str(datetime.today()) + f")  Submitting TRUNCATE command.  Target table: [ETLServer.StagingQGenda.{importTableName}]\n"
	print(log)
	listLog.append(log)

	cursorETL = ETL.cursor()
	cursorETL.execute("TRUNCATE TABLE " + importTableName + ";")
	cursorETL.commit()

	log = "(" + str(datetime.today()) + f")  Inserting records.  Target table: [ETLServer.StagingQGenda.{importTableName}]\n"
	print(log)
	listLog.append(log)

	cursorETL.execute("{Call import.usp_ScheduleAPI (?)}", response.text)
	# USP is defined in \QGenda-Data-Mart\pipeline\sql\Schedule\USP import,usp_ScheduleAPI.sql
	cursorETL.commit()

	log = "(" + str(datetime.today()) + ")  Data successfully transferred to ETLServer.\n\n"
	print(log)
	listLog.append(log)


	# BLOCK 03 | Retrieving data from Department Data Warehouse
	#
	#	- This block retrieves records from ProdServer that fall into the refresh window of 30 days
	#	- [stage.Schedule] is truncated in preparation for the new records
	#	- The records retrieved from ProdServer are inserted into [stage.Schedule]

	log = "\n\nETL PHASE: Data retrieval from ProdServer\n\n"
	print(log)
	listLog.append(log)

	log = "(" + str(datetime.today()) + f")  Retrieving records within refresh window from ProdServer.  Target: [{prodTableName}]\n"
	print(log)
	listLog.append(log)
	
	cursorCore = Core.cursor()
	cursorCore.execute("""
		SELECT
			ScheduleKey
			, TaskShiftKey
			, StaffKey
			, TaskKey
			, ScheduleDate
			, StartDate
			, StartTime
			, EndDate
			, EndTime
			, TaskName
			, StaffFName
			, StaffLName
			, Credit
			, TaskIsPrintStart
			, TaskIsPrintEnd
			, IsCred
			, IsLocked
			, IsPublished
			, IsStruck
			, Notes
			, NULL AS ETLCommand
		FROM """ + prodTableName + """
		WHERE ScheduleDate BETWEEN '""" + str(startDate) + "' AND '" + str(endDate) + """';
	""")
	rowsCore = cursorCore.fetchall()
	rowsReturned = len(rowsCore)

	if rowsReturned > 0:
		log = "(" + str(datetime.today()) + f")  Retrieved {str(rowsReturned)} records from ProdServer\n"
		print(log)
		listLog.append(log)

		log = "(" + str(datetime.today()) + f")  Preparing ETLServer for records.  Submitting TRUNCATE command.  Target: [{stageTableName}]\n"
		print(log)
		listLog.append(log)
		cursorETL.execute("TRUNCATE TABLE "+ stageTableName + ";")
		cursorETL.commit()

		log = "(" + str(datetime.today()) + f")  Inserting records retrieved from ProdServer.  Target: [{stageTableName}]\n"
		print(log)
		listLog.append(log)
		sql = """
			INSERT INTO """ + stageTableName + """
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
		"""
		cursorETL.executemany(sql, rowsCore)
		cursorETL.commit()
		
		log = "(" + str(datetime.today()) + ")  Retrieval of records from ProdServer complete\n\n"
		print(log)
		listLog.append(log)
	else:
		log = "(" + str(datetime.today()) + f")  No records available from [{prodTableName}] within the past 30 days\n"
		print(log)
		listLog.append(log)


	# BLOCK 04 | Consolidate and stage records
	#
	#	- This block calls a USP that performs several tasks
	#		1) Identifies and removes struck records from [stage.Schedule]
	#		2) Inserts new records found in [import.Schedule] to [stage.Schedule]
	#		3) Searches for newer values of existing records and updates
	#			- StartTime
	#			- EndTime
	#			- Notes

	log = "\n\nETL PHASE: Staging records on ETLServer\n\n"
	print(log)
	listLog.append(log)

	log = "(" + str(datetime.today()) + ")  Staging SCHEDULE data by calling [import.usp_DoStagingSchedule]\n"
	print(log)
	listLog.append(log)

	cursorETL.execute("{CALL import.usp_DoStagingSchedule}")
	# USP is defined in \QGenda-Data-Mart\pipeline\sql\Schedule\USP import,usp_DoStagingSchedule.sql
	cursorETL.commit()

	cursorETL.execute("SELECT COUNT(*) AS RecordCount FROM " + stageTableName + " WHERE ETLCommand = 'New';")
	rowETL = cursorETL.fetchone()
	recordCount = rowETL.RecordCount
	log = f"{logSpacer})  Found {str(recordCount)} records flagged as New\n"
	listLog.append(log)

	cursorETL.execute("SELECT COUNT(*) AS RecordCount FROM " + stageTableName + " WHERE ETLCommand = 'Update';")
	rowETL = cursorETL.fetchone()
	recordCount = rowETL.RecordCount
	log = f"{logSpacer})  Found {str(recordCount)} records flagged for Update\n"
	listLog.append(log)

	cursorETL.execute("SELECT COUNT(*) AS RecordCount FROM " + stageTableName + " WHERE ETLCommand = 'Delete';")
	rowETL = cursorETL.fetchone()
	recordCount = rowETL.RecordCount
	log = f"{logSpacer})  Found {str(recordCount)} records flagged for Deletion\n"
	listLog.append(log)	

	log = "(" + str(datetime.today()) + ")  Staging complete.\n"
	print(log)
	listLog.append(log)


	# BLOCK 05 | Push staged records to production
	#
	#	- Identifies and deletes records in ProdServer that have been flagged for
	# 		deletion or update in ProdServer
	#	- Identifies and copies records in ETLServer to ProdServer that have been 
	#		flagged as new or for update

	log = "\n\nETL PHASE: Pushing staged records to Department DW\n\n"
	print(log)
	listLog.append(log)

	log = "(" + str(datetime.today()) + f")  Identifying records flagged for update or deletion.  Target: [ProdServer.QGenda.{stageTableName}]\n"
	print(log)
	listLog.append(log)

	cursorETL.execute("""
		SELECT DISTINCT
			ScheduleKey
			, TaskShiftKey
			, StaffKey
			, TaskKey
			, ScheduleDate
			, StartDate
			, StartTime
			, EndDate
			, EndTime
			, TaskName
			, StaffFName
			, StaffLName
			, Credit
			, TaskIsPrintStart
			, TaskIsPrintEnd
			, IsCred
			, IsLocked
			, IsPublished
			, IsStruck
			, Notes
		FROM """ + stageTableName + """
		WHERE ETLCommand IN ('Update', 'Delete')
		ORDER BY ScheduleDate, StartTime;
	""")
	rowsETL = cursorETL.fetchall()
	rowsReturned = len(rowsETL)

	if rowsReturned > 0:
		log = "(" + str(datetime.today()) + f")  Deleting {str(rowsReturned)} records flagged for deletion or update.  Target: [Department DW.QGenda.{prodTableName}]\n"
		print(log)
		listLog.append(log)

		i = 1
		for row in rowsETL:
			i += 1
			if (i % 1000) == 0:
				print("Deleting records ...\n")
			key = row.ScheduleKey
			cursorCore.execute("DELETE FROM " + prodTableName + " WHERE ScheduleKey = ?", key)
		cursorCore.commit()
		log = "(" + str(datetime.today()) + ")  Records successfully deleted\n\n"
		print(log)
		listLog.append(log)
	else:
		log = "(" + str(datetime.today()) + f")  No record deletion required\n\n"
		print(log)
		listLog.append(log)
	
	log = "(" + str(datetime.today()) + f")  Identifying new and updated records.  Target: [ProdServer.QGenda.{stageTableName}]\n"
	print(log)
	listLog.append(log)

	cursorETL.execute("""
		SELECT DISTINCT
			ScheduleKey
			, TaskShiftKey
			, StaffKey
			, TaskKey
			, ScheduleDate
			, StartDate
			, StartTime
			, EndDate
			, EndTime
			, TaskName
			, StaffFName
			, StaffLName
			, Credit
			, TaskIsPrintStart
			, TaskIsPrintEnd
			, IsCred
			, IsLocked
			, IsPublished
			, IsStruck
			, Notes
		FROM """ + stageTableName + """
		WHERE ETLCommand IN ('New', 'Update')
		ORDER BY ScheduleDate, StartTime;
	""")
	rowsETL = cursorETL.fetchall()
	rowsReturned = len(rowsETL)

	if rowsReturned > 0:
		log = "(" + str(datetime.today()) + f")  Inserting {str(rowsReturned)} records flagged as new or updated.  Target: [ProdServer.QGenda.{prodTableName}]\n"
		print(log)
		listLog.append(log)

		sql = """
			INSERT INTO """ + prodTableName + """
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
		"""
		cursorCore.executemany(sql, rowsETL)
		cursorCore.commit()

		log = "(" + str(datetime.today()) + ")  Staged SCHEDULE records successfully written to ProdServer\n"
		print(log)
		listLog.append(log)
	else:
		log = "(" + str(datetime.today()) + ")  No new or updated records identified, no transfer required\n"
		print(log)
		listLog.append(log)
	
	processEnd = datetime.today()

	# BLOCK 06 | Write log and clean up
	#
	#	- Close ODBC connections
	#	- Complete logging
	#	- Return data

	listLog.append("\n\nETL PHASE: Process clean-up\n\n")
	ETL.close()
	Core.close()

	processDuration = processEnd - processStart
	
	log = f"End process timestamp: {str(processEnd)}\n"
	print(log)
	listLog.append(log)

	log = f"Process duration: {str(processDuration)}\n"
	print(log)
	listLog.append(log)
	
	log = "Schedule.getSchedule() complete\n\n"
	print(log)
	listLog.append(log)	
	
	return 200, listLog
# END OF FILE