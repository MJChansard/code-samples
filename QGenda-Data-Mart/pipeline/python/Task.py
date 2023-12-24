#   FILE HEADER
#       File Name:  Task.py
#       Author:     Matt Chansard
#       Project:    QGenda Data Mart
#   
#   DESCRIPTION
#       This python script defines a function which acquires QGenda Task records via the QGenda
#       REST API.  This file can only execute successfully in environments with ODBC connections 
#       defined  that match the DSNs in the pyodbc connection strings.
#
#	QGenda REST API (https://restapi.qgenda.com/)
#   Endpoint: Task (https://restapi.qgenda.com/#9ba04da9-3a43-4742-b812-14d49d4941dd)
#

import os
import pyodbc
import requests
import sys
from datetime import date, datetime, timedelta

def getTask(token):
	# BLOCK 01 | Initialization
	#
	#	- Initialize ODBC connections
	#	- Configuration for connecting to QGenda REST API
	#	- Request and store access token

	processStart = datetime.today()

	importTableName = "import.qdm_Task"
	stageTableName = "stage.qdm_Task"
	prodTableName = "dim.Task"

	listLog = ["Task.getTask() commencing\n", f"Process Start Timestamp: {str(processStart)}\n"]
	listLog.append("ETL PHASE: Initialization\n")

	ETL = pyodbc.connect('DSN=ETL1;Trusted_Connection=yes;')
	Core = pyodbc.connect('DSN=Core;Database=QGenda;Trusted_Connection=yes;')
	listLog.append("ODBC connections opened\n")


	# BLOCK 02 | Retreving data from Task Endpoint
	#
	#	- Prepare request parameters
	#   - Submit request to QGenda API
	#	- Truncate (clear) [import.Task]
	#	- Store data in ETLServer

	log = "\n\nETL PHASE: Data retrieval from QGenda API\n\n"
	print(log)
	listLog.append(log)

	log = "(" + str(datetime.today()) + ")  Preparing API request\n"
	print(log)
	listLog.append(log)

	rootURL = "https://api.qgenda.com/v2"
	endPointURL = "/task?&$select=TaskKey,Name,TaskId,Abbrev,Type,DepartmentId,EmrId,StartDate,EndDate,ContactInformation,Manual,RequireTimePunch,Notes&$orderby=Name"
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
	jsonFile = "Task_" + str(date.today()) + ".json"
	with open (jsonPath + jsonFile, "w") as file:
		file.write(response.text)

	log = "(" + str(datetime.today()) + ")  Received TASK data from QGenda API\n"
	print(log)
	listLog.append(log)


	log = "(" + str(datetime.today()) + f")  Submitting TRUNCATE command to [ETLServer.StagingQGenda.{importTableName}]\n"
	print(log)
	listLog.append(log)
	cursorETL = ETL.cursor()
	cursorETL.execute("TRUNCATE TABLE " + importTableName + ";")
	cursorETL.commit()

	log = "(" + str(datetime.today()) + f")  Inserting records.  Target table: [ETLServer.StagingQGenda.{importTableName}]\n"
	print(log)
	listLog.append(log)

	cursorETL.execute("{Call import.usp_TaskAPI (?)}", response.text)
	# USP is defined in \QGenda-Data-Mart\pipeline\sql\Task\USP import,usp_TaskAPI.sql
	cursorETL.commit()

	log = "(" + str(datetime.today()) + ")  Data successfully transferred to ETLServer.\n"
	print(log)
	listLog.append(log)


	# BLOCK 03 | Retrieving data from ProdServer
	#
	#	- This block retrieves all Task records from ProdServer
	#	- [stage.qdm_Task] is truncated in preparation for the new records
	#	- The records retrieved from ProdServer are inserted into [stage.qdm_Task]

	log = "\n\nETL PHASE: Data retrieval from ProdServer\n"
	print(log)
	listLog.append(log)

	log = "(" + str(datetime.today()) + f")  Retrieving records from ProdServer Target: [{prodTableName}]\n"
	print(log)
	listLog.append(log)

	cursorCore = Core.cursor()
	cursorCore.execute("""
		SELECT
			TaskKey
			, TaskName
			, TaskId
			, TaskAbbrev
			, TaskType
			, DepartmentId
			, EmrId
			, StartDate
			, EndDate
			, ContactInformation
			, IsManual
			, RequireTimePunch
			, Notes	
			, NULL AS ETLCommand
		FROM """ + prodTableName + """;
	""")
	rowsCore = cursorCore.fetchall()
	rowsReturned = len(rowsCore)

	log = "(" + str(datetime.today()) + f")  Retrieved {str(rowsReturned)} records from ProdServer\n"
	print(log)
	listLog.append(log)

	if rowsReturned > 0:
		log = "(" + str(datetime.today()) + f")  Preparing ETLServer for records.  Submitting TRUNCATE command.  Target table: {stageTableName}]\n"
		print(log)
		listLog.append(log)
		cursorETL.execute("TRUNCATE TABLE " + stageTableName + ";")
		
		log = "(" + str(datetime.today()) + f")  Inserting records retrieved from ProdServer.  Target table: {stageTableName}]\n"
		print(log)
		listLog.append(log)
		sql = """
			INSERT INTO """ + stageTableName + """
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);		"""
		# Column Counter                           10          14
		cursorETL.executemany(sql, rowsCore)
		cursorETL.commit()  
	else:
		log = "(" + str(datetime.today()) + f")  ERROR: No records found in [{prodTableName}]\n"
		print(log, end="\n")
		listLog.append(log)
		sys.exit()


	# BLOCK 04 | Consolidate and stage records
	#
	#	- This block calls a USP that performs several tasks
	#		1) Inserts newly created Tasks (those found in [import.Task] that do not exist in [stage.Task])
	#		2) Searches for newer values of existing records and performs updates
	#		3) Identifies deactivated/archived Tasks?

	log = "\n\nETL PHASE: Staging Task records\n"
	print(log, end="\n")
	listLog.append(log)

	log = "(" + str(datetime.today()) + f")  Staging Task data by calling [import.usp_DoStagingTask]\n"
	print(log)
	listLog.append(log)

	cursorETL.execute("{CALL import.usp_DoStagingTask}")
	# USP is defined in \QGenda-Data-Mart\pipeline\sql\Task\USP import,usp_DoStagingTask.sql
	cursorETL.commit()

	log = "(" + str(datetime.today()) + f")  Staging of Task records complete\n"
	print(log)
	listLog.append(log)


	# BLOCK 05 | Push staged records to production
	#
	#	- Transfers the freshly staged Task records to ProdServer

	log = "\nETL PHASE: Pushing staged records to ProdServer\n"
	print(log, end="\n")
	listLog.append(log)

	log = "(" + str(datetime.today()) + f")  Identifying records flagged for update only.  Deletion not part of current ETL process.  Target: [ProdServer.QGenda.{stageTableName}]\n"
	# Note: Only deletion that takes place is for records that have a staged replacement
	print(log)
	listLog.append(log)

	cursorETL.execute("""
		SELECT DISTINCT
			TaskKey
			, TaskName
			, TaskId
			, TaskAbbrev
			, TaskType
			, DepartmentId
			, EmrId
			, StartDate
			, EndDate
			, ContactInformation
			, IsManual
			, RequireTimePunch
			, Notes
		FROM """ + stageTableName + """
		WHERE ETLCommand = 'Update';
	""")
	rowsETL = cursorETL.fetchall()
	rowsReturned = len(rowsETL)

	if rowsReturned > 0:
		log = "(" + str(datetime.today()) + f")  Deleting records flagged for update.  Target: [ProdServer.QGenda.{prodTableName}]\n"
		print(log)
		listLog.append(log)

		for row in rowsETL:
			key = row.TaskKey
			cursorCore.execute("DELETE FROM " + prodTableName + " WHERE TaskKey = ?", key)
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
			TaskKey
			, TaskName
			, TaskId
			, TaskAbbrev
			, TaskType
			, DepartmentId
			, EmrId
			, StartDate
			, EndDate
			, ContactInformation
			, IsManual
			, RequireTimePunch
			, Notes
		FROM """ + stageTableName + """
		WHERE ETLCommand IN ('New', 'Update');
	""")
	rowsETL = cursorETL.fetchall()
	rowsReturned = len(rowsETL)

	if rowsReturned > 0:
		log = "(" + str(datetime.today()) + f")  Inserting {str(rowsReturned)} records flagged as new or updated.  Target: [ProdServer.QGenda.{prodTableName}]\n"
		print(log)
		listLog.append(log)

		sql = """
			INSERT INTO """ + prodTableName + """
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
		"""
		cursorCore.executemany(sql, rowsETL)
		cursorCore.commit()

		log = "(" + str(datetime.today()) + f")  Staged TASK records successfully written to ProdServer\n"
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
	#	- Write log to file

	listLog.append("\n\nETL PHASE: Process clean-up\n")
	ETL.close()
	Core.close()

	processDuration = processEnd - processStart
	
	log = f"End process timestamp: {str(processEnd)}\n"
	print(log)
	listLog.append(log)

	log = f"Process duration: {str(processDuration)}\n"
	print(log)
	listLog.append(log)
	
	log = "Task.getTask() complete\n\n"
	print(log)
	listLog.append(log)	

	return 200, listLog
# END OF FILE