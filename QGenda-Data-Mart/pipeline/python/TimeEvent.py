#   FILE HEADER
#       File Name:  TimeEvent.py
#       Author:     Matt Chansard
#       Project:    QGenda Data Mart
#   
#   DESCRIPTION
#       This python script defines a function which acquires QGenda Time Event records via the QGenda
#       REST API.  This file can only execute successfully in environments with ODBC connections 
#       defined  that match the DSNs in the pyodbc connection strings.
#
#	QGenda REST API (https://restapi.qgenda.com/)
#   Endpoint: TimeEvent (https://restapi.qgenda.com/#f61c3c47-8597-4f9e-92d5-f059c149dc2c)

import os
import pyodbc
import requests
from datetime import date, datetime, timedelta

def getTimeEvent(token, companyKey, startDate, endDate):
    # BLOCK 01 | Initialization
    #
    #	- Initialize ODBC connections
    #	- Configuration for connecting to QGenda REST API
    #	- Request and store access token

	processStart = datetime.today()
    
	importTableName = "import.qdm_TimeEvent"
	stageTableName = "stage.qdm_TimeEvent"
	prodTableName = "dbo.TimeEvent"

	listLog = ["TimeEvent.getTimeEvent() commencing\n", f"Process Start Timestamp: {str(processStart)}\n"]
	listLog.append("\n\nETL PHASE: Initialization\n\n")

	ETL = pyodbc.connect('DSN=ETL1')
	Core = pyodbc.connect('DSN=Core;Database=QGenda')
	listLog.append("ODBC connections opened\n")


    # BLOCK 02 | Retreving data from TimeEvent Endpoint
    #
    #	- Prepare request parameters
    #   - Submit request to QGenda API
    #	- Truncate (clear) [import.TimeEvent]
    #	- Store data in ETLServer

	log = "\n\nETL PHASE: Data retrieval from QGenda API\n\n"
	print(log)
	listLog.append(log)

	log = "(" + str(datetime.today()) + ")  Preparing API request\n"
	print(log, end="\n")
	listLog.append(log)

	fStartDate = startDate.strftime("%m/%d/%Y")
	fEndDate = endDate.strftime("%m/%d/%Y")
	rootURL = "https://api.qgenda.com/v2"
	endPointURL = f"/timeevent/?companyKey={companyKey}&startDate={fStartDate}&endDate={fEndDate}&$select=ScheduleEntryKey,TaskShiftKey,StaffKey,TaskKey,TimePunchEventKey,Date,DayOfWeek,ActualClockInLocal,EffectiveClockInLocal,ActualClockOutLocal,EffectiveClockOutLocal,Duration,IsStruck,IsEarly,IsLate,IsExcessiveDuration,IsExtended,IsUnplanned,FlagsResolved,Notes,LastModifiedDate"
	getURL = rootURL + endPointURL
	payload={}
	headers = {
		'Authorization': f'bearer {token}'
	}
        
	log = "(" + str(datetime.today()) + ")  Requesting data from QGenda API.\n"
	print(log)
	listLog.append(log)

	response = requests.get(getURL, headers=headers, data=payload)
	
	jsonPath = os.path.join("C:\\", "Users", "Public", "ANES ETL", "QGenda Data Mart", "json", "")
	jsonFile = "TimeEvent_" + str(date.today()) + ".json"
	with open (jsonPath + jsonFile, "w") as file:
		file.write(response.text)

	log = "(" + str(datetime.today()) + ")  Received TIME EVENT data from QGenda API\n\n"
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

	cursorETL.execute("{Call import.usp_TimeEventAPI (?)}", response.text)
	# USP is defined in \QGenda-Data-Mart\pipeline\sql\TimeEvent\USP import,usp_TimeEventAPI.sql
	cursorETL.commit()

	log = "(" + str(datetime.today()) + ")  Data successfully transferred to ETLServer.\n"
	print(log)
	listLog.append(log)


    # BLOCK 03 | Retrieving data from ProdServer
    #
    #	- This block retrieves records from ProdServer that fall into the refresh window of 30 days
    #	- [stage.TimeEvent] is truncated in preparation for the new records
    #	- The records retrieved from ProdServer are inserted into [stage.TimeEvent]
    #	- The records retrieved from ProdServer are deleted from [ProdServer.dbo.TimeEvent] to prevent
    #		duplicate records from existing after the new staged records have been inserted

	log = "\n\nETL PHASE: Data retrieval from ProdServer\n\n"
	print(log)
	listLog.append(log)

	log = "(" + str(datetime.today()) + f")  Retrieving records within refresh window from ProdServer.  Target table: [{prodTableName}]\n"
	print(log)
	listLog.append(log)

	cursorCore = Core.cursor()
	cursorCore.execute("""
        SELECT 
            ScheduleEntryKey
            , TaskShiftKey
            , StaffKey
            , TaskKey
            , TimePunchEventKey
            , TimeEventDate
            , TimeEventWeekday
            , ActualClockIn
            , EffectiveClockIn
            , ActualClockOut
            , EffectiveClockOut
            , Duration
            , IsStruck
            , IsEarly
            , IsLate
            , IsExcessiveDuration
            , IsExtended
            , IsUnplanned
            , FlagsResolved
            , Notes
            , LastModifiedDate
            , NULL AS ETLCommand
        FROM """ + prodTableName + """
        WHERE TimeEventDate BETWEEN '""" + str(startDate) + "' AND '" + str(endDate) + """';
    """)
	rowsCore = cursorCore.fetchall()
	rowsReturned = len(rowsCore)
	log = "(" + str(datetime.today()) + f")  Retrieved {str(rowsReturned)} records from ProdServer\n\n"
	print(log)
	listLog.append(log)

	if rowsReturned > 0:
		log = "(" + str(datetime.today()) + f")  Preparing ETLServer for records.  Submitting TRUNCATE command.  Target table: [{stageTableName}]\n"
		print(log)
		listLog.append(log)
		cursorETL.execute("TRUNCATE TABLE " + stageTableName + ";")
		cursorETL.commit()
        
		log = "(" + str(datetime.today()) + f")  Inserting records retrieved from ProdServer.  Target table: [{stageTableName}]\n"
		print(log)
		listLog.append(log)
		sql = """
			INSERT INTO """ + stageTableName + """
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		"""     # 22 columns
		cursorETL.executemany(sql, rowsCore)
		cursorETL.commit()

		log = "(" + str(datetime.today()) + ")  Retrieval of records from ProdServer complete\n\n"
		print(log)
		listLog.append(log)
	else:
		log = "(" + str(datetime.today()) + f")  No records available from {prodTableName}] within the past 30 days\n"
		print(log)
		listLog.append(log)


    # BLOCK 04 | Consolidate and stage records
    #
    #	- This block calls a USP that performs several tasks
    #		1) Identifies and removes struck records from [stage.TimeEvent]
    #		2) Inserts new records found in [import.TimeEvent] to [stage.TimeEvent]
    #		3) Searches for newer values of existing records and updates
    #			- Notes
    #			- LastModifiedDate

	log = "\n\nETL PHASE: Staging TimeEvent records\n\n"
	print(log, end="\n")
	listLog.append(log)

	log = "(" + str(datetime.today()) + ")  Staging TIME EVENT data by calling [import.usp_DoStagingTimeEvent]\n"
	print(log)
	listLog.append(log)

	cursorETL.execute("{CALL import.usp_DoStagingTimeEvent}")
	# USP is defined in \QGenda-Data-Mart\pipeline\sql\TimeEvent\USP import,usp_DoStagingTimeEvent.sql
	cursorETL.commit()

	log = "(" + str(datetime.today()) + ")  Staging complete.\n"
	print(log)
	listLog.append(log)


    # BLOCK 05 | Push staged records to production
    #
    #	- Transfers the freshly staged TimeEvent records to ProdServer
    #	- Close ODBC connections

	log = "\nETL PHASE: Pushing staged records to ProdServer\n"
	print(log, end="\n")
	listLog.append(log)

	log = "(" + str(datetime.today()) + f")  Identifying records flagged for update or deletion.  Target: [ProdServer.QGenda.{stageTableName}]\n"
	print(log)
	listLog.append(log)

	cursorETL.execute("""
		SELECT DISTINCT
			ScheduleEntryKey
			, TaskShiftKey
			, StaffKey
			, TaskKey
			, TimePunchEventKey
			, TimeEventDate
			, TimeEventWeekday
			, ActualClockIn
			, EffectiveClockIn
			, ActualClockOut
			, EffectiveClockOut
			, Duration
			, IsStruck
			, IsEarly
			, IsLate
			, IsExcessiveDuration
			, IsExtended
			, IsUnplanned
			, FlagsResolved
			, Notes
			, LastModifiedDate
		FROM """ + stageTableName + """
		WHERE ETLCommand IN ('Update', 'Delete')
		ORDER BY TimeEventDate, ActualClockIn
	""")
	rowsETL = cursorETL.fetchall()
	rowsReturned = len(rowsETL)

	if rowsReturned > 0:
		log = "(" + str(datetime.today()) + f")  Deleting {str(rowsReturned)} records flagged for deletion or update.  Target: [ProdServer.QGenda.{prodTableName}]\n"
		print(log)
		listLog.append(log)

		i = 1
		for row in rowsETL:
			i += 1
			if (i % 500) == 0:
				print("Deleting records ...\n")
			key = row.TimePunchEventKey
			cursorCore.execute("DELETE FROM " + prodTableName + " WHERE TimePunchEventKey = ?", key)
		cursorCore.commit()
		log = "(" + str(datetime.today()) + ")  Records successfully deleted\n\n"
		print(log)
		listLog.append(log)
	else:
		log = "(" + str(datetime.today()) + f")  No record deletion required\n\n"
		print(log)
		listLog.append(log)

	log = "(" + str(datetime.today()) + f")  Identifying new and updated records.  Target: [ProdServer.QGenda.{prodTableName}]\n"
	print(log)
	listLog.append(log)

	cursorETL.execute("""
		SELECT DISTINCT
			ScheduleEntryKey
			, TaskShiftKey
			, StaffKey
			, TaskKey
			, TimePunchEventKey
			, TimeEventDate
			, TimeEventWeekday
			, ActualClockIn
			, EffectiveClockIn
			, ActualClockOut
			, EffectiveClockOut
			, Duration
			, IsStruck
			, IsEarly
			, IsLate
			, IsExcessiveDuration
			, IsExtended
			, IsUnplanned
			, FlagsResolved
			, Notes
			, LastModifiedDate
		FROM """ + stageTableName + """
		WHERE ETLCommand IN ('New', 'Update')
		ORDER BY TimeEventDate, ActualClockIn
	""")
	rowsETL = cursorETL.fetchall()
	rowsReturned = len(rowsETL)

	if rowsReturned > 0:
		log = "(" + str(datetime.today()) + f")  Inserting records flagged as new or updated.  Target: [ProdServer.QGenda.{prodTableName}]\n"
		print(log)
		listLog.append(log)

		
		sql = """
			INSERT INTO """ + prodTableName + """
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
		# Column Counter                           10                            20
		
		cursorCore.executemany(sql, rowsETL)
		cursorCore.commit()

		log = "(" + str(datetime.today()) + ")  Staged TIME EVENT records successfully transferred\n"
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

	log = "TimeEvent.getTimeEvent() complete\n\n"
	print(log)
	listLog.append(log)

	return 200, listLog
# END OF FILE