#   FILE HEADER
#       File Name:  StaffMember.py
#       Author:     Matt C
#       Project:    QGenda Data Mart
#   
#   DESCRIPTION
#       This python script defines a function which acquires QGenda Schedule records via the QGenda
#       REST API.  This file can only execute successfully in environments with ODBC connections 
#       defined  that match the DSNs in the pyodbc connection strings.
#
#	QGenda REST API (https://restapi.qgenda.com/)
#   Endpoint: StaffMember (https://restapi.qgenda.com/#ccabfe64-2cfa-488b-901b-28fcac33939e)
#

import os
import pyodbc
import requests
from datetime import date, datetime, timedelta


def getStaffMember(token):
	# BLOCK 01 | Initialization
	#
	#	- Initialize ODBC connections
	#	- Configuration for connecting to QGenda REST API
	#	- Request and store access token

	processStart = datetime.today()

	importTableName = "import.qdm_StaffMember"
	stageTableName = "stage.qdm_StaffMember"
	prodTableName = "dim.StaffMember"

	listLog = ["StaffMember.getStaffMember() commencing\n", f"Process Start Timestamp: {str(processStart)}\n"]
	listLog.append("ETL PHASE: Initialization\n")

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
	print(log)
	listLog.append(log)

	rootURL = "https://api.qgenda.com/v2"
	endPointURL = "/staffmember?&$select=StaffKey,StaffId,Abbrev,StaffTypeKey,UserProfileKey,PayrollId,EmrId,Npi,FirstName,LastName,StartDate,EndDate,MobilePhone,Pager,Email,DeactivationDateUtc,UserLastLoginDateTimeUtc,SourceOfLogin&$orderby=LastName,FirstName"
	getURL = rootURL + endPointURL
	payload = {}
	headers = {
		'Authorization': f'bearer {token}'
	}

	log = "(" + str(datetime.today()) + ")  Requesting data from QGenda API.\n"
	print(log)
	listLog.append(log)

	response = requests.get(getURL, headers=headers, data=payload)

	jsonPath = os.path.join("C:\\", "Users", "Public", "ANES ETL", "QGenda Data Mart", "json", "")
	jsonFile = "StaffMember_" + str(date.today()) + ".json"
	with open(jsonPath + jsonFile, "w") as file:
		file.write(response.text)

	log = "(" + str(datetime.today()) + ")  Received STAFF MEMBER data from QGenda API\n\n"
	print(log)
	listLog.append(log)

	log = "(" + str(datetime.today()) + f")  Submitting TRUNCATE command.  Target table: [ETLServer.StagingQGenda.{importTableName}]\n"
	print(log, end="\n")
	listLog.append(log)

	cursorETL = ETL.cursor()
	cursorETL.execute("TRUNCATE TABLE " + importTableName + ";")
	cursorETL.commit()

	log = "(" + str(datetime.today()) + f")  Inserting records.  Target table: [ETLServer.StagingQGenda.{importTableName}]\n"
	print(log)
	listLog.append(log)

	cursorETL.execute("{Call import.usp_StaffMemberAPI (?)}", response.text)
	# USP is defined in \QGenda-Data-Mart\pipeline\sql\Staff Member\USP import,usp_StaffMemberAPI.sql
	cursorETL.commit()

	log = "(" + str(datetime.today()) + ")  Data successfully transferred to ETLServer.\n"
	print(log)
	listLog.append(log)


	# BLOCK 03 | Retrieving data from ProdServer
	#
	#	- This block retrieves records from ProdServer.  NOTE: No refresh window since not appropriate for Staff records
	#	- [stage.StaffMember] is truncated in preparation for the new records
	#	- The records retrieved from ProdServer are inserted into [stage.StaffMember]
	#	- The records retrieved from ProdServer are deleted from [ProdServer.dim.StaffMember] to prevent
	#		duplicate records from existing after the new staged records have been inserted

	log = "\n\nETL PHASE: Data retrieval from ProdServer\n\n"
	print(log)
	listLog.append(log)

	log = "(" + str(datetime.today()) + f")  Retrieving records from ProdServer.  Target table: [{prodTableName}]\n"
	print(log)
	listLog.append(log)

	cursorCore = Core.cursor()
	cursorCore.execute("""
		SELECT
			StaffKey
			, StaffId
			, StaffAbbrev
			, StaffTypeKey
			, UserProfileKey
			, PayrollId
			, EmrId
			, Npi
			, FirstName
			, LastName
			, StartDate
			, EndDate
			, MobilePhone
			, Pager
			, Email
			, IsActive
			, DeactivationDate
			, UserLastLoginDateTimeUTC
			, SourceOfLogin
			, 'N' AS PushToProductionFlag
		FROM """ + prodTableName + """;
	""")
	rowsCore = cursorCore.fetchall()
	rowsReturned = len(rowsCore)
	log = "(" + str(datetime.today()) + f")  Retrieved {str(rowsReturned)} records from ProdServer\n"
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
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
		"""
		cursorETL.executemany(sql, rowsCore)
		cursorETL.commit()

		log = "(" + str(datetime.today()) + ")  Retrieval of records from ProdServer complete\n\n"
		print(log)
		listLog.append(log)
	else:
		log = "(" + str(datetime.today()) + f")  No records found in [{prodTableName}]\n"
		print(log, end="\n")
		listLog.append(log)


	# BLOCK 04 | Consolidate and stage records
	#
	#	- This block calls a USP that performs several tasks
	#		1) Identifies newly deactivated Staff and updates records accordingly
	#		2) Inserts new records found in [import.StaffMember] to [stage.StaffMember]
	#		3) Searches for newer values of existing records and updates
	#			- UserLastLoginDateTimeUTC
	#			- SourceOfLogin

	log = "\n\nETL PHASE: Staging Staff Member records\n\n"
	print(log, end="\n")
	listLog.append(log)

	log = "(" + str(datetime.today()) + f")  Staging STAFF MEMBER data by calling [import.usp_DoStagingStaffMember]\n"
	print(log)
	listLog.append(log)

	cursorETL.execute("{CALL import.usp_DoStagingStaffMember}")
	# USP is defined in \QGenda-Data-Mart\pipeline\sql\Staff Member\USP import,usp_DoStagingStaffMember.sql
	cursorETL.commit()

	log = "(" + str(datetime.today()) + f")  Staging complete\n"
	print(log)
	listLog.append(log)


	# BLOCK 05 | Push staged records to production
	#
	#	- Transfers the freshly staged Staff Member records to ProdServer

	log = "\nETL PHASE: Pushing staged records to ProdServer\n"
	print(log, end="\n")
	listLog.append(log)

	# Source: [ETLServer.StagingQGenda.stage.StaffMember]
	# Destination: [ProdServer.QGenda.dbo.StaffMember]\n"

	log = "(" + str(datetime.today()) + f")  Selecting records flagged for transfer.  Target: [ETLServer.StagingQGenda.{stageTableName}]\n"
	print(log)
	listLog.append(log)

	cursorETL.execute("""
		SELECT DISTINCT
			StaffKey
			, StaffId
			, StaffAbbrev
			, StaffTypeKey
			, UserProfileKey
			, PayrollId
			, EmrId
			, Npi
			, FirstName
			, LastName
			, StartDate
			, EndDate
			, MobilePhone
			, Pager
			, Email
			, IsActive
			, DeactivationDate
			, UserLastLoginDateTimeUTC
			, SourceOfLogin
		FROM """ + stageTableName + """
		WHERE PushToProductionFlag = 'T'
		ORDER BY LastName, FirstName;
	""")
	rowsETL = cursorETL.fetchall()
	rowsReturned = len(rowsETL)

	if rowsReturned > 0:
		log = "(" + str(datetime.today()) + ")  Deleting records that will be replaced with a staged record.  Target: [ProdServer.QGenda.dim.StaffMember]\n"
		print(log)
		listLog.append(log)

		i = 1
		for row in rowsETL:
			i += 1
			if (i % 100) == 0:
				print("Deleting records ...\n")
			key = row.StaffKey
			cursorCore.execute("DELETE FROM " + prodTableName + " WHERE StaffKey = ?", key)
		cursorCore.commit()

		log = "(" + str(datetime.today()) + ")  Records successfully deleted\n\n"
		print(log)
		listLog.append(log)

		log = "(" + str(datetime.today()) + f")  Transferring staged records.  Target: [ProdServer.QGenda.{prodTableName}]\n"
		sql = """
			INSERT INTO """ + prodTableName + """
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
		# Column Counter                           10                         19
		cursorCore.executemany(sql, rowsETL)
		cursorCore.commit()

		log = "(" + str(datetime.today()) + f")  Staged Staff Member records successfully transferred\n"
		print(log, end="\n")
		listLog.append(log)
	else:
		log = "(" + str(datetime.today()) + f")  No changes to Staff Member records detected.\n"
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

	log = "StaffMember.getStaffMember() complete\n\n"
	print(log)
	listLog.append(log)

	return 200, listLog

# END OF FILE