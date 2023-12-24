#   FILE HEADER
#       File Name:  TagTask.py
#       Author:     Matt Chansard
#       Project:    QGenda Data Mart
#   
#   DESCRIPTION
#		This python script defines a function which acquires QGenda Task Tag records via the QGenda REST
#		API.  This file can only execute successfully in environments with ODBC connections defined that
#		match the DSNs in the pyodbc connection strings.
#
#	QGenda REST API (https://api.qgenda.com/v2/login)
#   Endpoint: Task (https://restapi.qgenda.com/#9ba04da9-3a43-4742-b812-14d49d4941dd)
#

import json
import os
import pyodbc
import requests
import sys
from datetime import date, datetime, timedelta

def getTaskTags(token):
	# BLOCK 01 | Initialization
	#
	#	- Initialize ODBC connections
	#	- Store names of SQL database objects
	#	- Create logging object

	processStart = datetime.today()

	listLog = ["TagTask.getTaskTags commencing\n", f"Process Start Timestamp: {str(processStart)}\n"]
	listLog.append("ETL PHASE: Initialization\n")

	importTableName = "import.qdm_TaggedTask"
	stageTableName = "stage.qdm_TaggedTask"
	prodTableName = "dim.TaggedTask"

	ETL = pyodbc.connect('DSN=ETL1')
	Core = pyodbc.connect('DSN=Core;Database=QGenda')
	listLog.append("ODBC Connections initialized")


	# BLOCK 02 | Retreving data from Task Endpoint
	#
	#	- Prepare request parameters
	#   - Submit request to QGenda API
	#	- Truncates (clear) appropriate table
	#	- Store data in ETLServer


	log = "\n\nETL PHASE: Data retrieval from QGenda API\n\n"
	print(log)
	listLog.append(log)

	log = "(" + str(datetime.today()) + ")  Preparing API request\n"
	print(log)
	listLog.append(log)

	rootURL = "https://api.qgenda.com/v2"
	endPointURL = "/task/?includes=Tags"
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
	jsonFile = "TaggedTask_" + str(date.today()) + ".json"
	with open (jsonPath + jsonFile, "w") as file:
		file.write(response.text)

	log = "(" + str(datetime.today()) + ")  Received TASK TAG data from QGenda API\n"
	print(log)
	listLog.append(log)

	log = "(" + str(datetime.today()) + f")  Submitting TRUNCATE command.  Target table: [ETLServer.StagingQGenda.{importTableName}]\n"
	print(log)
	listLog.append(log)

	cursorETL = ETL.cursor()
	cursorETL.execute("TRUNCATE TABLE " + importTableName + ";")
	cursorETL.commit()

	cursorETL = ETL.cursor()
	cursorETL.execute("{CALL import.usp_AppliedTagsAPI (?, ?)}", "Task", response.text)
	# USP is defined in \QGenda-Data-Mart\pipeline\sql\Tagged Records\USP import,usp_AppliedTagsAPI.sql
	cursorETL.commit()

	log = "(" + str(datetime.today()) + ")  Data successfully transferred to ETLServer.\n"
	print(log)
	listLog.append(log)


	# BLOCK 03 | Retrieving data from ProdServer
	#
	#	- This block retrieves records from ProdServer and inserts them into [stage.TaggedTags]

	log = "\n\nETL PHASE: Data retrieval from ProdServer\n"
	print(log)
	listLog.append(log)

	log = "(" + str(datetime.today()) + f")  Retrieving records from ProdServer. Target: [{prodTableName}]\n"
	print(log)
	listLog.append(log)

	cursorCore = Core.cursor()
	cursorCore.execute("""
		SELECT
			TaskKey
			, InvalidRecordFlag
			
			, CALevel_CA1
			, CALevel_CA2
			, CALevel_CA3
			, CALevel_Intern
			, CALevel_PGY4
			, CALevel_PGY5
			, CALevel_PGY6

			, Capacity_Cardiothoracic
			
			, CRNAType_FT
			, CRNAType_PRN
			
			, Division_Cardiothoracic
			, Division_CriticalCare
			, Division_CUHGeneralALL
			, Division_CUHGeneralPrimary
			, Division_CUHOB
			, Division_Liver
			, Division_Neuro
			, Division_OSCPrimary
			, Division_Pain
			, Division_Pediatrics
			, Division_PHHSGeneral
			, Division_PHHSOBHybrid
			, Division_PHHSOBPrimary
			, Division_PHHSRegional
			, Division_UHRegional
			, Division_ZaleCall
			
			, EmployeeType_APP
			, EmployeeType_FacultyFullTime
			, EmployeeType_FacultyPartTime
			, EmployeeType_FacultyPTNB
			, EmployeeType_FacultyTasks
			, EmployeeType_NonClinicalTime
			, EmployeeType_Trainee
			, EmployeeType_UTStaff
			
			, Integrations_Kronos
			
			, Location_CUH
			, Location_CUHCardiac
			, Location_CUHGeneral
			, Location_CUHNeuro
			, Location_FellowVacation
			, Location_ICU
			, Location_PainRoles
			, Location_PHHS
			, Location_UH
			, Location_UHOSC
			, Location_VA
			, Location_Zale
			
			, ND_Day
			, ND_Night
			
			, PrimarySite_PHHS
			, PrimarySite_UH
			
			, ProviderType_CRNA
			, ProviderType_Fellow
			, ProviderType_NP
			, ProviderType_PA
			, ProviderType_Physician
			, ProviderType_Resident
			, ProviderType_RRNA
			, ProviderType_UTStaff

			, QGendaAdminTags_Header
			, QGendaAdminTags_LBL

			, ShiftLength_8hr
			, ShiftLength_10hr
			, ShiftLength_11hr
			, ShiftLength_12hr
			, ShiftLength_13hr
			, ShiftLength_14hr
			, ShiftLength_16hr
			, ShiftLength_24hr
			
			, SystemTaskType_Unavailable
			, SystemTaskType_Working
			
			, TaskGrouping_AIC
			, TaskGrouping_CIC
			, TaskGrouping_Clinic
			, TaskGrouping_ICU
			, TaskGrouping_OR
			, TaskGrouping_Procedure
			, TaskGrouping_Telemedicine
			
			, TaskType1_Away
			, TaskType1_Call
			, TaskType1_Label
			, TaskType1_NonClinical
			, TaskType1_Working
			, NULL AS ETLCommand
		FROM """ + prodTableName + """
	""")
	rowsCore = cursorCore.fetchall()
	rowsReturned = len(rowsCore)

	if rowsReturned > 0:
		log = "(" + str(datetime.today()) + f")  Retrieved {str(rowsReturned)} records from ProdServer\n"
		print(log)
		listLog.append(log)

		log = "(" + str(datetime.today()) + f")  Preparing ETLServer for records.  Submitting TRUNCATE command.  Target table: {stageTableName}]\n"
		print(log)
		listLog.append(log)
		cursorETL.execute("TRUNCATE TABLE " + stageTableName + ";")
		cursorETL.commit()

		log = "(" + str(datetime.today()) + f")  Inserting records retrieved from ProdServer.  Target table: {stageTableName}]\n"
		print(log)
		listLog.append(log)
	
		sql = """
			INSERT INTO """ + stageTableName + """
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
		# Column Counter                           10                            20                            30                            40                            50                            60                            70                            80             85  
		cursorETL.executemany(sql, rowsCore)
		cursorETL.commit()  
	else:
		log = "(" + str(datetime.today()) + f")  No records available from [{prodTableName}]\n"
		print(log)
		listLog.append(log)

	# BLOCK 04 | Consolidate and stage records
	#
	#	- This block calls a USP that performs several tasks
	#		1) Inserts new records, those found in the import that do not yet exist in ProdServer
	#		2) Searches for newer values of existing records and performs updates
	#		3) Identifies deactivated/archived Tasks?

	log = "\n\nETL PHASE: Staging Task records\n"
	print(log)
	listLog.append(log)

	log = "(" + str(datetime.today()) + f")  Staging TASK TAG data by calling [import.usp_DoStagingTaskTags]\n"
	print(log)
	listLog.append(log)

	cursorETL.execute("{CALL import.usp_DoStagingTaskTags}")
	# USP is defined in \QGenda-Data-Mart\pipeline\sql\Tagged Records\Tagged Task\USP import,usp_DoStagingTaskTags.sql
	cursorETL.commit()

	log = "(" + str(datetime.today()) + f")  Staging of Task Tag records complete\n"
	print(log)
	listLog.append(log)


	# BLOCK 05 | Push staged records to production
	#
	#	- Transfers the freshly staged Task records to ProdServer

	log = "\nETL PHASE: Pushing staged records to ProdServer\n"
	print(log)
	listLog.append(log)

	log = "(" + str(datetime.today()) + f")  Identifying records flagged for update only.  Deletion not part of current ETL process.  Target: [ProdServer.QGenda.{stageTableName}]\n"
	print(log)
	listLog.append(log)
	cursorETL.execute("""
		SELECT
			TaskKey
			, InvalidRecordFlag
			
			, CALevel_CA1
			, CALevel_CA2
			, CALevel_CA3
			, CALevel_Intern
			, CALevel_PGY4
			, CALevel_PGY5
			, CALevel_PGY6

			, Capacity_Cardiothoracic
			
			, CRNAType_FT
			, CRNAType_PRN
			
			, Division_Cardiothoracic
			, Division_CriticalCare
			, Division_CUHGeneralALL
			, Division_CUHGeneralPrimary
			, Division_CUHOB
			, Division_Liver
			, Division_Neuro
			, Division_OSCPrimary
			, Division_Pain
			, Division_Pediatrics
			, Division_PHHSGeneral
			, Division_PHHSOBHybrid
			, Division_PHHSOBPrimary
			, Division_PHHSRegional
			, Division_UHRegional
			, Division_ZaleCall
			
			, EmployeeType_APP
			, EmployeeType_FacultyFullTime
			, EmployeeType_FacultyPartTime
			, EmployeeType_FacultyPTNB
			, EmployeeType_FacultyTasks
			, EmployeeType_NonClinicalTime
			, EmployeeType_Trainee
			, EmployeeType_UTStaff
			
			, Integrations_Kronos
			
			, Location_CUH
			, Location_CUHCardiac
			, Location_CUHGeneral
			, Location_CUHNeuro
			, Location_FellowVacation
			, Location_ICU
			, Location_PainRoles
			, Location_PHHS
			, Location_UH
			, Location_UHOSC
			, Location_VA
			, Location_Zale
			
			, ND_Day
			, ND_Night
			
			, PrimarySite_PHHS
			, PrimarySite_UH
			
			, ProviderType_CRNA
			, ProviderType_Fellow
			, ProviderType_NP
			, ProviderType_PA
			, ProviderType_Physician
			, ProviderType_Resident
			, ProviderType_RRNA
			, ProviderType_UTStaff

			, QGendaAdminTags_Header
			, QGendaAdminTags_LBL

			, ShiftLength_8hr
			, ShiftLength_10hr
			, ShiftLength_11hr
			, ShiftLength_12hr
			, ShiftLength_13hr
			, ShiftLength_14hr
			, ShiftLength_16hr
			, ShiftLength_24hr
			
			, SystemTaskType_Unavailable
			, SystemTaskType_Working
			
			, TaskGrouping_AIC
			, TaskGrouping_CIC
			, TaskGrouping_Clinic
			, TaskGrouping_ICU
			, TaskGrouping_OR
			, TaskGrouping_Procedure
			, TaskGrouping_Telemedicine
			
			, TaskType1_Away
			, TaskType1_Call
			, TaskType1_Label
			, TaskType1_NonClinical
			, TaskType1_Working
		FROM """ + stageTableName + """
		WHERE ETLCommand = 'Update';
	""")
	rowsETL = cursorETL.fetchall()
	rowsReturned = len(rowsETL)

	if rowsReturned > 0:
		log = "(" + str(datetime.today()) + f")  Deleting {str(rowsReturned)} record flagged for update.  Target: [ProdServer.QGenda.{prodTableName}]\n"
		print(log)
		listLog.append(log)

		for row in rowsETL:
			key = row.TaskKey
			cursorCore.execute("DELETE FROM " + prodTableName + " WHERE TaskKey = ?", key)
		cursorCore.commit()

		log = "(" + str(datetime.today()) + f")  Record deletion complete\n"
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
		SELECT
			TaskKey
			, InvalidRecordFlag
			
			, CALevel_CA1
			, CALevel_CA2
			, CALevel_CA3
			, CALevel_Intern
			, CALevel_PGY4
			, CALevel_PGY5
			, CALevel_PGY6

			, Capacity_Cardiothoracic
			
			, CRNAType_FT
			, CRNAType_PRN
			
			, Division_Cardiothoracic
			, Division_CriticalCare
			, Division_CUHGeneralALL
			, Division_CUHGeneralPrimary
			, Division_CUHOB
			, Division_Liver
			, Division_Neuro
			, Division_OSCPrimary
			, Division_Pain
			, Division_Pediatrics
			, Division_PHHSGeneral
			, Division_PHHSOBHybrid
			, Division_PHHSOBPrimary
			, Division_PHHSRegional
			, Division_UHRegional
			, Division_ZaleCall
			
			, EmployeeType_APP
			, EmployeeType_FacultyFullTime
			, EmployeeType_FacultyPartTime
			, EmployeeType_FacultyPTNB
			, EmployeeType_FacultyTasks
			, EmployeeType_NonClinicalTime
			, EmployeeType_Trainee
			, EmployeeType_UTStaff
			
			, Integrations_Kronos
			
			, Location_CUH
			, Location_CUHCardiac
			, Location_CUHGeneral
			, Location_CUHNeuro
			, Location_FellowVacation
			, Location_ICU
			, Location_PainRoles
			, Location_PHHS
			, Location_UH
			, Location_UHOSC
			, Location_VA
			, Location_Zale
			
			, ND_Day
			, ND_Night
			
			, PrimarySite_PHHS
			, PrimarySite_UH
			
			, ProviderType_CRNA
			, ProviderType_Fellow
			, ProviderType_NP
			, ProviderType_PA
			, ProviderType_Physician
			, ProviderType_Resident
			, ProviderType_RRNA
			, ProviderType_UTStaff

			, QGendaAdminTags_Header
			, QGendaAdminTags_LBL

			, ShiftLength_8hr
			, ShiftLength_10hr
			, ShiftLength_11hr
			, ShiftLength_12hr
			, ShiftLength_13hr
			, ShiftLength_14hr
			, ShiftLength_16hr
			, ShiftLength_24hr
			
			, SystemTaskType_Unavailable
			, SystemTaskType_Working
			
			, TaskGrouping_AIC
			, TaskGrouping_CIC
			, TaskGrouping_Clinic
			, TaskGrouping_ICU
			, TaskGrouping_OR
			, TaskGrouping_Procedure
			, TaskGrouping_Telemedicine
			
			, TaskType1_Away
			, TaskType1_Call
			, TaskType1_Label
			, TaskType1_NonClinical
			, TaskType1_Working
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
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
		# Column Counter                           10                            20                            30                            40                            50                            60                            70                            80          84
		cursorCore.executemany(sql, rowsETL)
		cursorCore.commit()

		log = "(" + str(datetime.today()) + f")  Staged TASK TAG records successfully written to ProdServer\n"
		print(log)
		listLog.append(log)
	else:
		log = "(" + str(datetime.today()) + f")  No new or updated records identified, no transfer required\n"
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
	
	log = "TagTask.GetTaskTags() complete\n\n"
	print(log)
	listLog.append(log)

	return 200, listLog

# END OF FILE