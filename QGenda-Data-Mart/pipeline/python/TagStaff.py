#   FILE HEADER
#       File Name:  TagStaff.py
#       Author:     Matt Chansard
#       Project:    QGenda Data Mart
#   
#   DESCRIPTION
#		This python script defines a function which acquires QGenda Staff Member Tag records via the
#		QGenda REST API.  This file can only execute successfully in environments with ODBC 
#		connections  defined  that match the DSNs in the pyodbc connection strings.
#
#   QGenda REST API (https://restapi.qgenda.com/)
#   Endpoint: StaffMember (https://restapi.qgenda.com/#ccabfe64-2cfa-488b-901b-28fcac33939e)
#

import os
import pyodbc
import requests
import sys
from datetime import date, datetime, timedelta

def getStaffTags(token):
	# BLOCK 01 | Initialization
	#
	#	- Initialize ODBC connections
	#	- Store names of SQL database objects
	#	- Create log

	processStart = datetime.today()

	importTableName = "import.qdm_TaggedStaff"
	stageTableName = "stage.qdm_TaggedStaff"
	prodTableName = "dim.TaggedStaff"

	listLog = ["TagStaff.getStaffTags commencing\n", f"Process Start Timestamp: {str(processStart)}\n"]
	listLog.append("ETL PHASE: Initialization\n")

	ETL = pyodbc.connect('DSN=ETL1')
	Core = pyodbc.connect('DSN=Core;Database=QGenda')
	listLog.append("ODBC Connections initialized")


	# BLOCK 02 | Retreving data from Staff Member Endpoint
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
	endPointURL = "/staffmember?includes=Tags"
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
	jsonFile = "TaggedStaff_" + str(date.today()) + ".json"
	with open (jsonPath + jsonFile, "w") as file:
		file.write(response.text)

	log = "(" + str(datetime.today()) + ")  Received STAFF MEMBER TAG data from QGenda API\n"
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

	cursorETL = ETL.cursor()
	cursorETL.execute("{CALL import.usp_AppliedTagsAPI (?, ?)}", "Staff", response.text)
	# USP is defined in \QGenda-Data-Mart\pipeline\sql\Tagged Records\USP import,usp_AppliedTagsAPI.sql
	cursorETL.commit()

	log = "(" + str(datetime.today()) + ")  Data successfully transferred to ETLServer.\n"
	print(log)
	listLog.append(log)


	# BLOCK 03 | Retrieving data from ProdServer
	#
	#	- This block retrieves records from ProdServer and inserts them into [stage.TaggedStaff]

	log = "\n\nETL PHASE: Data retrieval from ProdServer\n"
	print(log)
	listLog.append(log)

	log = "(" + str(datetime.today()) + f")  Retrieving records from ProdServer. Target: [{prodTableName}]\n"
	print(log)
	listLog.append(log)

	cursorCore = Core.cursor()
	cursorCore.execute("""
		SELECT
			StaffKey
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

			, CUH_CUH10hr
			, CUH_CUH13hr
			
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
			, Division_PHHSOBCUHCore
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
			
			, MDSimulation_SIM

			, ND_DAY
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
			
			, StaffPrimaryLocation_CUH
			, StaffPrimaryLocation_Zale
			
			, TTCMMockPunch_MDs
			, NULL AS ETLCommand
		FROM """ + prodTableName + """;
	""")

	rowsCore = cursorCore.fetchall()
	rowsReturned = len(rowsCore)

	if rowsReturned > 0:
		log = "(" + str(datetime.today()) + f")  Retrieved {str(rowsReturned)} records from ProdServer\n\n"
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
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
		# Column Counter                           10                            20                            30                            40                            50                             60                             70 
		cursorETL.executemany(sql, rowsCore)
		cursorETL.commit()  

		log = "(" + str(datetime.today()) + ")  Retrieval of records from ProdServer complete\n"
		print(log)
		listLog.append(log)
	else:
		log = "(" + str(datetime.today()) + f")  No records available from [{prodTableName}]\n"
		print(log)
		listLog.append(log)


	# BLOCK 04 | Consolidate and stage records
	#
	#	- This block calls a USP that performs several tasks
	#		1) Inserts new records, those found in the import that do not yet exist in ProdServer
	#		2) Searches for newer values of existing records and performs updates
	#		3) Identifies deactivated/archived Staff?

	log = "\n\nETL PHASE: Staging records on ETLServer\n\n"
	print(log)
	listLog.append(log)

	log = "(" + str(datetime.today()) + f")  Staging STAFF MEMBER TAG data by calling [import.usp_DoStagingStaffTags]\n"
	print(log)
	listLog.append(log)

	cursorETL.execute("{CALL import.usp_DoStagingStaffTags}")
	# USP is defined in \QGenda-Data-Mart\pipeline\sql\Tagged Records\TaggedStaff\USP import,import.usp_DoStagingStaffTags.sql
	cursorETL.commit()

	log = "(" + str(datetime.today()) + f")  Staging of Staff Tag records complete\n"
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
			StaffKey
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
			
			, CUH_CUH10hr
			, CUH_CUH13hr

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
			, Division_PHHSOBCUHCore
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
			
			, MDSimulation_SIM

			, ND_DAY
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
			
			, StaffPrimaryLocation_CUH
			, StaffPrimaryLocation_Zale
			
			, TTCMMockPunch_MDs
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
			key = row.StaffKey
			cursorCore.execute("DELETE from " + prodTableName + " WHERE StaffKey = ?", key)
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
			StaffKey
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

			, CUH_CUH10hr
			, CUH_CUH13hr
			
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
			, Division_PHHSOBCUHCore
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
			
			, MDSimulation_SIM

			, ND_DAY
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
			
			, StaffPrimaryLocation_CUH
			, StaffPrimaryLocation_Zale
			
			, TTCMMockPunch_MDs
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
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
		# Column Counter                           10                            20                            30                             40                             50                             60                      68
		cursorCore.executemany(sql, rowsETL)
		cursorCore.commit()

		log = "(" + str(datetime.today()) + f")  Staged STAFF MEMBER TAG records successfully written to ProdServer\n"
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
	
	log = "TagStaff.GetStaffTags() complete\n\n"
	print(log)
	listLog.append(log)

	return 200, listLog

# END OF FILE