#   FILE HEADER
#       File Name:  Tag.py
#       Author:     Matt Chansard
#       Project:    QGenda Data Mart
#   
#   DESCRIPTION
#       This python script defines a function which acquires QGenda Tag records from the Enterprise
#       Data Warehouse (EDW).  This file can only execute successfully in environments with ODBC connections 
#       defined  that match the DSNs in the pyodbc connection strings.
#

import os
import pyodbc
import requests
from datetime import date, datetime

def getTags(token, companyKey):
	# BLOCK 01 | Initialization
	#
	#	- Initialize ODBC connections
	#	- Store names for SQL database objects
	#	- Create log

	processStart = datetime.today()

	importTableName = "import.qdm_Tag"
	stageTableName = "stage.qdm_Tag"
	prodTableName = "dim.Tag"

	listLog = ["Tag.getTag() commencing\n", f"Process Start Timestamp: {str(processStart)}\n"]
	listLog.append("NOTE:  DATA SOURCE IS EDW, NOT QGENDA API\n")
	listLog.append("\n\nETL PHASE: Initialization\n\n")

	EDW = pyodbc.connect('DSN=QGendaMirror')
	ETL = pyodbc.connect('DSN=ETL1; Trusted_Connection=yes;')
	Core = pyodbc.connect('DSN=Core;Database=QGenda')
	listLog.append("ODBC connections opened")


	# BLOCK 02 | Retreving data EDW Tags Table
	#
	#	- Request data via QGenda API
	#	- Truncate (clear) [import.qdm_Tag]
	#	- Store data in ETLServer

	log = "(" + str(datetime.today()) + f")  Submitting TRUNCATE command.  Target table: [ETLServer.StagingQGenda.{importTableName}]\n"
	print(log)
	listLog.append(log)

	cursorETL = ETL.cursor()
	cursorETL.execute("TRUNCATE TABLE " + importTableName + ";")
	cursorETL.commit()

	log = "(" + str(datetime.today()) + ")  Getting data from EDW.  Target table: [anes.vw_STAGE_Tags]\n"
	print(log)
	listLog.append(log)

	cursorEDW = EDW.cursor()
	cursorEDW.execute("""
		SELECT
			CategoryKey
			, CategoryName
			, CategoryDateCreated		        = CAST(REPLACE(SUBSTRING(CategoryDateCreated, 1, 23), 'T', ' ') AS DATETIME)
			, CategoryDateModified		        = CAST(REPLACE(SUBSTRING(CategoryDateLastModified, 1, 23), 'T', ' ') AS DATETIME)
			, [Key]						        AS TagKey
			, [Name]					        AS TagName
			, TagDateCreated			        = CAST(REPLACE(SUBSTRING(DateCreated, 1, 23), 'T', ' ') AS DATETIME)
			, TagDateModified			        = CAST(REPLACE(SUBSTRING(DateLastModified, 1, 23), 'T', ' ') AS DATETIME) 
			, IsAvailableForCreditAllocation	= CASE WHEN IsAvailableForCreditAllocation = 'True' THEN 'T' ELSE 'F' END
			, IsAvailableForHoliday				= CASE WHEN IsAvailableForHoliday = 'True'			THEN 'T' ELSE 'F' END
			, IsAvailableForLocation			= CASE WHEN IsAvailableForLocation = 'True'			THEN 'T' ELSE 'F' END
			, IsAvailableForProfile				= CASE WHEN IsAvailableForProfile = 'True'			THEN 'T' ELSE 'F' END
			, IsAvailableForRequestLimit		= CASE WHEN IsAvailableForRequestLimit = 'True'		THEN 'T' ELSE 'F' END
			, IsAvailableForScheduleEntry		= CASE WHEN IsAvailableForScheduleEntry = 'True'	THEN 'T' ELSE 'F' END
			, IsAvailableForSeries				= CASE WHEN IsAvailableForSeries = 'True'			THEN 'T' ELSE 'F' END
			, IsAvailableForStaff				= CASE WHEN IsAvailableForStaff = 'True'			THEN 'T' ELSE 'F' END
			, IsAvailableForStaffLocation		= CASE WHEN IsAvailableForStaffLocation = 'True'	THEN 'T' ELSE 'F' END
			, IsAvailableForStaffTarget			= CASE WHEN IsAvailableForStaffTarget = 'True'		THEN 'T' ELSE 'F' END
			, IsAvailableForTask				= CASE WHEN IsAvailableForTask = 'True'				THEN 'T' ELSE 'F' END
			, IsFilterOnAdmin					= CASE WHEN IsFilterOnAdmin = 'True'				THEN 'T' ELSE 'F' END
			, IsFilterEverywhereExceptAdmin		= CASE WHEN IsFilterEverywhereExceptAdmin = 'True'	THEN 'T' ELSE 'F' END
			, IsPermissionCategory				= CASE WHEN IsPermissionCategory = 'True'			THEN 'T' ELSE 'F' END
			, IsSingleTaggingOnly				= CASE WHEN IsSingleTaggingOnly = 'True'			THEN 'T' ELSE 'F' END
			, IsTTCMCategory					= CASE WHEN IsTTCMCategory = 'True'					THEN 'T' ELSE 'F' END
			, IsUsedForFiltering				= CASE WHEN IsUsedForFiltering = 'True'				THEN 'T' ELSE 'F' END
			, IsUsedForStats					= CASE WHEN IsUsedForStats = 'True'					THEN 'T' ELSE 'F' END
		FROM anes.vw_STAGE_Tags
		ORDER BY CategoryName, TagName;
	""")
	rowsEDW = cursorEDW.fetchall()     #Returns a List[Row]
	rowsReturned = len(rowsEDW)
	
	if rowsReturned > 0:
		log = "(" + str(datetime.today()) + f")  {str(rowsReturned)} records pulled from EDW.\n\n"
		print(log)
		listLog.append(log)

		log = "(" + str(datetime.today()) + f")  Submitting TRUNCATE command.  Target: [ETLServer.StagingQgenda.{importTableName}]\n\n"
		print(log)
		listLog.append(log)		

		cursorETL = ETL.cursor()
		cursorETL.fast_executemany = False
		cursorETL.execute("TRUNCATE TABLE " + importTableName + ";")
		cursorETL.commit()

		log = "(" + str(datetime.today()) + f")  Inserting records.  Target table: [ETLServer.StagingQGenda.{importTableName}]\n"
		print(log)
		listLog.append(log)

		sql = "INSERT INTO " + importTableName + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
		cursorETL.executemany(sql, rowsEDW)

		log = "(" + str(datetime.today()) + ")  Data successfully transferred to ETLServer.\n\n"
		ETL.commit()
	else:
		log = "(" + str(datetime.today()) + f")  QUERY ERROR:  No records pulled from EDW.\n\n"
		print(log)
		listLog.append(log)
		return 400, listLog

	# BLOCK 03 | Retrieving data from ProdServer
	#
	#	- This block retrieves records from ProdServer
	#	- [stage.qdm_Tag] is truncated in preparation for the new records
	#	- The records retrieved from ProdServer are inserted into [stage.qdm_Tag]

	log = "\n\nETL PHASE: Data retrieval from ProdServer\n\n"
	print(log)
	listLog.append(log)

	log = "(" + str(datetime.today()) + f")  Submitting TRUNCATE command.  Target: [ETLServer.StagingQgenda.{stageTableName}]\n\n"
	print(log)
	listLog.append(log)
	cursorETL.execute("TRUNCATE TABLE " + stageTableName + ";")
	cursorETL.commit()


	log = "(" + str(datetime.today()) + f")  Retrieving records from ProdServer.  Target table: [{prodTableName}]\n"
	print(log)
	listLog.append(log)
	
	cursorCore = Core.cursor()
	cursorCore.execute("""
		SELECT
			CategoryKey
			, CategoryName
			, CategoryCreatedDateTime
			, CategoryModifiedDateTime
			, TagKey
			, TagName
			, TagCreatedDateTime
			, TagModifiedDateTime
			, IsAvailableForCreditAllocation
			, IsAvailableForHoliday
			, IsAvailableForLocation
			, IsAvailableForProfile
			, IsAvailableForRequestLimit
			, IsAvailableForScheduleEntry
			, IsAvailableForSeries
			, IsAvailableForStaff
			, IsAvailableForStaffLocation
			, IsAvailableForStaffTarget
			, IsAvailableForTask
			, IsFilterOnAdmin
			, IsFilterEverywhereExceptAdmin
			, IsPermissionCategory
			, IsSingleTaggingOnly
			, IsTTCMCategory
			, IsUsedForFiltering
			, IsUsedForStats
			, NULL AS ETLCommand
		FROM """ + prodTableName + """;
	""")
	rowsCore = cursorCore.fetchall()
	rowsReturned = len(rowsCore)

	if rowsReturned > 0:
		log = "(" + str(datetime.today()) + f")  Retrieved {str(rowsReturned)} records from ProdServer\n\n"
		print(log)
		listLog.append(log)

		log = "(" + str(datetime.today()) + f")  Inserting {str(rowsReturned)} records.  Target: [ETLServer.StagingQgenda.{stageTableName}]\n"
		print(log)
		listLog.append(log)

		sql = """
			INSERT INTO """ + stageTableName + """
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
		# Column Counter                           10                            20                   27
		cursorETL.executemany(sql, rowsCore)
		cursorETL.commit()

		log = "(" + str(datetime.today()) + f")  Records successfully transferred from ProdServer.\n\n"
		print(log)
		listLog.append(log)


	# BLOCK 04 | Consolidate and stage records
	#
	#	- This block calls a USP that performs several tasks
	#		1) Identifies and removes struck records from [stage.qdm_Tag]
	#		2) Inserts new records found in [import.qdm_Tag] to [stage.qdm_Tag]
	#		3) Searches for newer values of existing records and updates
	#			- StartTime
	#			- EndTime
	#			- Notes

	log = "\n\nETL PHASE: Staging records on ETLServer\n\n"
	print(log)
	listLog.append(log)

	log = "(" + str(datetime.today()) + ")  Staging TAG data by calling [import.usp_DoStagingTags]\n"
	print(log)
	listLog.append(log)

	cursorETL.execute("{CALL import.usp_DoStagingTags}")
	# USP is defined in \QGenda-Data-Mart\pipeline\sql\Tag\USP import,usp_DoStagingTags.sql
	cursorETL.commit()

	log = "(" + str(datetime.today()) + ")  Staging complete.\n"
	print(log)
	listLog.append(log)

	# BLOCK 05 | Push staged records to production
	#
	#	- Transfers the staged records to ProdServer

	log = "\n\nETL PHASE: Pushing staged records to ProdServer\n\n"
	print(log)
	listLog.append(log)

	log = "(" + str(datetime.today()) + f")  Identifying records flagged for update only.  Deletion not part of this ETL process.  Target: [ProdServer.QGenda.{prodTableName}]\n"
	print(log)
	listLog.append(log)

	cursorETL.execute("""
		SELECT
			CategoryKey
			, CategoryName
			, CategoryCreatedDateTime
			, CategoryModifiedDateTime
			, TagKey
			, TagName
			, TagCreatedDateTime
			, TagModifiedDateTime
			, IsAvailableForCreditAllocation
			, IsAvailableForHoliday
			, IsAvailableForLocation
			, IsAvailableForProfile
			, IsAvailableForRequestLimit
			, IsAvailableForScheduleEntry
			, IsAvailableForSeries
			, IsAvailableForStaff
			, IsAvailableForStaffLocation
			, IsAvailableForStaffTarget
			, IsAvailableForTask
			, IsFilterOnAdmin
			, IsFilterEverywhereExceptAdmin
			, IsPermissionCategory
			, IsSingleTaggingOnly
			, IsTTCMCategory
			, IsUsedForFiltering
			, IsUsedForStats
		FROM """ + stageTableName + """
		WHERE ETLCommand = 'Update';
	""")
	rowsETL = cursorETL.fetchall()
	rowsReturned = len(rowsETL)

	if rowsReturned > 0:
		log = "(" + str(datetime.today()) + f")  Deleting {str(rowsReturned)} records marked for update.  Target: [ProdServer.QGenda.{prodTableName}]\n"
		print(log)
		listLog.append(log)

		for row in rowsETL:
			key = row.TagKey
			cursorCore.execute("DELETE FROM " + prodTableName + " WHERE TagKey = ?", key)
		cursorCore.commit()

		log = "(" + str(datetime.today()) + f")  Deletion complete\n\n"
		print(log)
		listLog.append(log)

	log = "(" + str(datetime.today()) + f")  Identifying records new and updated records.  Target: [ProdServer.QGenda.{stageTableName}]\n"
	print(log)
	listLog.append(log)

	cursorETL.execute("""
		SELECT
			CategoryKey
			, CategoryName
			, CategoryCreatedDateTime
			, CategoryModifiedDateTime
			, TagKey
			, TagName
			, TagCreatedDateTime
			, TagModifiedDateTime
			, IsAvailableForCreditAllocation
			, IsAvailableForHoliday
			, IsAvailableForLocation
			, IsAvailableForProfile
			, IsAvailableForRequestLimit
			, IsAvailableForScheduleEntry
			, IsAvailableForSeries
			, IsAvailableForStaff
			, IsAvailableForStaffLocation
			, IsAvailableForStaffTarget
			, IsAvailableForTask
			, IsFilterOnAdmin
			, IsFilterEverywhereExceptAdmin
			, IsPermissionCategory
			, IsSingleTaggingOnly
			, IsTTCMCategory
			, IsUsedForFiltering
			, IsUsedForStats
		FROM """ + stageTableName + """
		WHERE ETLCommand IN ('New', 'Update');
	""")
	rowsETL = cursorETL.fetchall()
	rowsReturned = len(rowsETL)

	if rowsReturned > 0:
		log = "(" + str(datetime.today()) + f")  Inserting records flagged as new or updated.  Target: [ProdServer.QGenda.{prodTableName}]\n"
		print(log)
		listLog.append(log)

		sql = """
			INSERT INTO """ + prodTableName + """
				VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
		# Column Counter                          10                            20                26

		cursorCore.executemany(sql, rowsETL)
		cursorCore.commit()

		log = "(" + str(datetime.today()) + ")  Staged SCHEDULE records successfully transferred\n"
		print(log)
		listLog.append(log)
	else:
		log = "(" + str(datetime.today()) + ")  No new or updated records identified, no transfer required.\n"
		print(log)
		listLog.append(log)
	
	processEnd = datetime.today()


	# BLOCK 06 | Write log and clean up
	#
	#	- Close ODBC connections
	#	- Complete logging
	#	- Return data

	listLog.append("\n\nETL PHASE: Process clean-up\n\n")
	EDW.close()
	ETL.close()
	Core.close()

	processDuration = processEnd - processStart
	
	log = f"End process timestamp: {str(processEnd)}\n"
	print(log)
	listLog.append(log)

	log = f"Process duration: {str(processDuration)}\n"
	print(log)
	listLog.append(log)
	
	log = "Tag.getTags() complete\n\n"
	print(log)
	listLog.append(log)	
	
	return 200, listLog

# END OF FILE