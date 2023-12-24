#   FILE HEADER
#       File Name:  MTQT-CMC Absence Loader.py
#       Author:     Matt C
#       Project:    QGenda Migration
#   
#   DESCRIPTION
#		This file processes an Excel file containing absences of CMC Faculty and transmits the
#		records to the Reports database located on ProdServer
#

#	BLOCK 01 | #ScriptConfig

# Python Packages
import pandas as pd
import pyodbc
import os
import sys

from datetime import date, datetime

# Script Timestamps and Variables
processStartDateTime = datetime.today()
processRunDate = date.today()

# Set directory and file paths
currentPC = os.environ['COMPUTERNAME']
if currentPC == "laptop":
	logPath = os.path.join("C:\\", "Users", "scrubbed", "Sandbox", "MTQT", "logs", "")						# C:\Users\scrubbed\Sandbox\MTQT\logs\
elif currentPC == "etl":
	logPath = os.path.join("C:\\", "Users", "Public", "ANES ETL", "MTQT", "logs", "")						#C:\Users\Public\ANES ETL\MTQT\logs

if os.path.exists(logPath) == False:
	os.makedirs(logPath)

ingestDirectoryPath = os.path.join("C:\\", "Users", "scrubbed", "scrubbed", "Tasks", "Automations", "MTQT", "2023-09-18 Building automation for CMC", "")
fileList = os.listdir(ingestDirectoryPath)
for file in fileList:
	if file.endswith(".xlsx"):
		ingestFileName = file
		if "Historical" in file or "Modification" in file:
			recordType = "HE"
		else:
			recordType = "MG"
		break

#SQL Objects
sqlTableName = "dbo.FacultyAbsence"

#Logging Configuration
logFileName = f"MTQT-LoadAbsencesCMC_{processRunDate}.txt"
log = [f"MTQT CMC Faculty Absence Load Log_{str(processRunDate)}\n",f"Process Start Timestamp:{str(processStartDateTime)}\n"]

#Local methods
def CreateLogEntry(logObject: list[str], logString: str, applyTimeStamp: bool):
	from datetime import datetime

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

def WriteLogToFile(log, processStartDateTime):
	processEnd = datetime.today();
	if type(processStartDateTime) is datetime:
		processDuration = processEnd - processStartDateTime
	else:
		log.append("Unable to calculate process duration due to incorrect method call")

	try:
		with open(logPath + logFileName, 'x') as logFile:
			logFile.writelines(log)
			logFile.write(f"End process timestamp: {str(processEnd)}\n")
			logFile.write(f"Process duration: {str(processDuration)}\n")
			logFile.write("\n---- COMPLETE ----")
	except FileExistsError:
		with open(logPath + logFileName, 'a') as logFile:
			logFile.writelines(log)
			logFile.write("\n\n")
			logFile.write(f"Appending Log For Additional ETL processes\n")
			logFile.write(f"End process timestamp: {str(processEnd)}\n")
			logFile.write(f"Process duration: {str(processDuration)}\n")
			logFile.write("\n---- COMPLETE ----")


#ODBC Configuration
CreateLogEntry(log, "Initializing ODBC Connections", True)

cxnReportsDB = pyodbc.connect("DSN=Core;Database=Reports;")
cursorReports = cxnReportsDB.cursor()

cxnApmdwDB = pyodbc.connect("DSN=Core;Database=APMDW;")
cursorAPMDW = cxnApmdwDB.cursor()

CreateLogEntry(log, "ODBC Connections Established", True)


#	BLOCK 02 | Ingest Excel file
#
#	Structure of receiving table
#		PersonID				INT
#		FacultyFullName		NVARCHAR(30)
#		ScheduleDate		DATE
#		PayCode				NVARCHAR(10)
#		PayCodeHours		NVARCHAR(5)
#		FTE					DECIMAL(2,1)
#		FacultyGroup		NVARCHAR(3)
#		RecordAddedDate		DATETIME2		DEFAULT GETDATE()
#		RecordType			NVARCHAR(15)
#		ReportMonth			TINYINT			DEFAULT MONTH(GETDATE())
#		ReportYear			SMALLINT		DEFAULT YEAR(GETDATE())
#		ProducedErrorYN		NCHAR(1)		DEFAULT 'N'
#		ErrorComment		NVARCHAR(200)
CreateLogEntry(log, "Opening Excel file", True)
df = pd.read_excel(io=f"{os.path.join(ingestDirectoryPath, ingestFileName)}", sheet_name=0)

for index, row in df.iterrows():
	if row["PayCode"] == "EDUCATION":
		row["PayCode"] = "EDU"
	if row["PayCode"] == "BANK HOLIDAY" or row["PayCode"] == "HOLIDAY BANK":
		row["PayCode"] = "Holiday Banked"
	modifyPayCodeHours = str(row["PayCodeHours"])[0:5]
	if modifyPayCodeHours[0] == "0":
		modifyPayCodeHours = modifyPayCodeHours[1:5]


#	BLOCK 03 | Validations
#
#	- Ensure every employee only has one UTSW Person Number	(vdf1)
#	- Ensure the PersonID is correct				        (vdf2)	#TODO
vdf1 = df[['Name', 'PersonNumber']].nunique()
if vdf1.iloc[0] != vdf1.iloc[1]:
	CreateLogEntry(log, "Validation failed: Single employee tied to multiple PersonID values.")
	WriteLogToFile(log, processStartDateTime)
	sys.exit(10)


#	BLOCK 04 | Transmit records to database
CreateLogEntry(log, "Caching SQL syntax", True)
sql = f"""
	INSERT INTO {sqlTableName} (PersonID, FacultyFullName, ScheduleDate, PayCode, ImportPayHours, RecordType, FacultyGroup)
		VALUES (?, ?, ?, ?, ?, ?, ?);
"""
	
CreateLogEntry(log, "Transmitting records to database", True)
for index, row in df.iterrows():
	record = [row["PersonID"], row["Name"], row["ScheduleDate"], row["PayCode"], modifyPayCodeHours, recordType, "CMC"]
	cursorReports.execute(sql, record)
cursorReports.commit()
CreateLogEntry(log, "Records writtent to database successfully.", True)

cxnReportsDB.close()
cxnApmdwDB.close()

WriteLogToFile(log, processStartDateTime)

sys.exit(0)
# END OF FILE