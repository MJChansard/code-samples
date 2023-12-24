#   FILE HEADER
#       File Name:  QGendaMain.py
#       Author:     Matt C
#       Project:    QGenda Data Mart
#   
#   DESCRIPTION
#       This python script imports the following python scripts in order to manage their execution
#           - Schedule.py
#           - TimeEvent.py
#           - StaffMember.py
#           - Tag.py
#           - Task.py
#           - TagStaff.py
#           - TagTask.py
#
#   TECHNICAL Notes
#       - All scripts need to be kept within the same directory
#       - Each imported script defines a single function responsible for transferring
#         a specific set of records (Schedule, StaffMember,etc)

# Import QGenda Data Mart scripts
import Schedule
import TimeEvent
import Tag
import Task
import TagTask
import StaffMember
import TagStaff

# Python Packages
import os
import requests
import sys
from datetime import date, datetime, timedelta

# LOCAL METHODS
def WriteLogToFile(log):
    try:
        with open(logPath + logName, 'x') as logFile:
            logFile.writelines(log)
            logFile.write(f"End process timestamp: {str(processEnd)}\n")
            logFile.write(f"Process duration: {str(processDuration)}\n")
            logFile.write("\n---- QGENDA DATA MART REFRESH COMPLETE ----")
    except FileExistsError:
        with open(logPath + logName, 'a') as logFile:
            logFile.writelines(log)
            logFile.write("\n\n---- APPENDING ADDITIONAL LOGGING ----")
            logFile.write(f"End process timestamp: {str(processEnd)}\n")
            logFile.write(f"Process duration: {str(processDuration)}\n")
            logFile.write("\n---- QGENDA DATA MART REFRESH COMPLETE ----")

print("QGenda Data Mart update commencing.")

# BEGIN SCRIPT #
# BLOCK 01 | Initialization
	#   - Configure proxy
	#	- Configuration for connecting to QGenda REST API
	#	- Request and store access token

proxySite = "scrubbed"
os.environ["HTTP_PROXY"] = proxySite
os.environ["HTTPS_PROXY"] = proxySite

logPath = os.path.join("C:\\", "Users", "Public", "ANES ETL", "QGenda Data Mart", "logs", "")
logName = "QGendaPipelineLog_" + str(date.today()) + ".txt"

processStart = datetime.today()
mainLog = ["QGenda Data Mart Refresh Log\n","----------------------------\n", f"Process Start Timestamp: {str(processStart)}\n\n"]
            
log = "(" + str(datetime.today()) + ")  Authenticating with QGenda API\n"
print(log)
mainLog.append(log)

companyKey = "scrubbed"
url = "https://api.qgenda.com/v2/login"
headers = {
	'Content-Type': 'application/x-www-form-urlencoded'
}
payload='email=&password='      # Don't forget to add credentials

response = requests.request("POST", url, headers=headers, data=payload)
responseDictionary = response.json()
accessToken = responseDictionary['access_token']

log = "(" + str(datetime.today()) + ")  Authentication successful\n\n"
print(log)
mainLog.append(log)

log = "(" + str(datetime.today()) + ")  Determining date range for data refresh\n"
print(log)
mainLog.append(log)

#Refresh window set to 30 days prior to today through 60 days ahead of today
currentDate = date.today()
endDate = currentDate + timedelta(days=60)
startDate = currentDate - timedelta(days=30)

log = "(" + str(datetime.today()) + f")  Refresh window set from {str(startDate)} to {str(endDate)}.\n\n"
print(log)
mainLog.append(log)


log = "(" + str(datetime.today()) + ")  Refreshing [dbo.Schedule]\n"
print(log)
mainLog.append(log)

#result is a Tuple [int, list]
result = Schedule.getSchedule(accessToken, companyKey, startDate, endDate)

for entry in result[1]:
    mainLog.append(entry)

if result[0] == 200:   
    log = "(" + str(datetime.today()) + ")  Data refresh successful\n\n"
    print(log)
    mainLog.append(log)
else:
    log = "(" + str(datetime.today()) + ")  Data refresh failure\n\n"
    print(log)
    mainLog.append(log)
    WriteLogToFile(mainLog)
    sys.exit()


log = "(" + str(datetime.today()) + ")  Refreshing [dbo.TimeEvent]\n"
print(log)
mainLog.append(log)

#result is a Tuple [int, list]
result = TimeEvent.getTimeEvent(accessToken, companyKey, startDate, endDate)

for entry in result[1]:
    mainLog.append(entry)

if result[0] == 200:
    log = "(" + str(datetime.today()) + ")  Data refresh successful\n\n"
    print(log)
    mainLog.append(log)
else:
    log = "(" + str(datetime.today()) + ")  Data refresh failure\n\n"
    print(log)
    mainLog.append(log)
    WriteLogToFile(mainLog)
    sys.exit()


log = "(" + str(datetime.today()) + ")  Refreshing [dim.StaffMember]\n"
print(log)
mainLog.append(log)

#result is a Tuple [int, list]
result = StaffMember.getStaffMember(accessToken)

for entry in result[1]:
    mainLog.append(entry)

if result[0] == 200:
    log = "(" + str(datetime.today()) + ")  Data refresh successful\n\n"
    print(log)
    mainLog.append(log)
else:
    log = "(" + str(datetime.today()) + ")  Data refresh failure\n\n"
    print(log)
    mainLog.append(log)
    WriteLogToFile(mainLog)
    sys.exit()


log = "(" + str(datetime.today()) + ")  Refreshing [dim.Tag]\n"
print(log)
mainLog.append(log)

# result is a Tuple [int, list]
result = Tag.getTags(accessToken, companyKey)

for entry in result[1]:
    mainLog.append(entry)

if result[0] == 200:
    log = "(" + str(datetime.today()) + ")  Data refresh successful\n\n"
    print(log)
    mainLog.append(log)
else:
    log = "(" + str(datetime.today()) + ")  Data refresh failure\n\n"
    print(log)
    mainLog.append(log)
    WriteLogToFile(mainLog)
    sys.exit()

# Task
log = "(" + str(datetime.today()) + ")  Refreshing [dim.Task]\n"
print(log)
mainLog.append(log)

# result is a Tuple [int, list]
result = Task.getTask(accessToken)

for entry in result[1]:
    mainLog.append(entry)

if result[0] == 200:
    log = "(" + str(datetime.today()) + ")  Data refresh successful\n\n"
    print(log)
    mainLog.append(log)
else:
    log = "(" + str(datetime.today()) + ")  Data refresh failure\n\n"
    print(log)
    mainLog.append(log)
    WriteLogToFile(mainLog)
    sys.exit()

# Staff Tags
log = "(" + str(datetime.today()) + ")  Refreshing [dim.TaggedStaff]\n"
print(log)
mainLog.append(log)

result = TagStaff.getStaffTags(accessToken)

if result[0] == 200:
    log = "(" + str(datetime.today()) + ")  Data refresh successful\n\n"
    print(log)
    mainLog.append(log)
else:
    log = "(" + str(datetime.today()) + ")  Data refresh failure\n\n"
    print(log)
    mainLog.append(log)
    WriteLogToFile(mainLog)
    sys.exit()

# Task Tags
log = "(" + str(datetime.today()) + ")  Refreshing [dim.TaggedTask]\n"
print(log)
mainLog.append(log)

result = TagTask.getTaskTags(accessToken)

if result[0] == 200:
    log = "(" + str(datetime.today()) + ")  Data refresh successful\n\n"
    print(log)
    mainLog.append(log)
else:
    log = "(" + str(datetime.today()) + ")  Data refresh failure\n\n"
    print(log)
    mainLog.append(log)
    WriteLogToFile(mainLog)
    sys.exit()

processEnd = datetime.today()
processDuration = processEnd - processStart

WriteLogToFile(mainLog)
sys.exit()
#  END OF FILE