# code-samples
Combination of T-SQL and python code samples

# Description of repository contents
## QGenda Data Mart
The QGenda Data Mart folder contains the ETL process I developed to refresh a department clinical scheduling database.
This process acquires records via the QGenda REST API, and identifies whether the record
- is a new record
- is an updated version of an existing record
- no longer exists (perhaps due to scheduling error) and therefore should be removed

## QGenda Migration
This folder contains an automation I developed for a department to deliver a file of employee absences instead of 
requiring administrative staff to manually enter absences.  The process is executed via a Windows .bat file.

## Provider-Case-Log
This folder contains an ETL process I developed to acquire provider case data from Epic, an electronic medical record
system.  I've included this becuase the "USP import,stageProviderCaseLogUSP.sql" file contains a dynamic SQL solution.