/*	FILE HEADER
 *		File Name:	USP import,usp_TaskAPI.sql
 *		Author:		Matt Chansard
 *		Project:	QGenda Data Mart
 *
 *	DESCRIPTION
 *		This file contains the definition of a User-Defined Stored Procedure (USP).
 *		
 *		- Accepts a single argument @Json, which is a JSON string
 *	 	- Truncates any existing data prior to ETL
 *	 	- Parses the data passed in to the USP (@json) and inserts records into [import.qdm_Task]
 */


-- Connect to ANES-ETL1
ALTER PROCEDURE import.usp_TaskAPI ( @json NVARCHAR(MAX) )
AS
BEGIN
	SET NOCOUNT ON;

	IF (SELECT COUNT(*) FROM import.qdm_Task) > 0
		TRUNCATE TABLE import.qdm_Task;

	INSERT INTO import.qdm_Task
		SELECT
			TaskKey
			, TaskName									-- Originally named [Name]
			, TaskId				= IIF(TaskId = '', NULL, TaskId)
			, TaskAbbrev								-- Originally named [Abbrev]
			, TaskType									-- Originally named [Type]
			, DepartmentId			= IIF(DepartmentId = '', NULL, DepartmentId)
			, EmrId					= IIF(DepartmentId = '', NULL, DepartmentId)
			, StartDate				= SUBSTRING(StartDate, 1, 10)
			, EndDate				= SUBSTRING(EndDate, 1, 10)
			, ContactInformation	= IIF(ContactInformation = '', NULL, ContactInformation)
			, IsManual				= CASE (IsManual)			WHEN 'true' THEN 'T' ELSE 'F' END	-- Originally named [Manual]
			, RequireTimePunch		= CASE (RequireTimePunch)	WHEN 'true' THEN 'T' ELSE 'F' END
			, Notes					= IIF(Notes = '', NULL, Notes)
		FROM OPENJSON(@json)
		WITH
		(
			TaskKey					UNIQUEIDENTIFIER	'$.TaskKey',
			TaskName				NVARCHAR(60)		'$.Name',				-- Originally named [Name]
			TaskId					NVARCHAR(50)		'$.TaskId',
			TaskAbbrev				NVARCHAR(50)		'$.Abbrev',				-- Originally named [Abbrev]
			TaskType				NVARCHAR(15)		'$.Type',				-- Originally named [Type]
			DepartmentId			NVARCHAR(15)		'$.DepartmentId',	
			EmrId					NVARCHAR(15)		'$.EmrId',
			StartDate				NVARCHAR(20)		'$.StartDate',
			EndDate					NVARCHAR(20)		'$.EndDate',
			ContactInformation		NVARCHAR(50)		'$.ContactInformation',
			IsManual				NCHAR(5)			'$.Manual',				-- Originally named [Manual]
			RequireTimePunch		NCHAR(5)			'$.RequireTimePunch',
			Notes					NVARCHAR(255)		'$.Notes'
		);
END;
-- END OF FILE