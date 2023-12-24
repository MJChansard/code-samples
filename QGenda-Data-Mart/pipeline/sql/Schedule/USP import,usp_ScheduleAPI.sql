/*	FILE HEADER
 *		File Name:	USP import.usp_usp_ScheduleAPI.sql
 *		Author:		Matt Chansard
 *		Project:	QGenda Data Mart
 *
 *	DESCRIPTION
 *		This file contains the definition of a User-Defined Stored Procedure (USP).
 *
 *		- Accepts a single argument @json, which is a JSON string
 *	 	- Truncates any existing data prior to ETL
 *		- Parses the data passed in to the USP and inserts records into [import.qdm_Schedule]
 */
 
ALTER PROCEDURE import.usp_ScheduleAPI ( @json NVARCHAR(MAX) )
AS
BEGIN
	SET NOCOUNT ON;

	IF (SELECT COUNT(*) FROM import.qdm_Schedule) > 0
		TRUNCATE TABLE import.qdm_Schedule;

	INSERT INTO import.qdm_Schedule
		SELECT
			ScheduleKey		
			, TaskShiftKey			
			, StaffKey				
			, TaskKey				
			, ScheduleDate
			, StartDate
			, StartTime
			, EndDate
			, EndTime
			, TaskName
			, StaffFName
			, StaffLName
			, Credit
			, TaskIsPrintStart
			, TaskIsPrintEnd
			, IsCred				= CASE (IsCred)			WHEN 'true' THEN 'T' ELSE 'F' END
			, IsLocked				= CASE (IsLocked)		WHEN 'true' THEN 'T' ELSE 'F' END
			, IsPublished			= CASE (IsPublished)	WHEN 'true' THEN 'T' ELSE 'F' END
			, IsStruck				= CASE (IsStruck)		WHEN 'true' THEN 'T' ELSE 'F' END
			, Notes
		FROM OPENJSON(@json)
		WITH
		(
			ScheduleKey				UNIQUEIDENTIFIER	'$.ScheduleKey',
			TaskShiftKey			UNIQUEIDENTIFIER	'$.TaskShiftKey',
			StaffKey				UNIQUEIDENTIFIER	'$.StaffKey',
			TaskKey					UNIQUEIDENTIFIER	'$.TaskKey',
			ScheduleDate			DATE				'$.Date',
			StartDate				DATE				'$.StartDate',
			StartTime				TIME				'$.StartTime',
			EndDate					DATE				'$.EndDate',
			EndTime					TIME				'$.EndTime',
			TaskName				NVARCHAR(50)		'$.TaskName',
			StaffFName				NVARCHAR(50)		'$.StaffFName',
			StaffLName				NVARCHAR(50)		'$.StaffLName',
			Credit					DECIMAL(6,2)		'$.Credit',
			TaskIsPrintStart		NCHAR(1)			'$.TaskIsPrintStart',
			TaskIsPrintEnd			NCHAR(1)			'$.TaskIsPrintEnd',
			IsCred					NCHAR(1)			'$.IsCred',
			IsLocked				NCHAR(1)			'$.IsLocked',
			IsPublished				NCHAR(1)			'$.IsPublished',
			IsStruck				NVARCHAR(5)			'$.IsStruck',
			Notes					NVARCHAR(255)		'$.Notes'
		);
END;		-- OF CREATE PROCEDURE CALL