/*	FILE HEADER
 *		File Name:	USP import,usp_TimeEventAPI.sql
 *		Author:		Matt Chansard
 *		Project:	QGenda Data Mart
 *
 *	DESCRIPTION
 *		This file contains the definition of a User-Defined Stored Procedure (USP).
 *		
 *		- Accepts a single argument @Json, which is a JSON string
 *	 	- Truncates any existing data prior to ETL
 *	 	- Parses the data passed in to the USP (@json) and inserts records into [import.qdm_TimeEvent]
 */


 -- Connect to ANES-ETL1
ALTER PROCEDURE import.usp_TimeEventAPI ( @json NVARCHAR(MAX) )
AS
BEGIN
	SET NOCOUNT ON;

	IF (SELECT COUNT(*) FROM import.qdm_TimeEvent) > 0
		TRUNCATE TABLE import.qdm_TimeEvent;

	INSERT INTO import.qdm_TimeEvent
		SELECT
			ScheduleEntryKey
			, TaskShiftKey
			, StaffKey
			, TaskKey
			, TimePunchEventKey
			, TimeEventDate
			, TimeEventWeekday
			, ActualClockIn			= CAST(REPLACE(SUBSTRING(ActualClockIn,		1, 19), 'T', ' ') AS DATETIME)
			, EffectiveClockIn		= CAST(REPLACE(SUBSTRING(EffectiveClockIn,	1, 19), 'T', ' ') AS DATETIME)
			, ActualClockOut		= CAST(REPLACE(SUBSTRING(ActualClockOut,	1, 19), 'T', ' ') AS DATETIME)
			, EffectiveClockOut		= CAST(REPLACE(SUBSTRING(EffectiveClockOut, 1, 19), 'T', ' ') AS DATETIME)
			, Duration
			, IsStruck				= CASE (IsStruck)				WHEN 'true' THEN 'T' ELSE 'F' END	-- Contains T and F
			, IsEarly				= CASE (IsEarly)				WHEN 'true' THEN 'T' ELSE 'F' END	-- Contains T and F
			, IsLate				= CASE (IsLate)					WHEN 'true' THEN 'T' ELSE 'F' END	-- Contains T and F
			, IsExcessiveDuration	= CASE (IsExcessiveDuration)	WHEN 'true' THEN 'T' ELSE 'F' END	-- Contains F only
			, IsExtended			= CASE (IsExtended)				WHEN 'true' THEN 'T' ELSE 'F' END	-- Contains T and F
			, IsUnplanned			= CASE (IsUnplanned)			WHEN 'true' THEN 'T' ELSE 'F' END	-- Contains T and F
			, FlagsResolved			= CASE (FlagsResolved)			WHEN 'true' THEN 'T' ELSE 'F' END	-- Contains T and F
			, Notes
			, LastModifiedDate		= CAST(REPLACE(SUBSTRING(LastModifiedDate, 1, 19), 'T', ' ') AS DATETIME)
		FROM OPENJSON(@json)
		WITH
		(
			ScheduleEntryKey		UNIQUEIDENTIFIER	'$.ScheduleEntryKey',
			TaskShiftKey			UNIQUEIDENTIFIER	'$.TaskShiftKey',
			StaffKey				UNIQUEIDENTIFIER	'$.StaffKey',
			TaskKey					UNIQUEIDENTIFIER	'$.TaskKey',
			TimePunchEventKey		BIGINT				'$.TimePunchEventKey',
			TimeEventDate			DATE				'$.Date',
			TimeEventWeekday		TINYINT				'$.DayOfWeek',
			ActualClockIn			NVARCHAR(20)		'$.ActualClockInLocal',
			EffectiveClockIn		NVARCHAR(20)		'$.EffectiveClockInLocal',
			ActualClockOut			NVARCHAR(20)		'$.ActualClockOutLocal',
			EffectiveClockOut		NVARCHAR(20)		'$.EffectiveClockOutLocal',
			Duration				INT					'$.Duration',
			IsStruck				NVARCHAR(5)			'$.IsStruck',
			IsEarly					NVARCHAR(5)			'$.IsEarly',
			IsLate					NVARCHAR(5)			'$.IsLate',
			IsExcessiveDuration		NVARCHAR(5)			'$.IsExcessiveDuration',
			IsExtended				NVARCHAR(5)			'$.IsExtended',
			IsUnplanned				NVARCHAR(5)			'$.IsUnplanned',
			FlagsResolved			NVARCHAR(5)			'$.FlagsResolved',
			Notes					NVARCHAR(255)		'$.Notes',
			LastModifiedDate		NVARCHAR(35)		'$.LastModifiedDate'
		);
END;		-- OF CREATE PROCEDURE CALL
-- END OF FILE