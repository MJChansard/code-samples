/*	FILE HEADER
 *		File Name:	TABLE TimeEvent ETL.sql
 *		Author:		Matt Chansard
 *		Project:	QGenda Data Mart
 *
 *	DESCRIPTION
 *		This file contains database object definitions related to the ETL of QGenda TimeEvent
 *		records.  These objects are deployed on ANES-ETL1.swmed.org.
 */


-- Connect to ANES-ETL1
DROP TABLE IF EXISTS import.TimeEvent;          -- Previous name for this object
DROP TABLE IF EXISTS import.qdm_TimeEvent;
CREATE TABLE import.qdm_TimeEvent
(
	ScheduleEntryKey		UNIQUEIDENTIFIER
    , TaskShiftKey			UNIQUEIDENTIFIER
    , StaffKey				UNIQUEIDENTIFIER
    , TaskKey				UNIQUEIDENTIFIER
    , TimePunchEventKey		BIGINT
   
	, TimeEventDate			DATE				-- Originally named [Date]
	, TimeEventWeekday		TINYINT				-- Originally named [DayOfWeek]
	
	, ActualClockIn			SMALLDATETIME		-- Data source: [ActualClockInLocal]
	, EffectiveClockIn		SMALLDATETIME		-- Data source: [EffectiveClockInLocal]
	, ActualClockOut		SMALLDATETIME		-- Data source: [ActualClockOutLocal]
	, EffectiveClockOut		SMALLDATETIME		-- Data source: [EffectiveClockOutLocal]
    , Duration				INT

	, IsStruck				NCHAR(1)
    , IsEarly				NCHAR(1)
    , IsLate				NCHAR(1)
    , IsExcessiveDuration	NCHAR(1)
    , IsExtended			NCHAR(1)
    , IsUnplanned			NCHAR(1)
	, FlagsResolved			NCHAR(1)
	, Notes					NVARCHAR(255)
	, LastModifiedDate		SMALLDATETIME
);
	-- Deployed 07-19-2022
	-- 21 columns

-- Connect to ANES-ETL1
DROP TABLE IF EXISTS stage.TimeEvent;           -- Previous name for this object
DROP TABLE IF EXISTS stage.qdm_TimeEvent;
GO
CREATE TABLE stage.qdm_TimeEvent
(
	ScheduleEntryKey		UNIQUEIDENTIFIER
    , TaskShiftKey			UNIQUEIDENTIFIER
    , StaffKey				UNIQUEIDENTIFIER
    , TaskKey				UNIQUEIDENTIFIER
    , TimePunchEventKey		BIGINT
   
	, TimeEventDate			DATE				-- Originally named [Date]
	, TimeEventWeekday		TINYINT				-- Originally named [DayOfWeek]
	, ActualClockIn			SMALLDATETIME		-- Data source: [ActualClockInLocal]
    , EffectiveClockIn		SMALLDATETIME		-- Data source: [EffectiveClockInLocal]
	, ActualClockOut		SMALLDATETIME		-- Data source: [ActualClockOutLocal]
    , EffectiveClockOut		SMALLDATETIME		-- Data source: [EffectiveClockOutLocal]
    , Duration				INT

	, IsStruck				NCHAR(1)
    , IsEarly				NCHAR(1)
    , IsLate				NCHAR(1)
    , IsExcessiveDuration	NCHAR(1)
    , IsExtended			NCHAR(1)
    , IsUnplanned			NCHAR(1)
	, FlagsResolved			NCHAR(1)
	, Notes					NVARCHAR(255)
    , LastModifiedDate		SMALLDATETIME
	, ETLCommand			NVARCHAR(6)
);
	-- Deployed 07-19-2022
	-- 22 columns

-- END OF FILE --