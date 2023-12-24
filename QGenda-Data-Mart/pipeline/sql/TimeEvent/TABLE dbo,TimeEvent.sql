/*	FILE HEADER
 *		File Name:	TABLE dbo,TimeEvent.sql
 *		Author:		Matt Chansard
 *		Project:	QGenda Data Mart
 *
 *	DESCRIPTION
 *		This file defines a database object designed to receive processed QGenda TimeEvent
 *		records from ANES-ETL1.  This table is used for reporting purposes.
 */


-- Connect to ANESCore
USE QGenda;

DROP TABLE IF EXISTS dbo.TimeEvent;
CREATE TABLE dbo.TimeEvent
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
	-- Deployed 5-17-2022
	-- 21 columns


-- The following queries were used to assess data types and ultimately design the table 
-- definition above.  They are stored here to show what went into creating [dbo.TimeEvent]

-- Connect to DWPRDSQL.QGendaMirror
SELECT
	MAX(LEN(Notes)) AS mlNotes	-- 50
FROM anes.vw_STAGE_TimeEvent;

SELECT DISTINCT
--	Notes					-- Contains unique records
--	TimeZone				-- NULL or (UTC-06:00) Central Time (US & Canada)
--	Duration				-- Contains unique records
--	IsClockInGeoVerified	-- Only FALSE
--	IsClockOutGeoVerified	-- Only FALSE
--	ReasonCode				-- Only NULL
--	ReasonCodeId			-- Only NULL
--	ScheduleEntry			-- Only NULL
--	FlagsResolved			-- Only FALSE
FROM anes.vw_STAGE_TimeEvent;

SELECT
	TimePunchEventKey
	, ActualClockIn
	, ActualClockInLocal
	, EffectiveClockIn
	, EffectiveClockInLocal

	, ActualClockOut
	, ActualClockOutLocal
	, EffectiveClockOut
	, EffectiveClockOutLocal
FROM anes.vw_STAGE_TimeEvent
WHERE (ActualClockInLocal <> EffectiveClockInLocal)
   OR (ActualClockOutLocal <> EffectiveClockOutLocal)

/*	COLUMN NOTES
	StaffKey					-- Include, Key for Staff
    TaskKey						-- Include, Key for Task
    IsStruck					-- Include
    Notes						-- Include, has records
    ActualClockIn				-- Exclude, is local time + 5 hrs
    ActualClockOut				-- Exclude, is local time + 5 hrs
    CompanyKey					-- Exclude, not needed
    Date						-- Include, rename
    DayOfWeek					-- Include, rename
    Duration					-- Include
    EffectiveClockIn			-- Exclude, is local time + 5 hrs
    EffectiveClockOut			-- Exclude, is local time + 5 hrs
    IsClockInGeoVerified		-- Exclude, all records FALSE
    IsClockOutGeoVerified		-- Exclude, all records FALSE
    IsEarly						-- Include
    IsLate						-- Include
    IsExcessiveDuration			-- Include
    IsExtended					-- Include
    IsUnplanned					-- Include
    FlagsResolved				-- Include, only FALSE but minimal storage cost
    ReasonCode					-- Exclude, only NULLs
    ReasonCodeId				-- Exclude, only NULLS
    ScheduleEntry				-- Exclude, only NULLs
    ScheduleEntryKey			-- Include, FK to SCHEDULE
    TaskShiftKey				-- Include, FK
    TimePunchEventKey			-- Include, PK of table
    TimeZone					-- Exclude, NULL or (UTC-06:00) Central Time (US & Canada)
    ActualClockInLocal			-- Include
    ActualClockOutLocal			-- Include
    EffectiveClockInLocal		-- Include
    EffectiveClockOutLocal		-- Include
    LastModifiedDate			-- Include
*/

-- END OF FILE --