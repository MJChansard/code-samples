/*	FILE HEADER
 *		File Name:	TABLE dbo,Schedule.sql
 *		Author:		Matt Chansard
 *		Project:	QGenda Data Mart
 *
 *	DESCRIPTION
 *		This file defines a database object designed to receive processed QGenda Schedule
 *		records from ANES-ETL1.  This table is used for reporting purposes.
 */


-- Connect to ANESCore
USE QGenda;
DROP TABLE IF EXISTS dbo.Schedule;
GO
CREATE TABLE dbo.Schedule
(
	ScheduleKey			UNIQUEIDENTIFIER
	, TaskShiftKey		UNIQUEIDENTIFIER
	, StaffKey			UNIQUEIDENTIFIER
	, TaskKey			UNIQUEIDENTIFIER
	, ScheduleDate		DATE
	, StartDate			DATE
	, StartTime			TIME
	, EndDate			DATE
	, EndTime			TIME
	, TaskName			NVARCHAR(50)
	, StaffFName		NVARCHAR(50)
	, StaffLName		NVARCHAR(50)
	, Credit			DECIMAL(6,2)
--	, Credit			NVARCHAR(6)
	, TaskIsPrintStart	NCHAR(1)
	, TaskIsPrintEnd	NCHAR(1)
	, IsCred			NCHAR(1)
	, IsLocked			NCHAR(1)
	, IsPublished		NCHAR(1)
	, IsStruck			NCHAR(1)
	, Notes				NVARCHAR(255)
);
	-- Table deployed 5-17-2022


-- The following queries were used to assess data types and ultimately design the table 
-- definition above.  They are stored here to show what went into creating [dbo.Schedule]

-- Connect to DWPRDSQL.QGendaMirror
SELECT
	MAX(LEN(TaskName))		AS mlTaskName
	, MAX(LEN(TaskId))		AS mlTaskId
	, MAX(LEN(TaskAbbrev))	AS mlTaskAbbrev

	, MAX(LEN(StaffId))		AS mlStaffId
	, MAX(LEN(StaffAbbrev))	AS mlStaffAbbrev
	, MAX(LEN(StaffFName))	AS mlStaffFName
	, MAX(LEN(StaffLName))	AS mlStaffLName
	, MAX(LEN(StaffEmail))	AS mlStaffEmail

	, MAX(LEN(Notes))		AS mlNotes
FROM anes.vw_STAGE_Schedule;

SELECT DISTINCT Credit
FROM anes.vw_STAGE_Schedule;

/*	EXCLUDED COLUMNS
      BgColor						-- Not needed
      BillSysId						-- NULL only
      CompKey]						-- Not needed
      ExtCallSysId					-- NULL only
      TextColor						-- Not needed
      RegHours						-- NULL only     
      BillingTypeKey				-- NULL only
      UserProfile					-- NULL only
      PayrollStartDate				-- NULL only
      PayrollEndDate				-- NULL only
      DailyUnitAverage				-- 0.00 only
      StaffInternalId				-- NULL only
      UserLastLoginDateTimeUTC		-- Exclude for now
      SourceOfLogin					-- Exclude for now
      CalSyncKey					-- Not needed
      Tags							-- NULL only
      TTCMTags						-- NULL only
      CategoryKey					-- NULL only
      CategoryName					-- NULL only
      Key							-- NULL only
      Name							-- NULL only
      EffectiveFromDate				-- NULL only
      EffectiveToDate				-- NULL only
      IsSchedulable					-- NULL only
*/

-- END OF FILE --