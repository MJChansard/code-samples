/*	FILE HEADER
 *		File Name: TABLE Schedule ETL.sql
 *		Author: Matt Chansard
 *		Project: QGenda Data Mart
 *
 *	DESCRIPTION
 *		This file contains database object definitions related to the ETL of QGenda Schedule
 *		records.  These objects are deployed on ANES-ETL1.swmed.org.
 */

-- Connect to ANES-ETL1
DROP TABLE IF EXISTS import.Schedule;
DROP TABLE IF EXISTS import.qdm_Schedule;
CREATE TABLE import.qdm_Schedule
(
	ScheduleKey			UNIQUEIDENTIFIER
	, TaskShiftKey		UNIQUEIDENTIFIER
	, StaffKey			UNIQUEIDENTIFIER
	, TaskKey			UNIQUEIDENTIFIER
	, ScheduleDate		DATE
	, StartDate			DATE				
	, StartTime			TIME				-- Eligible for testing
	, EndDate			DATE
	, EndTime			TIME				-- Eligible for testing
	, TaskName			NVARCHAR(50)
	, StaffFName		NVARCHAR(50)
	, StaffLName		NVARCHAR(50)
	, Credit			DECIMAL(6,2)
	, TaskIsPrintStart	NCHAR(1)
	, TaskIsPrintEnd	NCHAR(1)
	, IsCred			NCHAR(1)
	, IsLocked			NCHAR(1)
	, IsPublished		NCHAR(1)
	, IsStruck			NCHAR(1)			-- Eligible for testing
	, Notes				NVARCHAR(255)		-- Eligible for testing
);
	-- 20 columns
	-- Deployed 07-19-2022
GO

-- Connect to ANES-ETL1
DROP TABLE IF EXISTS stage.Schedule;
DROP TABLE IF EXISTS stage.qdm_Schedule;
CREATE TABLE stage.qdm_Schedule
(
	ScheduleKey				UNIQUEIDENTIFIER
	, TaskShiftKey			UNIQUEIDENTIFIER
	, StaffKey				UNIQUEIDENTIFIER
	, TaskKey				UNIQUEIDENTIFIER
	, ScheduleDate			DATE
	, StartDate				DATE
	, StartTime				TIME
	, EndDate				DATE
	, EndTime				TIME
	, TaskName				NVARCHAR(50)
	, StaffFName			NVARCHAR(50)
	, StaffLName			NVARCHAR(50)
	, Credit				DECIMAL(6,2)
	, TaskIsPrintStart		NCHAR(1)
	, TaskIsPrintEnd		NCHAR(1)
	, IsCred				NCHAR(1)
	, IsLocked				NCHAR(1)
	, IsPublished			NCHAR(1)
	, IsStruck				NCHAR(1)
	, Notes					NVARCHAR(255)
	, ETLCommand			NVARCHAR(6)		-- New, Update, Delete
);
	-- 21 columns
	-- Deployed 07-19-2022
-- END OF FILE	