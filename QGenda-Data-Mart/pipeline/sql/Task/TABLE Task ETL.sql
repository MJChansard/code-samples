/*	FILE HEADER
 *		File Name:	TABLE Task ETL.sql
 *		Author:		Matt Chansard
 *		Project:	QGenda Data Mart
 *
 *	DESCRIPTION
 *		This file contains database object definitions related to the ETL of QGenda Task records
 *		These objects are deployed on ANES-ETL1.swmed.org.
 */

-- Connect to ANES-ETL1
DROP TABLE IF EXISTS import.TaskAPI;			-- Previous name of object
DROP TABLE IF EXISTS import.qdm_Task;
GO
CREATE TABLE import.qdm_Task
(
	TaskKey					UNIQUEIDENTIFIER
	, TaskName				NVARCHAR(60)		-- Originally named [Name]
	, TaskId				NVARCHAR(50)
	, TaskAbbrev			NVARCHAR(50)		-- Originally named [Abbrev]
	, TaskType				NCHAR(15)			-- Originally named [Type]
	, DepartmentId			NVARCHAR(15)
	, EmrId					NVARCHAR(15)
	, StartDate				DATE
	, EndDate				DATE
	, ContactInformation	NVARCHAR(50)
	, IsManual				NCHAR(1)			-- Originally named [Manual]
	, RequireTimePunch		NCHAR(1)
	, Notes					NVARCHAR(255)
);
	-- Deployed 07-20-2022
	-- 13 columns

DROP TABLE IF EXISTS stage.Task;				-- Previous name of object
DROP TABLE IF EXISTS stage.qdm_Task;
GO
CREATE TABLE stage.qdm_Task
(
	TaskKey					UNIQUEIDENTIFIER
	, TaskName				NVARCHAR(50)		-- Originally named [Name]
	, TaskId				NVARCHAR(50)
	, TaskAbbrev			NVARCHAR(50)		-- Originally named [Abbrev]
	, TaskType				NVARCHAR(15)		-- Originally named [Type]
	, DepartmentId			NVARCHAR(15)
	, EmrId					NVARCHAR(15)
	, StartDate				DATE
	, EndDate				DATE
	, ContactInformation	NVARCHAR(50)
	, IsManual				NCHAR(1)			-- Originally named [Manual]
	, RequireTimePunch		NCHAR(1)
	, Notes					NVARCHAR(255)
	, ETLCommand			NVARCHAR(6)
);
	-- Deployed 07-20-2022
	-- 14 columns

-- END OF FILE --