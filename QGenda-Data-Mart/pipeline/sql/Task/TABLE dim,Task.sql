/*	FILE HEADER
 *		File Name:	TABLE dim,Task.sql
 *		Author:		Matt Chansard
 *		Project:	QGenda Data Mart
 *
 *	DESCRIPTION
 *		This file defines a database object designed to receive processed QGenda Task records
 *		from ANES-ETL1.  This table is used for reporting purposes.
 */

USE QGenda;
DROP TABLE IF EXISTS dim.Task;
GO
CREATE TABLE dim.Task
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
);
	-- Deployed 05-20-2022

-- END OF FILE --