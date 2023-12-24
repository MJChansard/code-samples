/*	FILE HEADER
 *		File Name:	TABLE dim,StaffMember.sql
 *		Author:		Matt Chansard
 *		Project:	QGenda Data Mart
 *
 *	DESCRIPTION
 *		This file defines a database object designed to receive processed QGenda StaffMember
 *		records from ANES-ETL1.  This table is used for reporting purposes.
 */
 

USE QGenda;
DROP TABLE IF EXISTS dim.StaffMember;
GO
CREATE TABLE dim.StaffMember
(
	StaffKey					UNIQUEIDENTIFIER
	, StaffId					NVARCHAR(25)
	, StaffAbbrev				NVARCHAR(25)
	, StaffTypeKey				NVARCHAR(40)
	, UserProfileKey			UNIQUEIDENTIFIER
	, PayrollId					NVARCHAR(10)
	, EmrId						NVARCHAR(10)
	, Npi						BIGINT

	, FirstName					NVARCHAR(50)
	, LastName					NVARCHAR(50)
	, StartDate					DATE
	, EndDate					DATE
	, MobilePhone				NVARCHAR(15)
	, Pager						NVARCHAR(15)
	, Email						NVARCHAR(50)

	, IsActive					NCHAR(1)
	, DeactivationDate			DATE
	, UserLastLoginDateTimeUTC	DATETIME
	, SourceOfLogin				NCHAR(1)
);
	-- Deployed 07-19-2022
	-- 19 columns
-- END OF FILE --