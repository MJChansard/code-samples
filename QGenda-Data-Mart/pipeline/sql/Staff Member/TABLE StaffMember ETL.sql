/*	FILE HEADER
 *		File Name:	TABLE StaffMember ETL.sql
 *		Author:		Matt Chansard
 *		Project:	QGenda Data Mart
 *
 *	DESCRIPTION
 *		This file contains database object definitions related to the ETL of QGenda StaffMember
 *		records.  These objects are deployed on ANES-ETL1.swmed.org.
 */
 
 
-- Connect to ANES-ETL1
USE StagingQGenda;
DROP TABLE IF EXISTS import.StaffMember;		-- Previous name of this object
DROP TABLE IF EXISTS import.qdm_StaffMember;
GO
CREATE TABLE import.qdm_StaffMember
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

DROP TABLE IF EXISTS stage.StaffMember;			-- Previous name of this object
DROP TABLE IF EXISTS stage.qdm_StaffMember;
GO
CREATE TABLE stage.qdm_StaffMember
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
	, PushToProductionFlag		NCHAR(1)
);
	-- Deployed 07-19-2022
	-- 20 columns
	-- Deployed 07-19-2022
	-- 20 columns

-- END OF FILE --