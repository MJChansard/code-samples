/*	FILE HEADER
 *		File Name:	USP import.usp_StaffMemberAPI.sql
 *		Author:		Matt Chansard
 *		Project:	QGenda Data Mart
 *
 *	DESCRIPTION
 *		This file contains the definition of a User-Defined Stored Procedure (USP) 
 *		
 *		- Accepts a single argument @json, which is a JSON string
 *	 	- Truncates any existing data prior to ETL
 *		- Parses the data passed in to the USP and inserts records into [import.qdm_StaffMember]
 */


ALTER PROCEDURE import.usp_StaffMemberAPI ( @json NVARCHAR(MAX) )
AS
BEGIN
	SET NOCOUNT ON;

	IF (SELECT COUNT(*) FROM import.qdm_StaffMember) > 0
		TRUNCATE TABLE import.qdm_StaffMember;

	INSERT INTO import.qdm_StaffMember
		SELECT
			StaffKey
			, StaffId
			, StaffAbbrev
			, StaffTypeKey
			, UserProfileKey
			, PayrollId
			, EmrId
			, Npi

			, FirstName
			, LastName
			, StartDate					= CAST(SUBSTRING(StartDate, 1, 10)	AS DATE)
			, EndDate					= CAST(SUBSTRING(EndDate, 1, 10)	AS DATE)
			, MobilePhone
			, Pager
			, Email

			, IsActive                  = IIF(DeactivationDate IS NULL, 'T', 'F')
			, DeactivationDate			= SUBSTRING(DeactivationDate, 1, 10)
			, UserLastLoginDateTimeUTC	= CAST(REPLACE(SUBSTRING(UserLastLoginDateTimeUTC, 1, 23), 'T', ' ') AS DATETIME)
			, SourceOfLogin				= CASE (SourceOfLogin) WHEN 'Desktop' THEN 'D' WHEN 'Mobile' THEN 'M' ELSE NULL END
		FROM OPENJSON(@json)
		WITH
		(
			StaffKey					UNIQUEIDENTIFIER	'$.StaffKey',
			StaffId						NVARCHAR(25)		'$.StaffId',
			StaffAbbrev					NVARCHAR(25)		'$.Abbrev',
			StaffTypeKey				NVARCHAR(40)		'$.StaffTypeKey',
			UserProfileKey				UNIQUEIDENTIFIER	'$.UserProfileKey',
			PayrollId					NVARCHAR(10)		'$.PayrollId',
			EmrId						NVARCHAR(10)		'$.EmrId',
			Npi							BIGINT				'$.Npi',

			FirstName					NVARCHAR(50)		'$.FirstName',
			LastName					NVARCHAR(50)		'$.LastName',
			StartDate					NVARCHAR(30)		'$.StartDate',
			EndDate						NVARCHAR(30)		'$.EndDate',
			MobilePhone					NVARCHAR(15)		'$.MobilePhone',
			Pager						NVARCHAR(15)		'$.Pager',
			Email						NVARCHAR(50)		'$.Email',

			DeactivationDate			NVARCHAR(30)		'$.DeactivationDateUtc',
			UserLastLoginDateTimeUTC	NVARCHAR(30)		'$.UserLastLoginDateTimeUtc',
			SourceOfLogin				NVARCHAR(7)			'$.SourceOfLogin'
		);
END;		-- OF CREATE PROCEDURE CALL
-- END OF FILE