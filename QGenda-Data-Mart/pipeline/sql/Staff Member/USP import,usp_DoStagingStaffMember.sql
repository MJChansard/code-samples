/*	FILE HEADER
 *		File Name:	USP import,usp_DoStagingStaffMember.sql
 *		Author:		Matt Chansard
 *		Project:	QGenda Data Mart
 *
 *	DESCRIPTION
 *		This file contains the definition of a User-Defined Stored Procedure (USP) which performs
 *		the following tasks:
 *			- Identifies new records that do not yet exist in ANESCore
 *	 		- Identify records that exist but contain data that requires updating
 */

ALTER PROCEDURE import.usp_DoStagingStaffMember
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @NewRecords TABLE (StaffKey UNIQUEIDENTIFIER);
	INSERT INTO @NewRecords
		SELECT DISTINCT StaffKey
		FROM import.qdm_StaffMember
		EXCEPT
		SELECT DISTINCT StaffKey
		FROM stage.qdm_StaffMember;

	INSERT INTO stage.qdm_StaffMember
		SELECT
			v.StaffKey
			, i.StaffId
			, i.StaffAbbrev				-- Originally named [Abbrev]
			, i.StaffTypeKey
			, i.UserProfileKey
			, i.PayrollId
			, i.EmrId
			, i.Npi
			, i.FirstName
			, i.LastName
			, i.StartDate
			, i.EndDate
			, i.MobilePhone
			, i.Pager
			, i.Email
			, i.IsActive
			, i.DeactivationDate
			, i.UserLastLoginDateTimeUTC
			, i.SourceOfLogin
			, 'T' AS PushToProductionFlag
		FROM import.qdm_StaffMember AS i
			INNER JOIN @NewRecords AS v
				ON i.StaffKey = v.StaffKey;

	
	DECLARE @RecordsToUpdate TABLE (StaffKey UNIQUEIDENTIFIER);
	INSERT INTO @RecordsToUpdate
		SELECT DISTINCT StaffKey
		FROM import.qdm_StaffMember
		EXCEPT
		SELECT StaffKey
		FROM @NewRecords;

	SELECT
		i.StaffKey

		, i.StaffId AS iStaffId
		, s.StaffId AS sStaffId
		, cStaffId = 
			CASE
				WHEN i.StaffId = s.StaffId THEN 'Match'
				ELSE 'Conflict'
			END

		, i.StaffAbbrev AS iStaffAbbrev
		, s.StaffAbbrev AS sStaffAbbrev
		, cStaffAbbrev =
			CASE
				WHEN i.StaffAbbrev = s.StaffAbbrev THEN 'Match'
				ELSE 'Conflict'
			END

		, i.StaffTypeKey AS iStaffTypeKey
		, s.StaffTypeKey AS sStaffTypeKey
		, cStaffTypeKey = 
			CASE
				WHEN i.StaffTypeKey = s.StaffTypeKey THEN 'Match'
				ELSE 'Conflict'
			END

		, i.UserProfileKey AS iUserProfileKey
		, s.UserProfileKey AS sUserProfileKey
		, cUserProfileKey = 
			CASE
				WHEN i.UserProfileKey = s.UserProfileKey THEN 'Match'
				ELSE 'Conflict'
			END
		
		, i.PayrollId AS iPayrollId
		, s.PayrollId AS sPayrollId
		, cPayrollId = 
			CASE
				WHEN i.PayrollId = s.PayrollId THEN 'Match'
				ELSE 'Conflict'
			END

		, i.EmrId AS iEmrId
		, s.EmrId AS sEmrId
		, cEmrId = 
			CASE
				WHEN i.EmrId = s.EmrId THEN 'Match'
				ELSE 'Conflict'
			END

		, i.Npi AS iNpi
		, s.Npi AS sNpi
		, cNpi = 
			CASE
				WHEN i.Npi = s.Npi THEN 'Match'
				ELSE 'Conflict'
			END

		, i.FirstName AS iFirstName
		, s.FirstName AS sFirstName
		, cFirstName = 
			CASE
				WHEN i.FirstName = s.FirstName THEN 'Match'
				ELSE 'Conflict'
			END

		, i.LastName AS iLastName
		, s.LastName AS sLastName
		, cLastName =
			CASE
				WHEN i.LastName = s.LastName THEN 'Match'
				ELSE 'Conflict'
			END

		, i.StartDate AS iStartDate
		, s.StartDate AS sStartDate
		, cStartDate =
			CASE
				WHEN i.StartDate = s.StartDate THEN 'Match'
				ELSE 'Conflict'
			END

		, i.EndDate AS iEndDate
		, s.EndDate AS sEndDate
		, cEndDate =
			CASE
				WHEN i.EndDate = s.EndDate THEN 'Match'
				ELSE 'Conflict'
			END

		, i.MobilePhone AS iMobilePhone
		, s.MobilePhone AS sMobilePhone
		, cMobilePhone =
			CASE
				WHEN i.MobilePhone = s.MobilePhone THEN 'Match'
				ELSE 'Conflict'
			END

		, i.Pager AS iPager
		, s.Pager as sPager
		, cPager =
			CASE
				WHEN i.Pager = s.Pager THEN 'Match'
				ELSE 'Conflict'
			END

		, i.Email AS iEmail
		, s.Email AS sEmail
		, cEmail =
			CASE
				WHEN i.Email = s.Email THEN 'Match'
				ELSE 'Conflict'
			END

		, i.IsActive AS iIsActive
		, s.IsActive AS sIsActive
		, cIsActive =
			CASE
				WHEN i.IsActive = s.IsActive THEN 'Match'
				ELSE 'Conflict'
			END

		, i.DeactivationDate AS iDeactivationDate
		, s.DeactivationDate AS sDeactivationDate
		, cDeactivationDate =
			CASE
				WHEN i.DeactivationDate IS NOT NULL AND s.DeactivationDate IS NULL THEN 'Conflict'
				WHEN i.DeactivationDate IS NULL AND s.DeactivationDate IS NOT NULL THEN 'Conflict'
				WHEN i.DeactivationDate <> s.DeactivationDate THEN 'Conflict'
				WHEN i.DeactivationDate IS NULL AND s.DeactivationDate IS NULL THEN 'Match'
				WHEN i.DeactivationDate = s.DeactivationDate THEN 'Match'
			END

		, i.UserLastLoginDateTimeUTC AS iUserLastLogin
		, s.UserLastLoginDateTimeUTC AS sUserLastLogin
		, cUserLastLogin =
			CASE
				WHEN i.UserLastLoginDateTimeUTC = s.UserLastLoginDateTimeUTC THEN 'Match'
				ELSE 'Conflict'
			END

		, i.SourceOfLogin AS iSourceOfLogin
		, s.SourceOfLogin AS sSourceOfLogin
		, cSourceOfLogin =
			CASE
				WHEN i.SourceOfLogin = s.SourceOfLogin THEN 'Match'
				ELSE 'Conflict'
			END
	INTO #Comparison
	FROM import.qdm_StaffMember AS i
		INNER JOIN stage.qdm_StaffMember AS s
			ON i.StaffKey = s.StaffKey
		INNER JOIN @RecordsToUpdate AS fltr
			ON i.StaffKey = fltr.StaffKey;

	/*	Uncomment for debugging
	DROP TABLE IF EXISTS import.Comparison;
	SELECT *
	INTO import.Comparison
	FROM #Comparison
	ORDER BY iLastName, iFirstName;
	*/

	DECLARE @UpdateRecords TABLE (StaffKey UNIQUEIDENTIFIER);
	
	-- Correct changes to StaffId
	INSERT INTO @UpdateRecords
		SELECT StaffKey
		FROM #Comparison
		WHERE cStaffId = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_StaffMember
			SET StaffId = i.StaffId
				, PushToProductionFlag = 'T'
			FROM stage.qdm_StaffMember AS s
				INNER JOIN import.qdm_StaffMember AS i
					ON s.StaffKey = i.StaffKey
				INNER JOIN @UpdateRecords AS f
					ON s.StaffKey = f.StaffKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to StaffAbbrev
	INSERT INTO @UpdateRecords
		SELECT StaffKey
		FROM #Comparison
		WHERE cStaffAbbrev = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_StaffMember
			SET StaffAbbrev = i.StaffAbbrev
				, PushToProductionFlag = 'T'
			FROM stage.qdm_StaffMember AS s
				INNER JOIN import.qdm_StaffMember AS i
					ON s.StaffKey = i.StaffKey
				INNER JOIN @UpdateRecords AS f
					ON s.StaffKey = f.StaffKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to StaffTypeKey
	INSERT INTO @UpdateRecords
		SELECT StaffKey
		FROM #Comparison
		WHERE cStaffTypeKey = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_StaffMember
			SET StaffTypeKey = i.StaffTypeKey
				, PushToProductionFlag = 'T'
			FROM stage.qdm_StaffMember AS s
				INNER JOIN import.qdm_StaffMember AS i
					ON s.StaffKey = i.StaffKey
				INNER JOIN @UpdateRecords AS f
					ON s.StaffKey = f.StaffKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to UserProfileKey
	INSERT INTO @UpdateRecords
		SELECT StaffKey
		FROM #Comparison
		WHERE cUserProfileKey = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_StaffMember
			SET UserProfileKey = i.UserProfileKey
				, PushToProductionFlag = 'T'
			FROM stage.qdm_StaffMember AS s
				INNER JOIN import.qdm_StaffMember AS i
					ON s.StaffKey = i.StaffKey
				INNER JOIN @UpdateRecords AS f
					ON s.StaffKey = f.StaffKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to PayrollId
	INSERT INTO @UpdateRecords
		SELECT StaffKey
		FROM #Comparison
		WHERE cPayrollId = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_StaffMember
			SET PayrollId = i.PayrollId
				, PushToProductionFlag = 'T'
			FROM stage.qdm_StaffMember AS s
				INNER JOIN import.qdm_StaffMember AS i
					ON s.StaffKey = i.StaffKey
				INNER JOIN @UpdateRecords AS f
					ON s.StaffKey = f.StaffKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to EmrId
	INSERT INTO @UpdateRecords
		SELECT StaffKey
		FROM #Comparison
		WHERE cEmrId = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_StaffMember
			SET EmrId = i.EmrId
				, PushToProductionFlag = 'T'
			FROM stage.qdm_StaffMember AS s
				INNER JOIN import.qdm_StaffMember AS i
					ON s.StaffKey = i.StaffKey
				INNER JOIN @UpdateRecords AS f
					ON s.StaffKey = f.StaffKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to NPI
	INSERT INTO @UpdateRecords
		SELECT StaffKey
		FROM #Comparison
		WHERE cNpi = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_StaffMember
			SET Npi = i.Npi
				, PushToProductionFlag = 'T'
			FROM stage.qdm_StaffMember AS s
				INNER JOIN import.qdm_StaffMember AS i
					ON s.StaffKey = i.StaffKey
				INNER JOIN @UpdateRecords AS f
					ON s.StaffKey = f.StaffKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to FirstName
	INSERT INTO @UpdateRecords
		SELECT StaffKey
		FROM #Comparison
		WHERE cFirstName = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_StaffMember
			SET FirstName = i.FirstName
				, PushToProductionFlag = 'T'
			FROM stage.qdm_StaffMember AS s
				INNER JOIN import.qdm_StaffMember AS i
					ON s.StaffKey = i.StaffKey
				INNER JOIN @UpdateRecords AS f
					ON s.StaffKey = f.StaffKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to LastName
	INSERT INTO @UpdateRecords
		SELECT StaffKey
		FROM #Comparison
		WHERE cLastName = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_StaffMember
			SET LastName = i.LastName
				, PushToProductionFlag = 'T'
			FROM stage.qdm_StaffMember AS s
				INNER JOIN import.qdm_StaffMember AS i
					ON s.StaffKey = i.StaffKey
				INNER JOIN @UpdateRecords AS f
					ON s.StaffKey = f.StaffKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to StartDate
	INSERT INTO @UpdateRecords
		SELECT StaffKey
		FROM #Comparison
		WHERE cStartDate = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_StaffMember
			SET StartDate = i.StartDate
				, PushToProductionFlag = 'T'
			FROM stage.qdm_StaffMember AS s
				INNER JOIN import.qdm_StaffMember AS i
					ON s.StaffKey = i.StaffKey
				INNER JOIN @UpdateRecords AS f
					ON s.StaffKey = f.StaffKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to EndDate
	INSERT INTO @UpdateRecords
		SELECT StaffKey
		FROM #Comparison
		WHERE cEndDate = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_StaffMember
			SET EndDate = i.EndDate
				, PushToProductionFlag = 'T'
			FROM stage.qdm_StaffMember AS s
				INNER JOIN import.qdm_StaffMember AS i
					ON s.StaffKey = i.StaffKey
				INNER JOIN @UpdateRecords AS f
					ON s.StaffKey = f.StaffKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to MobilePhone
	INSERT INTO @UpdateRecords
		SELECT StaffKey
		FROM #Comparison
		WHERE cMobilePhone = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_StaffMember
			SET MobilePhone = i.MobilePhone
				, PushToProductionFlag = 'T'
			FROM stage.qdm_StaffMember AS s
				INNER JOIN import.qdm_StaffMember AS i
					ON s.StaffKey = i.StaffKey
				INNER JOIN @UpdateRecords AS f
					ON s.StaffKey = f.StaffKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to Pager
	INSERT INTO @UpdateRecords
		SELECT StaffKey
		FROM #Comparison
		WHERE cPager = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_StaffMember
			SET Pager = i.Pager
				, PushToProductionFlag = 'T'
			FROM stage.qdm_StaffMember AS s
				INNER JOIN import.qdm_StaffMember AS i
					ON s.StaffKey = i.StaffKey
				INNER JOIN @UpdateRecords AS f
					ON s.StaffKey = f.StaffKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to Email
	INSERT INTO @UpdateRecords
		SELECT StaffKey
		FROM #Comparison
		WHERE cEmail = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_StaffMember
			SET Email = i.Email
				, PushToProductionFlag = 'T'
			FROM stage.qdm_StaffMember AS s
				INNER JOIN import.qdm_StaffMember AS i
					ON s.StaffKey = i.StaffKey
				INNER JOIN @UpdateRecords AS f
					ON s.StaffKey = f.StaffKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to IsActive
	INSERT INTO @UpdateRecords
		SELECT StaffKey
		FROM #Comparison
		WHERE cIsActive = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_StaffMember
			SET IsActive = i.IsActive
				, PushToProductionFlag = 'T'
			FROM stage.qdm_StaffMember AS s
				INNER JOIN import.qdm_StaffMember AS i
					ON s.StaffKey = i.StaffKey
				INNER JOIN @UpdateRecords AS f
					ON s.StaffKey = f.StaffKey;

			DELETE FROM @UpdateRecords;
		END		

	-- Correct changes to DeactivationDate
	INSERT INTO @UpdateRecords
		SELECT StaffKey
		FROM #Comparison
		WHERE cDeactivationDate = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_StaffMember
			SET DeactivationDate =
					CASE
						WHEN i.DeactivationDate IS NULL THEN NULL
						ELSE i.DeactivationDate
					END
				, PushToProductionFlag = 'T'
			FROM stage.qdm_StaffMember AS s
				INNER JOIN import.qdm_StaffMember AS i
					ON s.StaffKey = i.StaffKey
				INNER JOIN @UpdateRecords AS f
					ON s.StaffKey = f.StaffKey;		

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to UserLastLoginDateTimeUTC
	INSERT INTO @UpdateRecords
		SELECT StaffKey
		FROM #Comparison
		WHERE cUserLastLogin = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_StaffMember
			SET UserLastLoginDateTimeUTC = i.UserLastLoginDateTimeUTC
				, PushToProductionFlag = 'T'
			FROM stage.qdm_StaffMember AS s
				INNER JOIN import.qdm_StaffMember AS i
					ON s.StaffKey = i.StaffKey
				INNER JOIN @UpdateRecords AS f
					ON s.StaffKey = f.StaffKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to SourceOfLogin
	INSERT INTO @UpdateRecords
		SELECT StaffKey
		FROM #Comparison
		WHERE cSourceOfLogin = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_StaffMember
			SET SourceOfLogin = i.SourceOfLogin
				, PushToProductionFlag = 'T'
			FROM stage.qdm_StaffMember AS s
				INNER JOIN import.qdm_StaffMember AS i
					ON s.StaffKey = i.StaffKey
				INNER JOIN @UpdateRecords AS f
					ON s.StaffKey = f.StaffKey;

			DELETE FROM @UpdateRecords;
		END
END;	-- OF CREATE PROCEDURE CALL

-- END OF FILE --