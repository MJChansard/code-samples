/*	FILE HEADER
 *		File Name:	USP import,usp_DoStagingTags.sql
 *		Author:		Matt Chansard
 *		Project:	QGenda Data Mart
 *
 *	DESCRIPTION
 *		This file contains the definition of a User-Defined Stored Procedure (USP) which performs
 *		the following tasks:
 *			- Identifies new records that do not yet exist in ANESCore
 *			- Identify records that exist but contain data that requires updating
 *
 *		A column called [ETLCommand] is used to identify what to do with a record
 *			- New:		Record does not exist in ANESCore but has been retrieved from the API
 *			- Update:	Record does exist ANESCore but API extract contains different values
 */

CREATE PROCEDURE import.usp_DoStagingTags
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @NewRecords TABLE (TagKey BIGINT);
	INSERT INTO @NewRecords
		SELECT DISTINCT TagKey
		FROM import.qdm_Tag
		EXCEPT
		SELECT DISTINCT TagKey
		FROM stage.qdm_Tag;

	INSERT INTO stage.qdm_Tag
		SELECT
			i.CategoryKey
			, i.CategoryName
			, i.CategoryCreatedDateTime
			, i.CategoryModifiedDateTime
			, f.TagKey
			, i.TagName
			, i.TagCreatedDateTime
			, i.TagModifiedDateTime
			, i.IsAvailableForCreditAllocation
			, i.IsAvailableForHoliday
			, i.IsAvailableForLocation
			, i.IsAvailableForProfile
			, i.IsAvailableForRequestLimit
			, i.IsAvailableForScheduleEntry
			, i.IsAvailableForSeries
			, i.IsAvailableForStaff
			, i.IsAvailableForStaffLocation
			, i.IsAvailableForStaffTarget
			, i.IsAvailableForTask
			, i.IsFilterOnAdmin
			, i.IsFilterEverywhereExceptAdmin
			, i.IsPermissionCategory
			, i.IsSingleTaggingOnly
			, i.IsTTCMCategory
			, i.IsUsedForFiltering
			, i.IsUsedForStats
			, ETLCommand = 'New'
		FROM import.qdm_Tag AS i
			INNER JOIN @NewRecords AS f
				ON i.TagKey = f.TagKey;
	
	SELECT
		i.TagKey

		, i.CategoryName AS iCategoryName
		, s.CategoryName AS sCategoryName
		, cCategoryName = IIF(i.CategoryName = s.CategoryName, 'Match', 'Conflict')

		, i.CategoryModifiedDateTime AS iCategoryModifiedDateTime
		, s.CategoryModifiedDateTime AS sCategoryModifiedDateTime
		, cCategoryModifiedDateTime = IIF(i.CategoryModifiedDateTime = s.CategoryModifiedDateTime, 'Match', 'Conflict')

		, i.TagName	AS iTagName
		, s.TagName AS sTagName
		, cTagName = IIF(i.CategoryModifiedDateTime = s.CategoryModifiedDateTime, 'Match', 'Conflict')
		
		, i.TagModifiedDateTime AS iTagModifiedDateTime
		, s.TagModifiedDateTime AS sTagModifiedDateTime
		, cTagModifiedDateTime = IIF(i.TagModifiedDateTime = s.TagModifiedDateTime, 'Match', 'Conflict')

		, i.IsAvailableForCreditAllocation AS iIsAvailableForCreditAllocation
		, s.IsAvailableForCreditAllocation AS sIsAvailableForCreditAllocation
		, cIsAvailableForCreditAllocation = IIF(i.IsAvailableForCreditAllocation = s.IsAvailableForCreditAllocation, 'Match', 'Conflict')

		, i.IsAvailableForHoliday AS iIsAvailableForHoliday
		, s.IsAvailableForHoliday AS sIsAvailableForHoliday
		, cIsAvailableForHoliday = IIF(i.IsAvailableForHoliday = s.IsAvailableForHoliday, 'Match', 'Conflict')

		, i.IsAvailableForLocation AS iIsAvailableForLocation
		, s.IsAvailableForLocation AS sIsAvailableForLocation
		, cIsAvailableForLocation = IIF(i.IsAvailableForLocation = s.IsAvailableForLocation, 'Match', 'Conflict')

		, i.IsAvailableForProfile AS iIsAvailableForProfile
		, s.IsAvailableForProfile AS sIsAvailableForProfile
		, cIsAvailableForProfile = IIF(i.IsAvailableForProfile = s.IsAvailableForProfile, 'Match', 'Conflict')

		, i.IsAvailableForRequestLimit AS iIsAvailableForRequestLimit
		, s.IsAvailableForRequestLimit AS sIsAvailableForRequestLimit
		, cIsAvailableForRequestLimit = IIF(i.IsAvailableForRequestLimit = s.IsAvailableForRequestLimit, 'Match', 'Conflict')

		, i.IsAvailableForScheduleEntry AS iIsAvailableForScheduleEntry
		, s.IsAvailableForScheduleEntry AS sIsAvailableForScheduleEntry
		, cIsAvailableForScheduleEntry = IIF(i.IsAvailableForScheduleEntry = s.IsAvailableForScheduleEntry, 'Match', 'Conflict')

		, i.IsAvailableForSeries AS iIsAvailableForSeries
		, s.IsAvailableForSeries AS sIsAvailableForSeries
		, cIsAvailableForSeries = IIF(i.IsAvailableForSeries = s.IsAvailableForSeries, 'Match', 'Conflict')

		, i.IsAvailableForStaff AS iIsAvailableForStaff
		, s.IsAvailableForStaff AS sIsAvailableForStaff
		, cIsAvailableForStaff = IIF(i.IsAvailableForStaff = s.IsAvailableForStaff, 'Match', 'Conflict')

		, i.IsAvailableForStaffLocation AS iIsAvailableForStaffLocation
		, s.IsAvailableForStaffLocation AS sIsAvailableForStaffLocation
		, cIsAvailableForStaffLocation = IIF(i.IsAvailableForStaffLocation = s.IsAvailableForStaffLocation, 'Match', 'Conflict')

		, i.IsAvailableForStaffTarget AS iIsAvailableForStaffTarget
		, s.IsAvailableForStaffTarget AS sIsAvailableForStaffTarget
		, cIsAvailableForStaffTarget = IIF(i.IsAvailableForStaffTarget = s.IsAvailableForStaffTarget, 'Match', 'Conflict')

		, i.IsAvailableForTask AS iIsAvailableForTask
		, s.IsAvailableForTask AS sIsAvailableForTask
		, cIsAvailableForTask = IIF(i.IsAvailableForTask = s.IsAvailableForTask, 'Match', 'Conflict')

		, i.IsFilterOnAdmin	AS iIsFilterOnAdmin	
		, s.IsFilterOnAdmin	 AS sIsFilterOnAdmin	
		, cIsFilterOnAdmin = IIF(i.IsFilterOnAdmin = s.IsFilterOnAdmin, 'Match', 'Conflict')

		, i.IsFilterEverywhereExceptAdmin AS iIsFilterEverywhereExceptAdmin
		, s.IsFilterEverywhereExceptAdmin AS sIsFilterEverywhereExceptAdmin
		, cIsFilterEverywhereExceptAdmin = IIF(i.IsFilterEverywhereExceptAdmin = s.IsFilterEverywhereExceptAdmin, 'Match', 'Conflict')

		, i.IsPermissionCategory AS iIsPermissionCategory
		, s.IsPermissionCategory AS sIsPermissionCategory
		, cIsPermissionCategory = IIF(i.IsPermissionCategory = s.IsPermissionCategory, 'Match', 'Conflict')

		, i.IsSingleTaggingOnly AS iIsSingleTaggingOnly
		, s.IsSingleTaggingOnly AS sIsSingleTaggingOnly
		, cIsSingleTaggingOnly = IIF(i.IsSingleTaggingOnly = s.IsSingleTaggingOnly, 'Match', 'Conflict')

		, i.IsTTCMCategory AS iIsTTCMCategory
		, s.IsTTCMCategory AS sIsTTCMCategory
		, cIsTTCMCategory = IIF(i.IsTTCMCategory = s.IsTTCMCategory, 'Match', 'Conflict')

		, i.IsUsedForFiltering AS iIsUsedForFiltering
		, s.IsUsedForFiltering AS sIsUsedForFiltering
		, cIsUsedForFiltering = IIF(i.IsUsedForFiltering = s.IsUsedForFiltering, 'Match', 'Conflict')
		
		, i.IsUsedForStats AS iIsUsedForStats
		, s.IsUsedForStats AS sIsUsedForStats
		, cIsUsedForStats = IIF(i.IsUsedForStats = s.IsUsedForStats, 'Match', 'Conflict')
	INTO #Comparison
	FROM import.qdm_Tag AS i
		INNER JOIN stage.qdm_Tag AS s
			ON i.TagKey = s.TagKey
	WHERE NOT i.TagKey IN (SELECT TagKey from @NewRecords);

	-- #CORRECTIONS
	PRINT 'Identifying and updating record conflicts.';
	DECLARE @UpdateRecords TABLE (TagKey BIGINT);
	
	-- Correct changes to CategoryName
	INSERT INTO @UpdateRecords
		SELECT TagKey
		FROM #Comparison
		WHERE cCategoryName = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Tag
			SET CategoryName = i.CategoryName
				, ETLCommand = 'Update'
			FROM stage.qdm_Tag AS s
				INNER JOIN import.qdm_Tag AS i
					ON s.TagKey = i.TagKey
				INNER JOIN @UpdateRecords AS f
					ON s.TagKey = f.TagKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to CategoryModifiedDateTime 
	INSERT INTO @UpdateRecords
		SELECT TagKey
		FROM #Comparison
		WHERE cCategoryModifiedDateTime  = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Tag
			SET CategoryModifiedDateTime = i.CategoryModifiedDateTime 
				, ETLCommand = 'Update'
			FROM stage.qdm_Tag AS s
				INNER JOIN import.qdm_Tag AS i
					ON s.TagKey = i.TagKey
				INNER JOIN @UpdateRecords AS f
					ON s.TagKey = f.TagKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to TagName
	INSERT INTO @UpdateRecords
		SELECT TagKey
		FROM #Comparison
		WHERE cTagName = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Tag
			SET TagName = i.TagName
				, ETLCommand = 'Update'
			FROM stage.qdm_Tag AS s
				INNER JOIN import.qdm_Tag AS i
					ON s.TagKey = i.TagKey
				INNER JOIN @UpdateRecords AS f
					ON s.TagKey = f.TagKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to TagModifiedDateTime
	INSERT INTO @UpdateRecords
		SELECT TagKey
		FROM #Comparison
		WHERE cTagModifiedDateTime = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Tag
			SET TagModifiedDateTime = i.TagModifiedDateTime
				, ETLCommand = 'Update'
			FROM stage.qdm_Tag AS s
				INNER JOIN import.qdm_Tag AS i
					ON s.TagKey = i.TagKey
				INNER JOIN @UpdateRecords AS f
					ON s.TagKey = f.TagKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to IsAvailableForCreditAllocation
	INSERT INTO @UpdateRecords
		SELECT TagKey
		FROM #Comparison
		WHERE cIsAvailableForCreditAllocation = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Tag
			SET IsAvailableForCreditAllocation = i.IsAvailableForCreditAllocation
				, ETLCommand = 'Update'
			FROM stage.qdm_Tag AS s
				INNER JOIN import.qdm_Tag AS i
					ON s.TagKey = i.TagKey
				INNER JOIN @UpdateRecords AS f
					ON s.TagKey = f.TagKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to IsAvailableForHoliday
	INSERT INTO @UpdateRecords
		SELECT TagKey
		FROM #Comparison
		WHERE cIsAvailableForHoliday = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Tag
			SET IsAvailableForHoliday = i.IsAvailableForHoliday
				, ETLCommand = 'Update'
			FROM stage.qdm_Tag AS s
				INNER JOIN import.qdm_Tag AS i
					ON s.TagKey = i.TagKey
				INNER JOIN @UpdateRecords AS f
					ON s.TagKey = f.TagKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to IsAvailableForLocation
	INSERT INTO @UpdateRecords
		SELECT TagKey
		FROM #Comparison
		WHERE cIsAvailableForLocation = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Tag
			SET IsAvailableForLocation = i.IsAvailableForLocation
				, ETLCommand = 'Update'
			FROM stage.qdm_Tag AS s
				INNER JOIN import.qdm_Tag AS i
					ON s.TagKey = i.TagKey
				INNER JOIN @UpdateRecords AS f
					ON s.TagKey = f.TagKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to IsAvailableForProfile
	INSERT INTO @UpdateRecords
		SELECT TagKey
		FROM #Comparison
		WHERE cIsAvailableForProfile = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Tag
			SET IsAvailableForProfile = i.IsAvailableForProfile
				, ETLCommand = 'Update'
			FROM stage.qdm_Tag AS s
				INNER JOIN import.qdm_Tag AS i
					ON s.TagKey = i.TagKey
				INNER JOIN @UpdateRecords AS f
					ON s.TagKey = f.TagKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to IsAvailableForRequestLimit
	INSERT INTO @UpdateRecords
		SELECT TagKey
		FROM #Comparison
		WHERE cIsAvailableForRequestLimit = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Tag
			SET IsAvailableForRequestLimit = i.IsAvailableForRequestLimit
				, ETLCommand = 'Update'
			FROM stage.qdm_Tag AS s
				INNER JOIN import.qdm_Tag AS i
					ON s.TagKey = i.TagKey
				INNER JOIN @UpdateRecords AS f
					ON s.TagKey = f.TagKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to IsAvailableForScheduleEntry 
	INSERT INTO @UpdateRecords
		SELECT TagKey
		FROM #Comparison
		WHERE cIsAvailableForScheduleEntry = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Tag
			SET IsAvailableForScheduleEntry = i.IsAvailableForScheduleEntry 
				, ETLCommand = 'Update'
			FROM stage.qdm_Tag AS s
				INNER JOIN import.qdm_Tag AS i
					ON s.TagKey = i.TagKey
				INNER JOIN @UpdateRecords AS f
					ON s.TagKey = f.TagKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to IsAvailableForSeries 
	INSERT INTO @UpdateRecords
		SELECT TagKey
		FROM #Comparison
		WHERE cIsAvailableForSeries = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Tag
			SET IsAvailableForSeries = i.IsAvailableForSeries 
				, ETLCommand = 'Update'
			FROM stage.qdm_Tag AS s
				INNER JOIN import.qdm_Tag AS i
					ON s.TagKey = i.TagKey
				INNER JOIN @UpdateRecords AS f
					ON s.TagKey = f.TagKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to IsAvailableForStaff
	INSERT INTO @UpdateRecords
		SELECT TagKey
		FROM #Comparison
		WHERE cIsAvailableForStaff = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Tag
			SET IsAvailableForStaff = i.IsAvailableForStaff
				, ETLCommand = 'Update'
			FROM stage.qdm_Tag AS s
				INNER JOIN import.qdm_Tag AS i
					ON s.TagKey = i.TagKey
				INNER JOIN @UpdateRecords AS f
					ON s.TagKey = f.TagKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to IsAvailableForStaffLocation
	INSERT INTO @UpdateRecords
		SELECT TagKey
		FROM #Comparison
		WHERE cIsAvailableForStaffLocation = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Tag
			SET IsAvailableForStaffLocation = i.IsAvailableForStaffLocation
				, ETLCommand = 'Update'
			FROM stage.qdm_Tag AS s
				INNER JOIN import.qdm_Tag AS i
					ON s.TagKey = i.TagKey
				INNER JOIN @UpdateRecords AS f
					ON s.TagKey = f.TagKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to IsAvailableForStaffTarget
	INSERT INTO @UpdateRecords
		SELECT TagKey
		FROM #Comparison
		WHERE cIsAvailableForStaffTarget = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Tag
			SET IsAvailableForStaffTarget = i.IsAvailableForStaffTarget
				, ETLCommand = 'Update'
			FROM stage.qdm_Tag AS s
				INNER JOIN import.qdm_Tag AS i
					ON s.TagKey = i.TagKey
				INNER JOIN @UpdateRecords AS f
					ON s.TagKey = f.TagKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to IsAvailableForTask
	INSERT INTO @UpdateRecords
		SELECT TagKey
		FROM #Comparison
		WHERE cIsAvailableForTask = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Tag
			SET IsAvailableForTask = i.IsAvailableForTask
				, ETLCommand = 'Update'
			FROM stage.qdm_Tag AS s
				INNER JOIN import.qdm_Tag AS i
					ON s.TagKey = i.TagKey
				INNER JOIN @UpdateRecords AS f
					ON s.TagKey = f.TagKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to IsFilterOnAdmin
	INSERT INTO @UpdateRecords
		SELECT TagKey
		FROM #Comparison
		WHERE cIsFilterOnAdmin = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Tag
			SET IsFilterOnAdmin = i.IsFilterOnAdmin
				, ETLCommand = 'Update'
			FROM stage.qdm_Tag AS s
				INNER JOIN import.qdm_Tag AS i
					ON s.TagKey = i.TagKey
				INNER JOIN @UpdateRecords AS f
					ON s.TagKey = f.TagKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to IsFilterEverywhereExceptAdmin
	INSERT INTO @UpdateRecords
		SELECT TagKey
		FROM #Comparison
		WHERE cIsFilterEverywhereExceptAdmin = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Tag
			SET IsFilterEverywhereExceptAdmin = i.IsFilterEverywhereExceptAdmin
				, ETLCommand = 'Update'
			FROM stage.qdm_Tag AS s
				INNER JOIN import.qdm_Tag AS i
					ON s.TagKey = i.TagKey
				INNER JOIN @UpdateRecords AS f
					ON s.TagKey = f.TagKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to IsPermissionCategory 
	INSERT INTO @UpdateRecords
		SELECT TagKey
		FROM #Comparison
		WHERE cIsPermissionCategory = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Tag
			SET IsPermissionCategory = i.IsPermissionCategory
				, ETLCommand = 'Update'
			FROM stage.qdm_Tag AS s
				INNER JOIN import.qdm_Tag AS i
					ON s.TagKey = i.TagKey
				INNER JOIN @UpdateRecords AS f
					ON s.TagKey = f.TagKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to IsSingleTaggingOnly
	INSERT INTO @UpdateRecords
		SELECT TagKey
		FROM #Comparison
		WHERE cIsSingleTaggingOnly = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Tag
			SET IsSingleTaggingOnly = i.IsSingleTaggingOnly
				, ETLCommand = 'Update'
			FROM stage.qdm_Tag AS s
				INNER JOIN import.qdm_Tag AS i
					ON s.TagKey = i.TagKey
				INNER JOIN @UpdateRecords AS f
					ON s.TagKey = f.TagKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to IsTTCMCategory
	INSERT INTO @UpdateRecords
		SELECT TagKey
		FROM #Comparison
		WHERE cIsTTCMCategory = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Tag
			SET IsTTCMCategory = i.IsTTCMCategory
				, ETLCommand = 'Update'
			FROM stage.qdm_Tag AS s
				INNER JOIN import.qdm_Tag AS i
					ON s.TagKey = i.TagKey
				INNER JOIN @UpdateRecords AS f
					ON s.TagKey = f.TagKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to IsUsedForFiltering
	INSERT INTO @UpdateRecords
		SELECT TagKey
		FROM #Comparison
		WHERE cIsUsedForFiltering = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Tag
			SET IsUsedForFiltering = i.IsUsedForFiltering
				, ETLCommand = 'Update'
			FROM stage.qdm_Tag AS s
				INNER JOIN import.qdm_Tag AS i
					ON s.TagKey = i.TagKey
				INNER JOIN @UpdateRecords AS f
					ON s.TagKey = f.TagKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to IsUsedForStats
	INSERT INTO @UpdateRecords
		SELECT TagKey
		FROM #Comparison
		WHERE cIsUsedForStats = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Tag
			SET IsUsedForStats = i.IsUsedForStats
				, ETLCommand = 'Update'
			FROM stage.qdm_Tag AS s
				INNER JOIN import.qdm_Tag AS i
					ON s.TagKey = i.TagKey
				INNER JOIN @UpdateRecords AS f
					ON s.TagKey = f.TagKey;

			DELETE FROM @UpdateRecords;
		END
	DROP TABLE #Comparison;
END;	-- OF CREATE PROCEDURE CALL

-- END OF FILE --