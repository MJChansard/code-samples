/*	FILE HEADER
 *		File Name:	USP import,usp_DoStagingTimeEvent.sql
 *		Author:		Matt Chansard
 *		Project:	QGenda Data Mart
 *
 *	DESCRIPTION
 *		This file contains the definition of a User-Defined Stored Procedure (USP) which performs
 *		the following tasks:
 *	 		- Identifies new records that do not yet exist in ANESCore
 *	 		- Identify records that exist but contain data that requires updating
 *
 *	COLUMN NOTES
 *		TimeEventDate			-- Originally named [Date]
 *		TimeEventWeekday		-- Originally named [DayOfWeek]
 *		ActualClockIn			-- Data source: [ActualClockInLocal]
 *		EffectiveClockIn		-- Data source: [EffectiveClockInLocal]
 *		ActualClockOut			-- Data source: [ActualClockOutLocal]
 *		EffectiveClockOut		-- Data source: [EffectiveClockOutLocal]
 */

-- Connect to ANES-ETL1
ALTER PROCEDURE import.usp_DoStagingTimeEvent
AS
BEGIN
	SET NOCOUNT ON;
	
	PRINT 'Removing struck records.';
	DECLARE @StruckRecords TABLE (TimePunchEventKey BIGINT);
	INSERT INTO @StruckRecords
		SELECT DISTINCT TimePunchEventKey
		FROM import.qdm_TimeEvent
		WHERE IsStruck = 'T';

	IF (SELECT COUNT(TimePunchEventKey) FROM @StruckRecords) > 0
	BEGIN
		UPDATE stage.qdm_TimeEvent
		SET ETLCommand = 'Delete'
		FROM stage.qdm_TimeEvent AS t
			INNER JOIN @StruckRecords AS fltr
				ON t.TimePunchEventKey = fltr.TimePunchEventKey;

		DELETE FROM import.qdm_TimeEvent
		WHERE IsStruck = 'T';
	END

	PRINT 'Identifying incomplete Time Events'
	DELETE FROM import.qdm_TimeEvent
	WHERE ActualClockOut IS NULL AND EffectiveClockOut IS NULL;
	
		
	PRINT 'Identifying new records';
	DECLARE @NewRecords TABLE (TimePunchEventKey BIGINT);
	INSERT INTO @NewRecords
		SELECT DISTINCT TimePunchEventKey
		FROM import.qdm_TimeEvent
		EXCEPT
		SELECT DISTINCT TimePunchEventKey
		FROM stage.qdm_TimeEvent;
			-- No need to filter [IsStruck] because those records already deleted

	PRINT 'Inserting new records into [stage.qdm_TimeEvent]';
	INSERT INTO stage.qdm_TimeEvent
		SELECT
			i.ScheduleEntryKey
			, i.TaskShiftKey
			, i.StaffKey
			, i.TaskKey
			, v.TimePunchEventKey
			, i.TimeEventDate			-- Source: [Date]
			, i.TimeEventWeekday		-- Source: [DayOfWeek]
			, i.ActualClockIn			-- Source: [ActualClockInLocal]
			, i.EffectiveClockIn		-- Source: [EffectiveClockInLocal]
			, i.ActualClockOut			-- Source: [ActualClockOutLocal]
			, i.EffectiveClockOut		-- Source: [EffectiveClockOutLocal]
			, i.Duration
			, i.IsStruck
			, i.IsEarly
			, i.IsLate
			, i.IsExcessiveDuration
			, i.IsExtended
			, i.IsUnplanned
			, i.FlagsResolved
			, i.Notes
			, i.LastModifiedDate
			, ETLCommand = 'New'
		FROM import.qdm_TimeEvent AS i
			INNER JOIN @NewRecords AS v
				ON i.TimePunchEventKey = v.TimePunchEventKey;

	
	PRINT 'Deleting dead records';
	DECLARE @DeadRecords TABLE (TimePunchEventKey BIGINT);
	INSERT INTO @DeadRecords
		SELECT DISTINCT TimePunchEventKey
		FROM stage.qdm_TimeEvent
		EXCEPT
		SELECT DISTINCT TimePunchEventKey
		FROM import.qdm_TimeEvent;

	UPDATE stage.qdm_TimeEvent
	SET ETLCommand = 'Delete'
	FROM stage.qdm_TimeEvent AS d
		INNER JOIN @DeadRecords AS f
			ON d.TimePunchEventKey = f.TimePunchEventKey;

	
	PRINT 'Building [#Comparison]';
	SELECT
		i.TimePunchEventKey

		, i.ScheduleEntryKey	AS iScheduleEntryKey
		, s.ScheduleEntryKey	AS sScheduleEntryKey
		, cScheduleEntryKey = IIF(i.ScheduleEntryKey = s.ScheduleEntryKey, 'Match', 'Conflict')

		, i.ActualClockIn AS iActualClockIn
		, s.ActualClockIn as sActualClockIn
		, cActualClockIn = IIF(i.ActualClockIn = s.ActualClockIn, 'Match', 'Conflict')

		, i.EffectiveClockIn AS iEffectiveClockIn
		, s.EffectiveClockIn AS sEffectiveClockIn
		, cEffectiveClockIn = IIF(i.EffectiveClockIn = s.EffectiveClockIn, 'Match', 'Conflict')

		, i.ActualClockOut AS iActualClockOut
		, s.ActualClockOut AS sActualClockOut
		, cActualClockOut = IIF(i.ActualClockOut = s.ActualClockOut, 'Match', 'Conflict')

		, i.EffectiveClockOut AS iEffectiveClockOut
		, s.EffectiveClockOut AS sEffectiveClockOut
		, cEffectiveClockOut = IIF(i.EffectiveClockOut = s.EffectiveClockOut, 'Match', 'Conflict')

		, i.Notes AS iNotes
		, s.Notes AS sNotes
		, cNotes =
			CASE
				WHEN i.Notes = s.Notes THEN 'Match'
				WHEN i.Notes IS NULL AND s.Notes IS NULL THEN 'Match'
				ELSE 'Conflict'
			END
		
		, i.LastModifiedDate AS iLastModifiedDate
		, s.LastModifiedDate AS sLastModifiedDate
		, cLastModifiedDate =
			CASE
				WHEN i.LastModifiedDate = s.LastModifiedDate THEN 'Match'
				WHEN i.LastModifiedDate IS NULL AND s.LastModifiedDate IS NULL THEN 'Match'
				ELSE 'Conflict'
			END
	INTO #Comparison
	FROM import.qdm_TimeEvent AS i
		INNER JOIN stage.qdm_TimeEvent AS s
			ON i.TimePunchEventKey = s.TimePunchEventKey
	WHERE NOT i.TimePunchEventKey IN (SELECT TimePunchEventKey FROM @NewRecords);

	DECLARE @UpdateRecords TABLE (TimePunchEventKey BIGINT);
	
	PRINT 'Applying corrections';

	-- Correct changes to ScheduleEntryKey
	INSERT INTO @UpdateRecords
		SELECT TimePunchEventKey
		FROM #Comparison
		WHERE cScheduleEntryKey = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_TimeEvent
			SET ScheduleEntryKey = i.ScheduleEntryKey
				, ETLCommand = 'Update'
			FROM stage.qdm_TimeEvent AS s
				INNER JOIN import.qdm_TimeEvent AS i
					ON s.TimePunchEventKey = i.TimePunchEventKey
				INNER JOIN @UpdateRecords AS f
					ON s.TimePunchEventKey = f.TimePunchEventKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to Actual Clock-In
	INSERT INTO @UpdateRecords
		SELECT TimePunchEventKey
		FROM #Comparison
		WHERE cActualClockIn = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_TimeEvent
			SET ActualClockIn = i.ActualClockIn
				, ETLCommand = 'Update'
			FROM stage.qdm_TimeEvent AS s
				INNER JOIN import.qdm_TimeEvent AS i
					ON s.TimePunchEventKey = i.TimePunchEventKey
				INNER JOIN @UpdateRecords AS f
					ON s.TimePunchEventKey = f.TimePunchEventKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to Effective Clock-In
	INSERT INTO @UpdateRecords
		SELECT TimePunchEventKey
		FROM #Comparison
		WHERE cEffectiveClockIn = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_TimeEvent
			SET EffectiveClockIn = i.EffectiveClockIn
				, ETLCommand = 'Update'
			FROM stage.qdm_TimeEvent AS s
				INNER JOIN import.qdm_TimeEvent AS i
					ON s.TimePunchEventKey = i.TimePunchEventKey
				INNER JOIN @UpdateRecords AS f
					ON s.TimePunchEventKey = f.TimePunchEventKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to Actual Clock-Out
	INSERT INTO @UpdateRecords
		SELECT TimePunchEventKey
		FROM #Comparison
		WHERE cActualClockOut = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_TimeEvent
			SET ActualClockOut = i.ActualClockOut
				, ETLCommand = 'Update'
			FROM stage.qdm_TimeEvent AS s
				INNER JOIN import.qdm_TimeEvent AS i
					ON s.TimePunchEventKey = i.TimePunchEventKey
				INNER JOIN @UpdateRecords AS f
					ON s.TimePunchEventKey = f.TimePunchEventKey;

			DELETE FROM @UpdateRecords;
		END	

	-- Correct changes to Effective Clock-Out
	INSERT INTO @UpdateRecords
		SELECT TimePunchEventKey
		FROM #Comparison
		WHERE cEffectiveClockOut = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_TimeEvent
			SET EffectiveClockOut = i.EffectiveClockOut
				, ETLCommand = 'Update'
			FROM stage.qdm_TimeEvent AS s
				INNER JOIN import.qdm_TimeEvent AS i
					ON s.TimePunchEventKey = i.TimePunchEventKey
				INNER JOIN @UpdateRecords AS f
					ON s.TimePunchEventKey = f.TimePunchEventKey;

			DELETE FROM @UpdateRecords;
		END		
	
	-- Correct changes to Notes
	INSERT INTO @UpdateRecords
		SELECT TimePunchEventKey
		FROM #Comparison
		WHERE cNotes = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_TimeEvent
			SET Notes = i.Notes
				, ETLCommand = 'Update'
			FROM stage.qdm_TimeEvent AS s
				INNER JOIN import.qdm_TimeEvent AS i
					ON s.TimePunchEventKey = i.TimePunchEventKey
				INNER JOIN @UpdateRecords AS f
					ON s.TimePunchEventKey = f.TimePunchEventKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to LastModified Date
	INSERT INTO @UpdateRecords
		SELECT TimePunchEventKey
		FROM #Comparison
		WHERE cLastModifiedDate = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_TimeEvent
			SET LastModifiedDate = i.LastModifiedDate
				, ETLCommand = 'Update'
			FROM stage.qdm_TimeEvent AS s
				INNER JOIN import.qdm_TimeEvent AS i
					ON s.TimePunchEventKey = i.TimePunchEventKey
				INNER JOIN @UpdateRecords AS f
					ON s.TimePunchEventKey = f.TimePunchEventKey;

			DELETE FROM @UpdateRecords;
		END

	
	
	DROP TABLE #Comparison;
END;	-- OF CREATE PROCEDURE CALL
	-- Deployed on 07-07-2022	

-- END OF FILE --