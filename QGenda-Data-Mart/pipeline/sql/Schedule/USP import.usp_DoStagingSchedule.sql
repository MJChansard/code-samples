/*	FILE HEADER
 *		File Name:	USP import.usp_DoStagingSchedule.sql
 *		Author:		Matt Chansard
 *		Project:	QGenda Data Mart
 *
 *	DESCRIPTION
 *		This file contains the definition of a User-Defined Stored Procedure (USP) which performs
 *		the following tasks:
 *	 		- Identifies new records that do not yet exist in ANESCore
 *	 		- Identify records that exist but contain data that requires updating
 */

ALTER PROCEDURE import.usp_DoStagingSchedule
AS
BEGIN
	SET NOCOUNT ON;

	PRINT 'Identifying deleted records';
	DECLARE @DeletedRecords TABLE (ScheduleKey UNIQUEIDENTIFIER);
	INSERT INTO @DeletedRecords
		SELECT DISTINCT ScheduleKey
		FROM stage.qdm_Schedule
		EXCEPT
		SELECT DISTINCT ScheduleKey
		FROM import.qdm_Schedule;

	UPDATE stage.qdm_Schedule
	SET ETLCommand = 'Delete'
	FROM stage.qdm_Schedule AS schd
		INNER JOIN @DeletedRecords AS f
			ON schd.ScheduleKey = f.ScheduleKey;

	PRINT 'Identifying struck records';
	DECLARE @StruckRecords TABLE (ScheduleKey UNIQUEIDENTIFIER);
	INSERT INTO @StruckRecords
		SELECT DISTINCT ScheduleKey
		FROM import.qdm_Schedule
		WHERE IsStruck = 'T';

	UPDATE stage.qdm_Schedule
	SET ETLCommand = 'Delete'
	FROM stage.qdm_Schedule AS t
		INNER JOIN @StruckRecords AS fltr
			ON t.ScheduleKey = fltr.ScheduleKey;
	PRINT 'Struck records marked for deletion.'

	DELETE FROM import.qdm_Schedule
	WHERE IsStruck = 'T';

	DECLARE @NewRecords TABLE (ScheduleKey UNIQUEIDENTIFIER);
	INSERT INTO @NewRecords
		SELECT DISTINCT ScheduleKey
		FROM import.qdm_Schedule
		EXCEPT
		SELECT DISTINCT ScheduleKey
		FROM stage.qdm_Schedule;

	INSERT INTO stage.qdm_Schedule
		SELECT
			v.ScheduleKey
			, i.TaskShiftKey
			, i.StaffKey
			, i.TaskKey
			, i.ScheduleDate
			, i.StartDate
			, i.StartTime
			, i.EndDate
			, i.EndTime
			, i.TaskName
			, i.StaffFName
			, i.StaffLName
			, i.Credit
			, i.TaskIsPrintStart
			, i.TaskIsPrintEnd
			, i.IsCred
			, i.IsLocked
			, i.IsPublished
			, i.IsStruck
			, i.Notes
			, 'New' AS ETLCommand
		FROM import.qdm_Schedule AS i
			INNER JOIN @NewRecords AS v
				ON i.ScheduleKey = v.ScheduleKey;

	DECLARE @DeadRecords TABLE (ScheduleKey UNIQUEIDENTIFIER);
	INSERT INTO @DeadRecords
		SELECT DISTINCT ScheduleKey
		FROM stage.qdm_Schedule
		EXCEPT
		SELECT DISTINCT ScheduleKey
		FROM import.qdm_Schedule;

	UPDATE stage.qdm_Schedule
	SET ETLCommand = 'Delete'
	FROM stage.qdm_Schedule AS d
		INNER JOIN @DeadRecords AS f
			ON d.ScheduleKey = f.ScheduleKey;
	
	SELECT
		i.ScheduleKey

		, i.StartDate AS iStartDate
		, s.StartDate AS sStartDate
		, cStartDate = IIF(i.StartDate = s.StartDate, 'Match', 'Conflict')

		, i.StartTime AS iStartTime
		, s.StartTime AS sStartTime
		, cStartTime = 
			CASE
				WHEN i.StartTime = s.StartTime THEN 'Match'
				ELSE 'Conflict'
			END

		, i.EndDate AS iEndDate
		, s.EndDate AS sEndDate
		, cEndDate = IIF(i.EndDate = s.EndDate, 'Match', 'Conflict')

		, i.EndTime AS iEndTime
		, s.EndTime AS sEndTime
		, cEndTime =
			CASE
				WHEN i.EndTime = s.EndTime THEN 'Match'
				ELSE 'Conflict'
			END
		, i.TaskName AS iTaskName
		, s.TaskName AS sTaskName
		, cTaskName = IIF(i.TaskName = s.TaskName, 'Match', 'Conflict')

		, i.StaffLName AS iStaffLName
		, s.StaffLName AS sStaffLName
		, cStaffLName = IIF(i.StaffLName = s.StaffLName, 'Match', 'Conflict')

		, i.Credit AS iCredit
		, s.Credit AS sCredit
		, cCredit = IIF(i.Credit = s.Credit, 'Match', 'Conflict')
		
		, i.Notes AS iNotes
		, s.Notes AS sNotes
		, cNotes =
			CASE
				WHEN i.Notes = s.Notes THEN 'Match'
				ELSE 'Conflict'
			END
	INTO #Comparison
	FROM import.qdm_Schedule AS i
		INNER JOIN stage.qdm_Schedule AS s
			ON i.ScheduleKey = s.ScheduleKey
	WHERE NOT i.ScheduleKey IN (SELECT ScheduleKey from @NewRecords);

	DECLARE @UpdateRecords TABLE (ScheduleKey UNIQUEIDENTIFIER);
	
	-- Correct changes to Start Dates
	INSERT INTO @UpdateRecords
		SELECT ScheduleKey
		FROM #Comparison
		WHERE cStartDate = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Schedule
			SET StartDate = i.StartDate
				, ETLCommand = 'Update'
			FROM stage.qdm_Schedule AS s
				INNER JOIN import.qdm_Schedule AS i
					ON s.ScheduleKey = i.ScheduleKey
				INNER JOIN @UpdateRecords AS f
					ON s.ScheduleKey = f.ScheduleKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to Start Times
	INSERT INTO @UpdateRecords
		SELECT ScheduleKey
		FROM #Comparison
		WHERE cStartTime = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Schedule
			SET StartTime = i.StartTime
				, ETLCommand = 'Update'
			FROM stage.qdm_Schedule AS s
				INNER JOIN import.qdm_Schedule AS i
					ON s.ScheduleKey = i.ScheduleKey
				INNER JOIN @UpdateRecords AS f
					ON s.ScheduleKey = f.ScheduleKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to End Dates
	INSERT INTO @UpdateRecords
		SELECT ScheduleKey
		FROM #Comparison
		WHERE cEndDate = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Schedule
			SET EndDate = i.EndDate
				, ETLCommand = 'Update'
			FROM stage.qdm_Schedule AS s
				INNER JOIN import.qdm_Schedule AS i
					ON s.ScheduleKey = i.ScheduleKey
				INNER JOIN @UpdateRecords AS f
					ON s.ScheduleKey = f.ScheduleKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to End Times
	INSERT INTO @UpdateRecords
		SELECT ScheduleKey
		FROM #Comparison
		WHERE cEndTime = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Schedule
			SET EndTime = i.EndTime
				, ETLCommand = 'Update'
			FROM stage.qdm_Schedule AS s
				INNER JOIN import.qdm_Schedule AS i
					ON s.ScheduleKey = i.ScheduleKey
				INNER JOIN @UpdateRecords AS f
					ON s.ScheduleKey = f.ScheduleKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to Task Names
	INSERT INTO @UpdateRecords
		SELECT ScheduleKey
		FROM #Comparison
		WHERE cTaskName = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Schedule
			SET TaskName = i.TaskName
				, ETLCommand = 'Update'
			FROM stage.qdm_Schedule AS s
				INNER JOIN import.qdm_Schedule AS i
					ON s.ScheduleKey = i.ScheduleKey
				INNER JOIN @UpdateRecords AS f
					ON s.ScheduleKey = f.ScheduleKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to Staff Last Name
	INSERT INTO @UpdateRecords
		SELECT ScheduleKey
		FROM #Comparison
		WHERE cStaffLName = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Schedule
			SET StaffLName = i.StaffLName
				, ETLCommand = 'Update'
			FROM stage.qdm_Schedule AS s
				INNER JOIN import.qdm_Schedule AS i
					ON s.ScheduleKey = i.ScheduleKey
				INNER JOIN @UpdateRecords AS f
					ON s.ScheduleKey = f.ScheduleKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to Credit
	INSERT INTO @UpdateRecords
		SELECT ScheduleKey
		FROM #Comparison
		WHERE cCredit = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Schedule
			SET Credit = i.Credit
				, ETLCommand = 'Update'
			FROM stage.qdm_Schedule AS s
				INNER JOIN import.qdm_Schedule AS i
					ON s.ScheduleKey = i.ScheduleKey
				INNER JOIN @UpdateRecords AS f
					ON s.ScheduleKey = f.ScheduleKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to Notes
	INSERT INTO @UpdateRecords
		SELECT ScheduleKey
		FROM #Comparison
		WHERE cNotes = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Schedule
			SET Notes = i.Notes
				, ETLCommand = 'Update'
			FROM stage.qdm_Schedule AS s
				INNER JOIN import.qdm_Schedule AS i
					ON s.ScheduleKey = i.ScheduleKey
				INNER JOIN @UpdateRecords AS f
					ON s.ScheduleKey = f.ScheduleKey;

			DELETE FROM @UpdateRecords;
		END
	

	DECLARE @OrphanRecords TABLE (ScheduleKey UNIQUEIDENTIFIER);
	INSERT INTO @OrphanRecords
		SELECT DISTINCT ScheduleKey
		FROM stage.qdm_Schedule
		WHERE NOT ETLCommand IN ('New', 'Update', 'Delete')
		EXCEPT
		SELECT DISTINCT ScheduleKey
		FROM import.qdm_Schedule;

	IF (SELECT COUNT(ScheduleKey) FROM @OrphanRecords) > 0
		UPDATE stage.qdm_Schedule
		SET ETLCommand = 'Delete'
		FROM stage.qdm_Schedule AS t
			INNER JOIN @OrphanRecords AS fltr
				ON t.ScheduleKey = fltr.ScheduleKey;

	DROP TABLE #Comparison;
END;	-- OF CREATE PROCEDURE CALL

-- END OF FILE --