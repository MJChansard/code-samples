/*	FILE HEADER
 *		File Name:	USP import,usp_DoStagingTask.sql
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


ALTER PROCEDURE import.usp_DoStagingTask
AS
BEGIN
	SET NOCOUNT ON;
	
	PRINT 'Identifying new records';
	DECLARE @NewRecords TABLE (TaskKey UNIQUEIDENTIFIER);
	INSERT INTO @NewRecords
		SELECT DISTINCT TaskKey
		FROM import.qdm_Task
		EXCEPT
		SELECT DISTINCT TaskKey
		FROM stage.qdm_Task;

	INSERT INTO stage.qdm_Task
		SELECT
			v.TaskKey
			, i.TaskName
			, i.TaskId
			, i.TaskAbbrev				-- Originally named [Abbrev]
			, i.TaskType				-- Originally named [Type]
			, i.DepartmentId
			, i.EmrId
			, i.StartDate
			, i.EndDate
			, i.ContactInformation
			, i.IsManual				-- Originally named [Manual]
			, i.RequireTimePunch
			, i.Notes
			, ETLCommand = 'New'
		FROM import.qdm_Task AS i
			INNER JOIN @NewRecords AS v
				ON i.TaskKey = v.TaskKey;

	DELETE FROM import.qdm_Task
	WHERE TaskKey IN (SELECT TaskKey FROM @NewRecords);
	
	PRINT 'Creating #Comparison'
	SELECT
		i.TaskKey

		, i.TaskName AS iTaskName
		, s.TaskName AS sTaskName
		, cTaskName = 
			CASE
				WHEN i.TaskName = s.TaskName					THEN 'Match'
				WHEN i.TaskName IS NULL AND s.TaskName IS NULL	THEN 'Match'
				ELSE 'Conflict'
			END

		, i.TaskId AS iTaskId
		, s.TaskId AS sTaskId
		, cTaskId =
			CASE
				WHEN i.TaskId = s.TaskId					THEN 'Match'
				WHEN i.TaskId IS NULL AND s.TaskId IS NULL	THEN 'Match'
				ELSE 'Conflict'
			END

		, i.TaskAbbrev AS iTaskAbbrev
		, s.TaskAbbrev AS sTaskAbbrev
		, cTaskAbbrev = 
			CASE
				WHEN i.TaskAbbrev = s.TaskAbbrev					THEN 'Match'
				WHEN i.TaskAbbrev IS NULL AND s.TaskAbbrev IS NULL	THEN 'Match'
				ELSE 'Conflict'
			END

		, i.DepartmentId AS iDepartmentId
		, s.DepartmentId AS sDepartmentId
		, cDepartmentId = 
			CASE
				WHEN i.DepartmentId = s.DepartmentId					THEN 'Match'
				WHEN i.DepartmentId IS NULL AND s.DepartmentId IS NULL	THEN 'Match'
				ELSE 'Conflict'
			END
		
		, i.StartDate AS iStartDate
		, s.StartDate AS sStartDate
		, cStartDate = 
			CASE
				WHEN i.StartDate = s.StartDate						THEN 'Match'
				WHEN i.StartDate IS NULL AND s.StartDate IS NULL	THEN 'Match'
				ELSE 'Conflict'
			END

		, i.EndDate AS iEndDate
		, s.EndDate AS sEndDate
		, cEndDate = 
			CASE
				WHEN i.EndDate = s.EndDate						THEN 'Match'
				WHEN i.EndDate IS NULL AND s.EndDate IS NULL	THEN 'Match'
				ELSE 'Conflict'
			END

		, i.IsManual AS iIsManual
		, s.IsManual AS sIsManual
		, cIsManual = 
			CASE
				WHEN i.IsManual = s.IsManual					THEN 'Match'
				WHEN i.IsManual IS NULL AND s.IsManual IS NULL	THEN 'Match'
				ELSE 'Conflict'
			END

		, i.RequireTimePunch AS iRequireTimePunch
		, s.RequireTimePunch AS sRequireTimePunch
		, cRequireTimePunch = 
			CASE
				WHEN i.RequireTimePunch = s.RequireTimePunch					THEN 'Match'
				WHEN i.RequireTimePunch IS NULL AND s.RequireTimePunch IS NULL	THEN 'Match'
				ELSE 'Conflict'
			END

		, i.Notes AS iNotes
		, s.Notes AS sNotes
		, cNotes =
			CASE
				WHEN i.Notes = s.Notes						THEN 'Match'
				WHEN i.Notes IS NULL AND s.Notes IS NULL	THEN 'Match'
				ELSE 'Conflict'
			END
	INTO #Comparison
	FROM import.qdm_Task AS i
		INNER JOIN stage.qdm_Task AS s
			ON i.TaskKey = s.TaskKey;

	-- # CORRECTIONS
	PRINT 'Applying corrections to [stage.qdm_Task]';
	DECLARE @UpdateRecords TABLE (TaskKey UNIQUEIDENTIFIER);
	
	-- Correct changes to TaskName
	INSERT INTO @UpdateRecords
		SELECT TaskKey
		FROM #Comparison
		WHERE cTaskName = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Task
			SET TaskName = i.TaskName
				, ETLCommand = 'Update'
			FROM stage.qdm_Task AS s
				INNER JOIN import.qdm_Task AS i
					ON s.TaskKey = i.TaskKey
				INNER JOIN @UpdateRecords AS f
					ON s.TaskKey = f.TaskKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to TaskId
	INSERT INTO @UpdateRecords
		SELECT TaskKey
		FROM #Comparison
		WHERE cTaskId = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Task
			SET TaskId = i.TaskId
				, ETLCommand = 'Update'
			FROM stage.qdm_Task AS s
				INNER JOIN import.qdm_Task AS i
					ON s.TaskKey = i.TaskKey
				INNER JOIN @UpdateRecords AS f
					ON s.TaskKey = f.TaskKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to TaskAbbrev
	INSERT INTO @UpdateRecords
		SELECT TaskKey
		FROM #Comparison
		WHERE cTaskAbbrev = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Task
			SET TaskAbbrev = i.TaskAbbrev
				, ETLCommand = 'Update'
			FROM stage.qdm_Task AS s
				INNER JOIN import.qdm_Task AS i
					ON s.TaskKey = i.TaskKey
				INNER JOIN @UpdateRecords AS f
					ON s.TaskKey = f.TaskKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to DepartmentId
	INSERT INTO @UpdateRecords
		SELECT TaskKey
		FROM #Comparison
		WHERE cDepartmentId = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Task
			SET DepartmentId = i.DepartmentId
				, ETLCommand = 'Update'
			FROM stage.qdm_Task AS s
				INNER JOIN import.qdm_Task AS i
					ON s.TaskKey = i.TaskKey
				INNER JOIN @UpdateRecords AS f
					ON s.TaskKey = f.TaskKey;

			DELETE FROM @UpdateRecords;
		END
	
	-- Correct changes to StartDate
	INSERT INTO @UpdateRecords
		SELECT TaskKey
		FROM #Comparison
		WHERE cStartDate = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Task
			SET StartDate = i.StartDate
				, ETLCommand = 'Update'
			FROM stage.qdm_Task AS s
				INNER JOIN import.qdm_Task AS i
					ON s.TaskKey = i.TaskKey
				INNER JOIN @UpdateRecords AS f
					ON s.TaskKey = f.TaskKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to EndDate
	INSERT INTO @UpdateRecords
		SELECT TaskKey
		FROM #Comparison
		WHERE cEndDate = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Task
			SET EndDate = i.EndDate
				, ETLCommand = 'Update'
			FROM stage.qdm_Task AS s
				INNER JOIN import.qdm_Task AS i
					ON s.TaskKey = i.TaskKey
				INNER JOIN @UpdateRecords AS f
					ON s.TaskKey = f.TaskKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to IsManual
	INSERT INTO @UpdateRecords
		SELECT TaskKey
		FROM #Comparison
		WHERE cIsManual = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Task
			SET IsManual = i.IsManual
				, ETLCommand = 'Update'
			FROM stage.qdm_Task AS s
				INNER JOIN import.qdm_Task AS i
					ON s.TaskKey = i.TaskKey
				INNER JOIN @UpdateRecords AS f
					ON s.TaskKey = f.TaskKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to RequireTimePunch
	INSERT INTO @UpdateRecords
		SELECT TaskKey
		FROM #Comparison
		WHERE cRequireTimePunch = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Task
			SET RequireTimePunch = i.RequireTimePunch
				, ETLCommand = 'Update'
			FROM stage.qdm_Task AS s
				INNER JOIN import.qdm_Task AS i
					ON s.TaskKey = i.TaskKey
				INNER JOIN @UpdateRecords AS f
					ON s.TaskKey = f.TaskKey;

			DELETE FROM @UpdateRecords;
		END

	-- Correct changes to Notes
	INSERT INTO @UpdateRecords
		SELECT TaskKey
		FROM #Comparison
		WHERE cNotes = 'Conflict';

	IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
		BEGIN
			UPDATE stage.qdm_Task
			SET Notes = i.Notes
				, ETLCommand = 'Update'
			FROM stage.qdm_Task AS s
				INNER JOIN import.qdm_Task AS i
					ON s.TaskKey = i.TaskKey
				INNER JOIN @UpdateRecords AS f
					ON s.TaskKey = f.TaskKey;

			DELETE FROM @UpdateRecords;
		END
	
	DROP TABLE #Comparison;
END;	-- OF CREATE PROCEDURE CALL

-- Deployed on 07-12-2022
-- END OF FILE --
