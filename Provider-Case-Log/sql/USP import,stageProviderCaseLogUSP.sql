
ALTER PROCEDURE import.usp_DoStagingProviderCaseLogs
AS
BEGIN
	SET NOCOUNT ON;
	
	DECLARE @NewRecords			TABLE (CaseID	INT);
	DECLARE @DropRecords		TABLE (CaseID	INT);
	DECLARE @CompareRecords		TABLE (CaseID	INT);
	DECLARE @RestructureRecords	TABLE (CaseID	INT);
	
	-- New Records
	INSERT INTO @NewRecords
		SELECT DISTINCT CaseID
		FROM import.ProviderCaseLog_OpTime
		EXCEPT
		SELECT DISTINCT CaseID
		FROM stage.ProviderCaseLog;

	INSERT INTO stage.ProviderCaseLog
		SELECT DISTINCT
			f.CaseID
			, s.LineNumber
			, s.CaseDate
			, s.ProviderEpicID
			, s.ProviderType
			, s.ProviderAnesthesiaStart
			, s.ProviderAnesthesiaStop
			, s.PatientDischarged
			, s.LogStatus
			, s.CaseStatus
			, s.LastUpdate
			, 'New' AS ETLCommand
		FROM import.ProviderCaseLog_OpTime AS s
			INNER JOIN @NewRecords AS f
				ON s.CaseID = f.CaseID;


	-- Delete records
	INSERT INTO @DropRecords
		SELECT DISTINCT CaseID
		FROM stage.ProviderCaseLog
		EXCEPT
		SELECT DISTINCT CaseID
		FROM import.ProviderCaseLog_OpTime;

	UPDATE stage.ProviderCaseLog
	SET ETLCommand = 'Delete'
	FROM stage.ProviderCaseLog AS s
		INNER JOIN @DropRecords AS f
			ON s.CaseID = f.CaseID;


	-- Update Records
	INSERT INTO @CompareRecords
		SELECT CaseID FROM import.ProviderCaseLog_OpTime
		UNION
		SELECT CaseID FROM stage.ProviderCaseLog;

	-- Flag records with conflicting structure for Update 
	WITH ImportCountCTE AS
	(
		SELECT DISTINCT
			d.CaseID
			, MAX(d.LineNumber) AS CaseLineCount
		FROM import.ProviderCaseLog_OpTime AS d
			INNER JOIN @CompareRecords AS f
				ON d.CaseID = f.CaseID
		GROUP BY d.CaseID
		
	)
	, StageCountCTE AS
	(
		SELECT DISTINCT
			d.CaseID
			, MAX(d.LineNumber) AS CaseLineCount
		FROM stage.ProviderCaseLog AS d
			INNER JOIN @CompareRecords AS f
				ON d.CaseID = f.CaseID
		GROUP BY d.CaseId
	)
	, ComparisonCTE AS
	(
		SELECT
			i.CaseID
			, i.CaseLineCount	AS iCaseLineCount
			, s.CaseLineCount	AS sCaseLineCount
			, cCaseLineCount = IIF(i.CaseLineCount = s.CaseLineCount, 'Match', 'Conflict')
		FROM ImportCountCTE AS i
			INNER JOIN StageCountCTE AS s
				ON i.CaseID = s.CaseID
	)
	INSERT INTO @RestructureRecords
		SELECT DISTINCT
			CaseID
		FROM ComparisonCTE
		WHERE cCaseLineCount = 'Conflict';
	
	DELETE FROM stage.ProviderCaseLog
	WHERE CaseID IN (SELECT CaseID FROM @RestructureRecords);

	INSERT INTO stage.ProviderCaseLog
		SELECT
			f.CaseID
			, s.LineNumber
			, s.CaseDate
			, s.ProviderEpicID
			, s.ProviderType
			, s.ProviderAnesthesiaStart
			, s.ProviderAnesthesiaStop
			, s.PatientDischarged
			, s.LogStatus
			, s.CaseStatus
			, s.LastUpdate
			, 'Update' AS ETLCommand
		FROM import.ProviderCaseLog_OpTime AS s
			INNER JOIN @RestructureRecords AS f
				ON s.CaseID = f.CaseID;

	-- Remove restructured records from comparison since changes have already been staged
	DELETE FROM @CompareRecords
	WHERE CaseID IN (SELECT CaseID FROM @RestructureRecords);

	-- Build #Comparison
	SELECT DISTINCT
		i.CaseID		AS iCaseID
		, s.CaseID		AS sCaseID
		, i.LineNumber	AS iLineNumber
		, s.LineNumber	AS sLineNumber
		
		, i.CaseDate 	AS iCaseDate
		, s.CaseDate	AS sCaseDate
		, cCaseDate = IIF(i.CaseDate = s.CaseDate, 'Match', 'Conflict')

		, i.ProviderEpicID	AS iProviderEpicID
		, s.ProviderEpicID	AS sProviderEpicID
		, cProviderEpicID = IIF(i.ProviderEpicID = s.ProviderEpicID, 'Match', 'Conflict')
		
		, i.ProviderType	AS iProviderType
		, s.ProviderType	AS sProviderType
		, cProviderType = IIF(i.ProviderType = s.ProviderType, 'Match', 'Conflict')

		, i.ProviderAnesthesiaStart AS iProviderAnesthesiaStart
		, s.ProviderAnesthesiaStart	AS sProviderAnesthesiaStart
		, cProviderAnesthesiaStart = IIF(i.ProviderAnesthesiaStart = s.ProviderAnesthesiaStart, 'Match', 'Conflict')

		, i.ProviderAnesthesiaStop AS iProviderAnesthesiaStop
		, s.ProviderAnesthesiaStop AS sProviderAnestheisaStop
		, cProviderAnesthesiaStop = IIF(i.ProviderAnesthesiaStop = s.ProviderAnesthesiaStop, 'Match', 'Conflict')

		, i.PatientDischarged AS iPatientDischarged
		, s.PatientDischarged AS sPatientDischarged
		, cPatientDischarged = IIF(i.PatientDischarged = s.PatientDischarged, 'Match', 'Conflict')

		, i.LogStatus	AS iLogStatus
		, s.LogStatus	AS sLogStatus
		, cLogStatus = IIF(i.LogStatus = s.LogStatus, 'Match', 'Conflict')

		, i.CaseStatus	AS iCaseStatus
		, s.CaseStatus	AS sCaseStatus
		, cCaseStatus = IIF(i.CaseStatus = s.CaseStatus, 'Match', 'Conflict')

		, i.LastUpdate	AS iLastUpdate
		, s.LastUpdate	AS sLastUpdate
		, cLastUpdate = IIF(i.LastUpdate = s.LastUpdate, 'Match', 'Conflict')

	INTO #Comparison
	FROM import.ProviderCaseLog_OpTime AS i
		INNER JOIN stage.ProviderCaseLog AS s
			ON i.CaseID = s.CaseID
		INNER JOIN @CompareRecords AS f
			ON i.CaseId = f.CaseID
	WHERE NOT s.ETLCommand IS NULL
	ORDER BY i.CaseID, i.LineNumber;

	
	-- #CORRECTIONS
	/*	NOTE
	 *		This dynamic SQL solution performs the update provided below without the need to
	 *		duplicate the statement repeatedly while changing 2 or 3 column names.
	 *
	 *				INSERT INTO @UpdateRecords
	 *					SELECT iCaseID
	 *					FROM #Comparison
	 *					WHERE cCaseDate = 'Conflict';
	 *
	 *				IF (SELECT COUNT(*) FROM @UpdateRecords) > 0
	 *					BEGIN
	 *						UPDATE stage.ProviderCaseLog
	 *						SET CaseDate = i.CaseDate
	 *							, ETLCommand = 'Update'
	 *						FROM stage.ProviderCaseLog AS s
	 *							INNER JOIN #Import AS i
	 *								ON s.CaseID = i.CaseID
	 *							INNER JOIN @UpdateRecords AS f
	 *								ON s.CaseID = f.CaseID;
	 *
	 *						DELETE FROM @UpdateRecords;
	 *					END
	 */

	CREATE TABLE #UpdateRecords	(CaseID INT);
	CREATE TABLE #ColumnList	(ColumnNumber TINYINT, ColumnName	VARCHAR(25));

	INSERT INTO #ColumnList
		VALUES
		(1, 'CaseDate'),
		(2, 'ProviderEpicId'),
		(3, 'ProviderType'),
		(4, 'ProviderAnesthesiaStart'),
		(5, 'ProviderAnesthesiaStop'),
		(6, 'PatientDischarged'),
		(7, 'LogStatus'),
		(8, 'CaseStatus'),
		(9, 'LastUpdate');
	
	DECLARE @i TINYINT = 1;
	DECLARE @column VARCHAR(25);
	DECLARE @tsql NVARCHAR(MAX);
	DECLARE @ColumnQuery	NVARCHAR(100) = 'SET @columnOUT = (SELECT ColumnName FROM #ColumnList WHERE ColumnNumber = @RecordNumber);'
	DECLARE @ConflictQuery	NVARCHAR(150) = 'SELECT DISTINCT iCaseID FROM #Comparison WHERE c{} = ''Conflict'';'
	DECLARE @UpdateQuery	NVARCHAR(350) = 'UPDATE stage.ProviderCaseLog SET {} = i.{}, ETLCommand = ''Update'' FROM stage.ProviderCaseLog AS s INNER JOIN import.ProviderCaseLog_OpTime AS i ON s.CaseID = i.CaseID AND s.ProviderEpicID = i.ProviderEpicID INNER JOIN #UpdateRecords AS f ON s.CaseID = f.CaseID;'
	
	WHILE @i <= (SELECT COUNT(*) FROM #ColumnList)
	BEGIN
		EXECUTE sp_executesql
			@ColumnQuery
			, N'@RecordNumber TINYINT, @columnOUT NVARCHAR(25) OUTPUT'
			, @RecordNumber = @i
			, @columnOUT = @column OUTPUT;
	
		SET @tsql = REPLACE(@ConflictQuery, '{}', @column);
	
		INSERT INTO #UpdateRecords
			EXECUTE sp_executesql @tsql;
	
		IF (SELECT COUNT(*) FROM #UpdateRecords) > 0
		BEGIN 
			SET @tsql = REPLACE(@UpdateQuery, '{}', @column);
			EXECUTE sp_executesql @tsql;
			TRUNCATE TABLE #UpdateRecords;
		END
	
	SET @i = @i + 1;
	END;
	
	-- Clean up
	DROP TABLE #ColumnList;
	DROP TABLE #UpdateRecords;
	DROP TABLE #Comparison;

END;	-- OF CREATE PROCEDURE CALL

-- END OF FILE