DECLARE @StartDate DATE;
DECLARE @EndDate DATE;
DECLARE @AsOfDate NVARCHAR(7);
DECLARE @CurrentDate DATE = GETDATE();
DECLARE @RecordType NCHAR(3) = IIF(DAY(@CurrentDate) <= 15, 'HE', 'MG');

IF @RecordType = 'MG'
	BEGIN
		SET @StartDate = DATEFROMPARTS(YEAR(@CurrentDate), MONTH(@CurrentDate), 1);
		SET @AsOfDate = CONCAT(YEAR(@CurrentDate), MONTH(@CurrentDate), '%');
	END

IF @RecordType = 'HE'
	BEGIN
		SET @StartDate = DATEADD(DAY, -DAY(@CurrentDate) + 1, DATEADD(MONTH, -1, @CurrentDate));
		SET @AsOfDate = CONCAT(YEAR(DATEADD(MONTH, -1, @CurrentDate)), MONTH(DATEADD(MONTH, -1, @CurrentDate)), '%');
	END

SET @EndDate = EOMONTH(@StartDate);

WITH FteCTE AS
(
	SELECT DISTINCT
		pos.PersonID
		, pos.PositionOccupantName
		, pos.FTE
		, pos.AsOfDate
	FROM APMDW.emp.Position AS pos
	WHERE AsOfDate = (SELECT MAX(AsOfDate) FROM APMDW.emp.Position WHERE AsOfDate LIKE @AsOfDate)
	  AND pos.PersonID <> 0
)
UPDATE dbo.FacultyAbsence
SET FTE = cte.FTE
	, ReportPayHours = Reports.dbo.fn_FteToWorkdayValue(cte.FTE)
	, ReportMonth = MONTH(src.ScheduleDate)
	, ReportYear = YEAR(src.ScheduleDate)
FROM Reports.dbo.FacultyAbsence AS src
	INNER JOIN FteCTE AS cte
		ON src.PersonID = cte.PersonID
WHERE src.FTE IS NULL;

IF @RecordType = 'MG'
BEGIN
	DECLARE @Validation TABLE
	(
		PersonID	INT
		, ScheduleDate		DATE
		, MultiPayCode		TINYINT
		, MultiPayHours		TINYINT
		, MultiFacultyGroup	TINYINT
	);

	INSERT INTO @Validation
		SELECT DISTINCT
			PersonID
			, ScheduleDate
			, MultiPayCode = COUNT(DISTINCT PayCode)
			, MultiPayHours = COUNT(DISTINCT ReportPayHours)
			, MultiFacultyGroup = COUNT(DISTINCT FacultyGroup)
		FROM Reports.dbo.FacultyAbsence
		WHERE ReportMonth = MONTH(@CurrentDate)
		  AND ReportYear = YEAR(@CurrentDate)
		  AND PayCode IN ('VACATION', 'SICK', 'FUNERAL', 'JURY DTY', 'EDU')
		  AND RecordType = 'MG'
		GROUP BY PersonID, ScheduleDate;
	
	WITH MultiFacultyGroupCTE AS
	(
		SELECT
			v.PersonID
			, v.ScheduleDate
			, fa.FacultyGroup
			, FacultyGroupOrder = ROW_NUMBER() OVER(PARTITION BY v.PersonID, v.ScheduleDate ORDER BY fa.FacultyGroup)
		FROM @Validation AS v
			INNER JOIN dbo.FacultyAbsence AS fa
				ON v.PersonID = fa.PersonID
					AND v.ScheduleDate = fa.ScheduleDate
		WHERE fa.RecordType = 'MG'
			AND MultiPayCode = 1
			AND MultiPayHours = 1
			AND MultiFacultyGroup > 1
	)
	, ErrorsCTE AS
	(
		SELECT
			PersonID
			, ScheduleDate
			, FacultyGroup
		FROM MultiFacultyGroupCTE
		WHERE FacultyGroupOrder > 1
	)
	UPDATE dbo.FacultyAbsence
	SET ProducedErrorYN = 'Y'
		, ErrorSource = 'MIG'
		, ErrorComment = 'Duplicate absence found - QGenda and CMC'
	FROM dbo.FacultyAbsence AS fa
		INNER JOIN ErrorsCTE AS cte
			ON fa.PersonID = cte.PersonID
				AND fa.ScheduleDate = cte.ScheduleDate
				AND fa.RecordType = 'MG'
				AND fa.FacultyGroup = cte.FacultyGroup;
END;

-- END OF FILE --