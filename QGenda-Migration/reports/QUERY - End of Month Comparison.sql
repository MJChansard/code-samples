USE Reports;

DECLARE @CurrentDate DATE = GETDATE();
DECLARE @ReportMonth INT = MONTH(@CurrentDate) - 1;
DECLARE @ReportYear INT = YEAR(@CurrentDate);


-- End of Month Report Data
SELECT
	r.PersonID
	, r.FacultyFullName
	, r.FTE
	, r.ScheduleDate
	, r.PayCode
	, r.ReportPayHours
	, r.FacultyGroup
	, p.SupervisorName
FROM dbo.FacultyAbsence AS r
	INNER JOIN APMDW.emp.Position AS p
		ON r.PersonID = p.PersonID
		AND AsOfDate = 
			(
				SELECT MAX(AsOfDate)
				FROM APMDW.emp.Position
				WHERE SUBSTRING(CAST(AsOfDate AS NVARCHAR(8)), 1, 4) = @ReportYear
				  AND SUBSTRING(CAST(AsOfDate AS NVARCHAR(8)), 5, 2) = @ReportMonth
			)
WHERE ReportMonth = @ReportMonth
  AND ReportYear = @ReportYear
  AND RecordType = 'HE'
  AND PayCode IN ('VACATION', 'SICK', 'JURY DTY', 'FUNERAL')
ORDER BY r.FacultyGroup DESC, r.FacultyFullName, r.ScheduleDate;

WITH MigrationCTE AS
(
	SELECT *
	FROM dbo.FacultyAbsence
	WHERE ReportMonth = @ReportMonth
	  AND ReportYear = @ReportYear
	  AND RecordType = 'MG'
	  AND PayCode IN ('VACATION', 'SICK', 'JURY DTY', 'FUNERAL')
)
, EndOfMonthCTE AS
(
	SELECT *
	FROM dbo.FacultyAbsence
	WHERE ReportMonth = @ReportMonth
	  AND ReportYear = @ReportYear
	  AND RecordType = 'HE'
	  AND PayCode IN ('VACATION', 'SICK', 'JURY DTY', 'FUNERAL')
)
, ReportCTE AS
(
	SELECT
		mig.PersonID	AS mPersonNumber
		, mig.FacultyFullName	AS mFacultyFullName
		, mig.FTE				AS mFTE
		, mig.ScheduleDate		AS mScheduleDate
		, mig.PayCode			AS mPayCode
		, mig.ReportPayHours	AS mPayHourAmount
		, mig.FacultyGroup		AS mFacultyGroup

		, eom.PersonID	AS ePersonNumber
		, eom.FacultyFullName	AS eFacultyFullName
		, eom.FTE				AS eFTE
		, eom.ScheduleDate		AS eScheduleDate
		, eom.PayCode			AS ePayCode
		, eom.ReportPayHours	AS eHours
		, eom.FacultyGroup		AS eFacultyGroup

		, Finding = 
			CASE
				WHEN mig.PersonID IS NULL
				THEN 'Record added EOM'

				WHEN eom.PersonID IS NULL
				THEN 'Record removed by EOM'

				WHEN mig.ReportPayHours <> eom.ReportPayHours
				  OR (mig.ReportPayHours IS NULL AND eom.ReportPayHours IS NOT NULL)
				  OR (mig.ReportPayHours IS NOT NULL AND eom.ReportPayHours IS NULL)
				  OR (mig.ReportPayHours IS NULL AND eom.ReportPayHours IS NULL)
				THEN 'Pay hour conflict'

				WHEN mig.FTE <> eom.FTE
				  OR (mig.FTE IS NULL AND eom.FTE IS NOT NULL)
				  OR (mig.FTE IS NOT NULL AND eom.FTE IS NULL)
				  OR (mig.FTE IS NULL AND eom.FTE IS NULL)
				THEN 'FTE change'

				ELSE 'Valid'
			END
	FROM MigrationCTE AS mig
		FULL OUTER JOIN EndOfMonthCTE AS eom
			ON mig.PersonID = eom.PersonID
			AND mig.ScheduleDate = eom.ScheduleDate
			AND mig.PayCode = eom.PayCode
)
--SELECT * FROM ReportCTE ORDER BY mFacultyFullName, eFacultyFullName, mScheduleDate, eScheduleDate;
SELECT
	cte.*
	, SupervisorName = IIF(cte.eFacultyGroup = 'CMC', 'Kimatian, Stephen', emp.SupervisorName)
FROM ReportCTE AS cte
	INNER JOIN APMDW.emp.Position AS emp
		ON IIF(cte.mPersonNumber IS NOT NULL, mPersonNumber, ePersonNumber) = emp.PersonID
		AND AsOfDate =
			(
				SELECT MAX(AsOfDate)
				FROM APMDW.emp.Position
				WHERE SUBSTRING(CAST(AsOfDate AS NVARCHAR(8)), 1, 4) = @ReportYear
				  AND SUBSTRING(CAST(AsOfDate AS NVARCHAR(8)), 5, 2) = @ReportMonth
			)
WHERE Finding <> 'Valid'
ORDER BY cte.eFacultyGroup, cte.eFacultyFullName, cte.eScheduleDate;

-- Find Holiday/Holiday Banked Discrepancies
WITH MigrationCTE AS
(
	SELECT *
	FROM dbo.FacultyAbsence
	WHERE ReportMonth = @ReportMonth
	  AND ReportYear = @ReportYear
	  AND RecordType = 'MG'
	  AND PayCode IN ('HOLIDAY', 'Holiday Banked')
)
, EndOfMonthCTE AS
(
	SELECT *
	FROM dbo.FacultyAbsence
	WHERE ReportMonth = @ReportMonth
	  AND ReportYear = @ReportYear
	  AND RecordType = 'HE'
	  AND PayCode IN ('HOLIDAY', 'Holiday Banked')
)
SELECT DISTINCT
	mig.PersonID	AS mPersonNumber
	, mig.FacultyFullName	AS mName
	, mig.ScheduleDate		AS mScheduleDate
	, mig.PayCode			AS mPayCode
	, mig.ReportPayHours	AS mHours
	, mig.FTE				AS mFTE
	, mig.FacultyGroup		AS mFacultyGroup

	, eom.PersonID	AS ePersonNumber
	, eom.FacultyFullName	AS eName
	, eom.ScheduleDate		AS eScheduleDate
	, eom.PayCode			AS ePayCode
	, eom.ReportPayHours	AS eHours
	, eom.FTE				AS eFTE
	, eom.FacultyGroup		AS eFacultyGroup

	, Finding = 
		CASE
			WHEN mig.PersonID IS NULL
			THEN 'Added EOM'

			WHEN eom.PersonID IS NULL
			THEN 'Removed by EOM'

			ELSE 'Valid'
		END
FROM MigrationCTE AS mig
	FULL OUTER JOIN EndOfMonthCTE AS eom
		ON mig.PersonID = eom.PersonID
		AND mig.ScheduleDate = eom.ScheduleDate
		AND mig.PayCode = eom.PayCode
ORDER BY eFacultyGroup DESC, eName, eScheduleDate;

-- Find Education, FMLA discrepancies
WITH MigrationCTE AS
(
	SELECT *
	FROM dbo.FacultyAbsence
	WHERE ReportMonth = @ReportMonth
	  AND ReportYear = @ReportYear
	  AND RecordType = 'MG'
	  AND PayCode IN ('FMLA', 'EDUCATION')
)
, EndOfMonthCTE AS
(
	SELECT *
	FROM dbo.FacultyAbsence
	WHERE ReportMonth = @ReportMonth
	  AND ReportYear = @ReportYear
	  AND RecordType = 'HE'
	  AND PayCode IN ('FMLA', 'EDUCATION')
)
SELECT DISTINCT
	mig.PersonID	AS mPersonNumber
	, mig.FacultyFullName	AS mName
	, mig.ScheduleDate		AS mScheduleDate
	, mig.PayCode			AS mPayCode
	, mig.ReportPayHours	AS mHours
	, mig.FTE				AS mFTE
	, mig.FacultyGroup		AS mFacultyGroup

	, eom.PersonID	AS ePersonNumber
	, eom.FacultyFullName	AS eName
	, eom.ScheduleDate		AS eScheduleDate
	, eom.PayCode			AS ePayCode
	, eom.ReportPayHours	AS eHours
	, eom.FTE				AS eFTE
	, eom.FacultyGroup		AS eFacultyGroup

	, Finding = 
		CASE
			WHEN mig.PersonID IS NULL
			THEN 'Added EOM'

			WHEN eom.PersonID IS NULL
			THEN 'Removed by EOM'

			ELSE 'Valid'
		END
FROM MigrationCTE AS mig
	FULL OUTER JOIN EndOfMonthCTE AS eom
		ON mig.PersonID = eom.PersonID
		AND mig.ScheduleDate = eom.ScheduleDate
		AND mig.PayCode = eom.PayCode
ORDER BY eFacultyGroup DESC, eName, eScheduleDate;

/*
WITH MigrationCTE AS
(
	SELECT
		PersonID
		, ScheduleDate
		, PayCode
	FROM dbo.FacultyAbsence
	WHERE ReportMonth = @ReportMonth
	  AND ReportYear = @ReportYear
	  AND RecordType = 'MG'
	  AND PayCode IN ('VACATION', 'SICK', 'JURY DTY', 'FUNERAL')
)
, EndOfMonthCTE AS
(
	SELECT
		PersonID
		, ScheduleDate
		, PayCode
	FROM dbo.FacultyAbsence
	WHERE ReportMonth = @ReportMonth
	  AND ReportYear = @ReportYear
	  AND RecordType = 'HE'
	  AND PayCode IN ('VACATION', 'SICK', 'JURY DTY', 'FUNERAL')
)
SELECT *
FROM MigrationCTE
EXCEPT
SELECT *
FROM EndOfMonthCTE;
*/