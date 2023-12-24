SET NOCOUNT ON;

DECLARE @CurrentDate DATE = GETDATE();
SELECT DISTINCT
	PersonID
	, CONVERT(NVARCHAR, ScheduleDate, 101)	AS ScheduleDate
	, StartTime = IIF(FacultyGroup = 'CMC', '07:00', '08:00')
	, PayCode
	, ReportPayHours	AS PayCodeHours
	, ''	AS Comment
	, ''	AS CostCenterNumber
	, 'Y'	AS UpdateOverride
FROM dbo.FacultyAbsence
WHERE ReportMonth = MONTH(@CurrentDate)
  AND ReportYear = YEAR(@CurrentDate)
  AND PayCode IN ('VACATION', 'SICK', 'FUNERAL', 'JURY DTY')
  AND ProducedErrorYN = 'N'
ORDER BY ScheduleDate, PersonID;

-- END OF FILE --