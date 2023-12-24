/* FILE HEADER
 *	File Name:  FUNCTION dbo,fn_FteToWorkdayValue.py
 *	Author:     Matt Chansard
 *	Project:    QGenda Migration (MTQT)
 *
 *	DESCRIPTION
 *	This function is used to convert a FTE decimal value to an amount of hours and minutes that an
 *	employee is expected to work each day.  For example, a FTE value of 1.0 converts to 8:00 since
 *	a full-time employee works 40 hours a week, which translates to 8 hours each work day.
 *
*/
CREATE FUNCTION dbo.fn_FteToWorkdayValue (@FTE DECIMAL(3,2))
RETURNS NCHAR(4)
WITH EXECUTE AS CALLER
AS
BEGIN
	IF @FTE > 1.0
		RETURN '-1';
	ELSE
		DECLARE @CalculateDayHours DECIMAL(5,2) = (40.0 * @FTE) / 5;
		DECLARE @DayHoursString NVARCHAR(6) = CAST(@CalculateDayHours AS NVARCHAR(8));
		DECLARE @DecimalIndex INT = CHARINDEX('.', @DayHoursString, 1);

		DECLARE @DayHours INT = CAST(SUBSTRING(@DayHoursString, 1, @DecimalIndex - 1) AS INT);
		DECLARE @DayMinutes INT = 60 * CAST(SUBSTRING(@DayHoursString, @DecimalIndex, LEN(@DayHoursString) - @DecimalIndex) AS DECIMAL(3,2));

		RETURN CONCAT(
				CAST(@DayHours AS NCHAR(1)),
				':',
				IIF(@DayMinutes = 0, '00', CAST(@DayMinutes AS NCHAR(2)))
			);
END;

-- END OF FILE --