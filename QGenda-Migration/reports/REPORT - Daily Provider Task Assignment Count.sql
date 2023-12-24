USE QGenda;

DECLARE @Month	INT = MONTH(GETDATE());
DECLARE @Year	INT = YEAR(GETDATE());
WITH GatherCTE AS
(
	SELECT DISTINCT
		ScheduleKey
		, schd.ScheduleDate
		, schd.TaskKey
		, schd.TaskName
		, schd.StaffKey
		, stf.FirstName
		, stf.LastName

	FROM dbo.Schedule AS schd
		INNER JOIN dim.StaffMember AS stf
			ON schd.StaffKey = stf.StaffKey
		INNER JOIN dim.TaggedStaff AS tag
			ON stf.StaffKey = tag.StaffKey
	WHERE MONTH(schd.ScheduleDate) = @Month
	  AND YEAR(schd.ScheduleDate) = @Year
	  AND
	  (
		tag.EmployeeType_FacultyFullTime = 1 OR
		tag.EmployeeType_FacultyPartTime = 1
	  )
	  AND NOT schd.TaskKey IN
	  (
		'F0EEAC8A-82DE-4C87-81A1-0F3D66C5D941'		-- Schedule Change Requests
		, '8B7C6677-6C14-4A81-B61E-3000CF18EE3F'	-- Task Name: Lottery Week MD
		, '1B0048FB-3142-4C96-BA7E-515E6A8EE3DC'	-- Task Name: Lottery 2-Weeks MD
		, '6A0C5C03-51E3-4D4C-B2E4-968DE56347DE'	-- Task Name: Vacation Lottery #1
		, '7BDA5F95-996F-4D78-9E4C-ABAFBC022B29'	-- Task Name: Vacation Lottery #2
		, '9F22606D-C5B4-49CA-B1DE-064587B83CA0'	-- Task Name: Vacation Lottery #3
		, '42D1C008-B351-4759-B632-AE5D22AEB714'	-- Task Name: Vacation Lottery #4
		, 'B3F55AF6-DA15-46DA-B8A4-AF1523D19DAD'	-- Task Name: Vacation Lottery #5
		, '9F60B7CC-EF7C-4520-9257-45311C14E07E'	-- Task Name: Vacation Lottery #6
		, '8861B788-E00D-4C32-A4B3-454017A95F20'	-- Task Name: Vacation Lottery #7
		, 'B4B568C0-3048-4B67-B558-A9B594E640B6'	-- Task Name: Vacation Lottery #8
		, '877B0256-966B-4F13-B4D2-196082437F99'	-- Task Name: Vacation Lottery #9
		, 'B7E3EBED-3B5C-408B-B1C9-D1BD96ED410F'	-- Task Name: Vacation Lottery #10
		, '6E8FACB0-CDC0-40C3-8121-5658C514171E'	-- Task Name: Vacation Lottery #11
		, 'B86D0AC0-528B-4FB9-8795-D6A3E35CCDAA'	-- Task Name: Vacation Lottery #12
		, '50976E96-B945-4A45-B82D-0142A402C4F0'	-- Task Name: Vacation Lottery #13

		, '050C13AE-4D3B-47C9-840A-15E8A76B659C'	-- Task Name: Academic-0
		, '0aaaeb17-2489-48ff-94cd-1a223e8d76ce'	-- Task Name: Fellow Interview
		, '484403F1-C036-4553-A8E4-85ECBC5712DD'	-- Task Name: Meeting
		, 'A6E3509C-0F6A-4E2F-B57A-7C13AFFC119C'	-- Task Name: No Call
		, 'FCD2C3B6-3B65-488C-A966-0805A158E6D9'	-- Task Name: No Call - Neuro
		, '6EB3D931-02C4-4B4C-BD1F-E48E54F9FFA7'	-- Task Name: Orientation
		, '7BC0143B-037B-46F6-9D02-542EBAF6F136'	-- Task Name: Post Call MD
		, '1D8060B3-4215-4F0D-B33D-6CF85ED86640'	-- Task Name: Post Call-Liver

		, '7ED0D8C4-7F89-44F0-8B4F-FE29427709EC'	-- Task Name: lbl C-CVICU
		, '6BC63F17-ED53-4111-9601-C26B7125E8DA'	-- Task Name: lbl C-CVICU Nt
		, 'A1AB41D8-8780-43A3-9144-8FC33D8F1723'	-- Task Name: lbl C-NCCU
		, '05A61D6A-9800-4410-9EC2-46B599996A11'	-- Task Name: lbl C-OBD
		, 'CF302B8D-27ED-4645-B0E5-5CA6CEE6F8CF'	-- Task Name: lbl C-OBN
		, 'B442175F-CA2F-4563-AAAD-7CFB126DEC1C'	-- Task Name: lbl COV ICU
		, '35911166-5281-4335-BB34-D3FBEA65DC89'	-- Task Name: lbl COV ICU Nt
		, '8BB87212-4B8C-486F-B4B4-78F91CAD9985'	-- Task Name: lbl C-SICU
		, 'BB2BECB2-61C8-4E48-9D54-D1ABFE43901F'	-- Task Name: lbl C-SICU Nt
		, 'F2939F0B-A81E-4C22-BF9D-D2EA461B49D9'	-- Task Name: lbl P-NCCU
		, 'DA1D2F1F-770B-4183-A8C8-035BF909C326'	-- Task Name: lbl THD-NCCU
		, '78C6D78B-961E-44FC-9AFE-AFF67B1830D2'	-- Task Name: lbl THD-NCCU Nt
		, '927E287E-CDFF-411B-B5CB-C38ED3F993C8'	-- Task Name: lbl VA-ICU
		, 'FCC92D5D-8457-4A9A-A353-2D2A3F01E2D9'	-- Task Name: lbl Z-ICU
	  )
)
, CounterCTE AS
(
	SELECT
		*
		, AssignmentCount = ROW_NUMBER() OVER(PARTITION BY ScheduleDate, StaffKey ORDER BY TaskName)
	FROM GatherCTE
)
, IdentifyCTE AS
(
	SELECT
		StaffKey
		, ScheduleDate
		, MAX(AssignmentCount) AS AssignmentCount
	FROM CounterCTE AS d
	GROUP BY StaffKey, ScheduleDate
	HAVING MAX(AssignmentCount) > 1
)
, LeaveCTE AS
(
	SELECT
		StaffKey
		, ScheduleDate
	FROM CounterCTE AS d
	WHERE TaskKey IN 
	(
		'D9166442-41A0-4B55-98E8-A87BF450BF5D'		-- Task Name: Funeral
		, 'F236856F-03DC-4B5D-8A00-904D5689C9C2'	-- Task Name: Holiday
		, '396AF91E-F77C-436E-92C9-5002878FC337'	-- Task Name: Jury Duty
		, 'B96AA5BD-D6D6-4ADA-8FF5-174207BF8745'	-- Task Name: Scheduled Sick
		, '8B4CE19F-0EAA-4F8B-BCB4-763BB3A2DFD3'	-- Task Name: Unscheduled Sick
		, 'EFFA2FF7-D6D5-40C9-A910-436A8FCD3C52'	-- Task Name: Vacation- MD
	)
)
SELECT 
	d.*
FROM CounterCTE AS d
	INNER JOIN IdentifyCTE AS fltr
		ON d.StaffKey = fltr.StaffKey AND d.ScheduleDate = fltr.ScheduleDate
	INNER JOIN LeaveCTE AS lv 
		ON d.StaffKey = lv.StaffKey AND d.ScheduleDate = lv.ScheduleDate;

--ORDER BY ScheduleDate, StaffKey;