/*	FILE HEADER
 *		File Name:	USP import,usp_DoStagingTaskTags.sql
 *		Author:		Matt Chansard
 *		Project:	QGenda Data Mart
 *
 *	DESCRIPTION
 *		This file contains the definition of a User-Defined Stored Procedure (USP) which performs
 *		the following tasks:
 *			- Ensures that only one type of Tag Record exists (Staff or Task)
 *			- Identifies new records that do not yet exist in ANESCore
 *			- Identify Task records that exist but need updates to which tags are applied
 *
 *		A column called [ETLCommand] is used to identify what to do with a record
 *			- New:		Record does not exist in ANESCore but has been retrieved from the API
 *			- Update:	Record does exist ANESCore but API extract contains different values
 */


-- Connect to ANES-ETL1
ALTER PROCEDURE import.usp_DoStagingTaskTags
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @NewRecords		TABLE	(TaskKey UNIQUEIDENTIFIER);
	DECLARE @NoRecords		TINYINT;
	DECLARE @MixedRecords	TINYINT;

	SET @NoRecords = 
		CASE
			WHEN (SELECT COUNT(*) AS Test FROM import.TagsAPI) = 0
			THEN 1
			ELSE 0
		END;

	SET @MixedRecords = 
		CASE
			WHEN (SELECT DISTINCT EntityType FROM import.TagsAPI) <> 'Task' THEN 1
			ELSE 0
		END

	IF @NoRecords = 1 OR @MixedRecords = 1	
		BEGIN
			IF @noRecords = 1
				PRINT 'No records available in [import].[TagsAPI]';
			IF @mixedRecords = 1
				PRINT '[import].[TagsAPI] contains Task and Staff records.';
		END
	ELSE 
		BEGIN
			INSERT INTO @NewRecords
				SELECT DISTINCT EntityKey
				FROM import.TagsAPI
				EXCEPT
				SELECT DISTINCT TaskKey
				FROM stage.qdm_TaggedTask;

			INSERT INTO import.qdm_TaggedTask (TaskKey)
				SELECT TaskKey FROM @NewRecords
				UNION
				SELECT DISTINCT TaskKey FROM stage.qdm_TaggedTask;

			-- Only thing not considered here is records that exist in ANESCore but do not exist in the API data
			-- What happens to archived Tasks?
			

			-- #PARSE TAG DATA
			UPDATE import.qdm_TaggedTask
			SET 
			-- Tag Category: CA Level
				CALevel_CA1 =
					CASE
						WHEN i.Tags LIKE '%{"Key":235089,"Name":"CA1"}%' THEN 1
						ELSE 0
					END
				, CALevel_CA2 =
					CASE
						WHEN i.Tags LIKE '%{"Key":235090,"Name":"CA2"}%' THEN 1
						ELSE 0
					END
				, CALevel_CA3 =
					CASE
						WHEN i.Tags LIKE '%{"Key":235091,"Name":"CA3"}%' THEN 1
						ELSE 0
					END
				, CALevel_Intern =
					CASE
						WHEN i.Tags LIKE '%{"Key":256006,"Name":"Intern"}%' THEN 1
						ELSE 0
					END
				, CALevel_PGY4 = 
					CASE
						WHEN i.Tags LIKE '%{"Key":254982,"Name":"PGY4"}%' THEN 1
						ELSE 0
					END
				, CALevel_PGY5 =
					CASE
						WHEN i.Tags LIKE '%{"Key":256005,"Name":"PGY5"}%' THEN 1
						ELSE 0
					END
				, CALevel_PGY6 =
					CASE
						WHEN i.Tags LIKE '%{"Key":254983,"Name":"PGY6"}%' THEN 1
						ELSE 0
					END
			-- Tag Category: Capacity Tab
				, Capacity_Cardiothoracic = IIF(i.Tags LIKE '%{"Key":393623,"Name":"Cardiothoracic Capacity"}%', 1, 0)
					
			-- Tag Category: CRNA Type
				, CRNAType_FT = 
					CASE
						WHEN i.Tags LIKE '%{"Key":313538,"Name":"FT"}%' THEN 1
						ELSE 0
					END
				, CRNAType_PRN = 
					CASE
						WHEN i.Tags LIKE '%{"Key":313537,"Name":"PRN"}%' THEN 1
						ELSE 0
					END
			-- Tag Category: Division
				,  Division_Cardiothoracic =
					CASE
						WHEN i.Tags LIKE '%{"Key":213970,"Name":"Cardiothoracic"}%' THEN 1
						ELSE 0
					END
				, Division_CriticalCare =
					CASE
						WHEN i.Tags LIKE '%{"Key":256810,"Name":"Critical Care"}%' THEN 1
						ELSE 0
					END
				, Division_CUHGeneralALL =
					CASE
						WHEN i.Tags LIKE '%{"Key":231530,"Name":"CUH General ALL"}%' THEN 1
						ELSE 0
					END
				, Division_CUHGeneralPrimary = IIF(i.Tags LIKE '%{"Key":308250,"Name":"CUH General Primary"}%', 1, 0)
					
				, Division_CUHOB = 
					CASE
						WHEN i.Tags LIKE '%{"Key":231532,"Name":"CUH OB"}%' THEN 1
						ELSE 0
					END
				, Division_Liver = 
					CASE
						WHEN i.Tags LIKE '%{"Key":297702,"Name":"Liver"}%' THEN 1
						ELSE 0
					END
				, Division_Neuro =
					CASE
						WHEN i.Tags LIKE '%{"Key":305852,"Name":"Neuro"}%' THEN 1
						ELSE 0
					END				
				, Division_OSCPrimary = 
					CASE
						WHEN i.Tags LIKE '%{"Key":305859,"Name":"OSC Primary"}%' THEN 1
						ELSE 0
					END
				, Division_Pain = 
					CASE
						WHEN i.Tags LIKE '%{"Key":305860,"Name":"Pain"}%' THEN 1
						ELSE 0
					END
				, Division_Pediatrics = 
					CASE
						WHEN i.Tags LIKE '%{"Key":312365,"Name":"Pediatrics"}%' THEN 1
						ELSE 0
					END
				, Division_PHHSGeneral = 
					CASE
						WHEN i.Tags LIKE '%{"Key":305856,"Name":"PHHS General"}%' THEN 1
						ELSE 0
					END
				, Division_PHHSOBHybrid =	-- NO RECORDS WITH THIS TAG FOUND 10-26-2023
					CASE
						WHEN i.Tags LIKE '%{"Key":313701,"Name":"PHHS OB Hybrid"}%' THEN 1
						ELSE 0
					END
				, Division_PHHSOBPrimary = 
					CASE
						WHEN i.Tags LIKE '%{"Key"305857:,"Name":"PHHS OB Primary"}%' THEN 1
						ELSE 0
					END
				, Division_PHHSRegional =
					CASE
						WHEN i.Tags LIKE '%{"Key":308251,"Name":"PHHS Regional"}%' THEN 1
						ELSE 0
					END
				, Division_UHRegional =
					CASE
						WHEN i.Tags LIKE '%{"Key":305853,"Name":"UH Regional"}%' THEN 1
						ELSE 0
					END
				, Division_ZaleCall =
					CASE
						WHEN i.Tags LIKE '%{"Key":305858,"Name":"Zale Call"}%' THEN 1
						ELSE 0
					END
			-- Tag Category: Employee Type
				, EmployeeType_APP =
					CASE
						WHEN i.Tags LIKE '%{"Key":280299,"Name":"APP"}%' THEN 1
						ELSE 0
					END
				, EmployeeType_FacultyFullTime = 
					CASE
						WHEN i.Tags LIKE '%{"Key":312369,"Name":"Faculty-Full Time"}%' THEN 1
						ELSE 0
					END
				, EmployeeType_FacultyPartTime = 
					CASE
						WHEN i.Tags LIKE '%{"Key":280298,"Name":"Faculty-Part Time"}%' THEN 1
						ELSE 0
					END
				, EmployeeType_FacultyPTNB =
					CASE
						WHEN i.Tags LIKE '%{"Key":312368,"Name":"Faculty-PTNB"}%' THEN 1
						ELSE 0
					END
				, EmployeeType_FacultyTasks	 = 
					CASE
						WHEN i.Tags LIKE '%{"Key":312372,"Name":"Faculty-Tasks"}%' THEN 1
						ELSE 0
					END
				, EmployeeType_NonClinicalTime =
					CASE
						WHEN i.Tags LIKE '%{"Key":322174,"Name":"Non-Clinical Time"}%' THEN 1
						ELSE 0
					END
				, EmployeeType_Trainee = 
					CASE
						WHEN i.Tags LIKE '%{"Key":280302,"Name":"Trainee"}%' THEN 1
						ELSE 0
					END
				, EmployeeType_UTStaff =
					CASE
						WHEN i.Tags LIKE '%{"Key":280300,"Name":"UT Staff"}%' THEN 1
						ELSE 0
					END
				, Integrations_Kronos = 
					CASE
						WHEN i.Tags LIKE '%{"Key":319900,"Name":"Kronos"}%' THEN 1
					END
			-- Tag Category: Location
				, Location_CUH =
					CASE
						WHEN i.Tags LIKE '%{"Key":262917,"Name":"CUH"}%' THEN 1
						ELSE 0
					END
				, Location_CUHCardiac = 
					CASE
						WHEN i.Tags LIKE '%{"Key":328482,"Name":"CUH Cardiac"}%' THEN 1
						ELSE 0
					END
				, Location_CUHGeneral = 
					CASE
						WHEN i.Tags LIKE '%{"Key":328479,"Name":"CUH General"}%' THEN 1
						ELSE 0
					END
				, Location_CUHNeuro =
					CASE
						WHEN i.Tags LIKE '%{"Key":328480,"Name":"CUH Neuro"}%' THEN 1
						ELSE 0
					END
				, Location_FellowVacation =
					CASE
						WHEN i.Tags LIKE '%{"Key":344047,"Name":"Fellow Vacation"}%' THEN 1
						ELSE 0
					END
				, Location_ICU =
					CASE
						WHEN i.Tags LIKE '%{"Key":313739,"Name":"ICU"}%' THEN 1
						ELSE 0
					END
				, Location_PainRoles = 
					CASE
						WHEN i.Tags LIKE '%{"Key":314663,"Name":"Pain Roles"}%' THEN 1
						ELSE 0
					END
				, Location_PHHS =
					CASE
						WHEN i.Tags LIKE '%{"Key":305849,"Name":"PHHS"}%' THEN 1
						ELSE 0
					END
				, Location_UH =
					CASE
						WHEN i.Tags LIKE '%{"Key":306084,"Name":"UH"}%' THEN 1
						ELSE 0
					END
				, Location_UHOSC =
					CASE
						WHEN i.Tags LIKE '%{"Key":262947,"Name":"UH OSC"}%' THEN 1
						ELSE 0
					END
				, Location_VA = 
					CASE
						WHEN i.Tags LIKE '%{"Key":443531,"Name":"VA"}%' THEN 1
						ELSE 0
					END
				, Location_Zale = 
					CASE
						WHEN i.Tags LIKE '%{"Key":262918,"Name":"Zale"}%' THEN 1
						ELSE 0
					END
				-- Tag Category: ND
				, ND_Day = 
					CASE
						WHEN i.Tags LIKE '%{"Key":257139,"Name":"Day"}%' THEN 1
						ELSE 0
					END
				, ND_Night = 
					CASE
						WHEN i.Tags LIKE '%{"Key":257138,"Name":"Night"}%' THEN 1
						ELSE 0
					END
				-- Tag Category: Primary Site
				, PrimarySite_PHHS =
					CASE
						WHEN i.Tags LIKE '%{"Key":253943,"Name":"PHHS"}%' THEN 1
						ELSE 0
					END
				, PrimarySite_UH =
					CASE
						WHEN i.Tags LIKE '%{"Key":231539,"Name":"UH"}%' THEN 1
						ELSE 0
					END
				-- Tag Category: Provider Type
				, ProviderType_CRNA = 
					CASE
						WHEN i.Tags LIKE '%{"Key":213973,"Name":"CRNA"}%' THEN 1
						ELSE 0
					END
				, ProviderType_Fellow = 
					CASE
						WHEN i.Tags LIKE '%{"Key":213976,"Name":"Fellow"}%' THEN 1
						ELSE 0
					END
				, ProviderType_NP = 
					CASE
						WHEN i.Tags LIKE '%{"Key":213978,"Name":"NP"}%' THEN 1
						ELSE 0
					END
				, ProviderType_PA =
					CASE
						WHEN i.Tags LIKE '%{"Key":256808,"Name":"PA"}%' THEN 1
						ELSE 0
					END
				, ProviderType_Physician =
					CASE
						WHEN i.Tags LIKE '%{"Key":213972,"Name":"Physician"}%' THEN 1
						ELSE 0
					END
				, ProviderType_Resident =
					CASE
						WHEN i.Tags LIKE '%{"Key":213975,"Name":"Resident"}%' THEN 1
						ELSE 0
					END
				, ProviderType_RRNA =
					CASE
						WHEN i.Tags LIKE '%{"Key":213974,"Name":"RRNA"}%' THEN 1
						ELSE 0
					END
				, ProviderType_UTStaff =
					CASE
						WHEN i.Tags LIKE '%{"Key":213977,"Name":"UT Staff"}%' THEN 1
						ELSE 0
					END
			-- Tag Category: QGenda Admin Tags
				, QGendaAdminTags_Header = IIF(i.Tags LIKE '%{"Key":320341,"Name":"Header"}%', 1, 0)
				, QGendaAdminTags_LBL = IIF(i.Tags LIKE '%{"Key":320342,"Name":"LBL"}%', 1, 0)

			-- Tag Category: Shift Length
				, ShiftLength_8hr = IIF(i.Tags LIKE '%{"Key":235094,"Name":"8hr"}%', 1, 0)
				, ShiftLength_10hr = IIF(i.Tags LIKE '%{"Key":235095,"Name":"10hr"}%', 1, 0)
				, ShiftLength_11hr = IIF(i.Tags LIKE '%{"Key":235096,"Name":"11hr"}%', 1, 0)
				, ShiftLength_12hr = IIF(i.Tags LIKE '%{"Key":235097,"Name":"12hr"}%', 1, 0)
				, ShiftLength_13hr = IIF(i.Tags LIKE '%{"Key":235098,"Name":"13hr"}%', 1, 0)
				, ShiftLength_14hr = IIF(i.Tags LIKE '%{"Key":235099,"Name":"14hr"}%', 1, 0)
				, ShiftLength_16hr = IIF(i.Tags LIKE '%{"Key":235100,"Name":"16hr"}%', 1, 0)
				, ShiftLength_24hr = IIF(i.Tags LIKE '%{"Key":235101,"Name":"24hr"}%', 1, 0)

				-- Tag Category: System Task Type
				, SystemTaskType_Unavailable =
					CASE
						WHEN i.Tags LIKE '%{"Key":213941,"Name":"Unavailable"}%' THEN 1
						ELSE 0
					END
				, SystemTaskType_Working =
					CASE
						WHEN i.Tags LIKE '%{"Key":213940,"Name":"Working"}%' THEN 1
						ELSE 0
					END
				-- Tag Category: Task Grouping
				, TaskGrouping_AIC =
					CASE
						WHEN i.Tags LIKE '%{"Key":306146,"Name":"AIC"}%' THEN 1
						ELSE 0
					END			
				, TaskGrouping_CIC =
					CASE
						WHEN i.Tags LIKE '%{"Key":306147,"Name":"CIC"}%' THEN 1
						ELSE 0
					END	
				, TaskGrouping_Clinic =
					CASE
						WHEN i.Tags LIKE '%{"Key":306142,"Name":"Clinic"}%' THEN 1
						ELSE 0
					END	
				, TaskGrouping_ICU =
					CASE
						WHEN i.Tags LIKE '%{"Key":306143,"Name":"ICU"}%' THEN 1
						ELSE 0
					END	
				, TaskGrouping_OR =
					CASE
						WHEN i.Tags LIKE '%{"Key":306148,"Name":"OR"}%' THEN 1
						ELSE 0
					END	
				, TaskGrouping_Procedure =
					CASE
						WHEN i.Tags LIKE '%{"Key":306144,"Name":"Procedure"}%' THEN 1
						ELSE 0
					END	
				, TaskGrouping_Telemedicine	=
					CASE
						WHEN i.Tags LIKE '%{"Key":306145,"Name":"Telemedicine"}%' THEN 1
						ELSE 0
					END
				-- Tag Category: Task Type 1
				, TaskType1_Away = 	
					CASE
						WHEN i.Tags LIKE '%{"Key":213967,"Name":"Away"}%' THEN 1
						ELSE 0
					END	
				, TaskType1_Call =
					CASE
						WHEN i.Tags LIKE '%{"Key":213966,"Name":"Call"}%' THEN 1
						ELSE 0
					END	
				, TaskType1_Label =
					CASE
						WHEN i.Tags LIKE '%{"Key":327752,"Name":"Label"}%' THEN 1
						ELSE 0
					END	
				, TaskType1_NonClinical =
					CASE
						WHEN i.Tags LIKE '%{"Key":213969,"Name":"Non-Clinical"}%' THEN 1
						ELSE 0
					END	
				, TaskType1_Working =
					CASE
						WHEN i.Tags LIKE '%{"Key":213968,"Name":"Working"}%' THEN 1
						ELSE 0
					END	
			FROM import.qdm_TaggedTask AS s
				INNER JOIN import.TagsAPI AS i
					ON s.TaskKey = i.EntityKey AND i.EntityType = 'Task';
			PRINT 'Parsing of tag data for tasks completed.';
		

			PRINT 'Beginning comparison of staged records.';
			SELECT
				i.TaskKey

				, i.CALevel_CA1						AS iCALevel_CA1
				, s.CALevel_CA1						AS sCALevel_CA1
				, cCALevel_CA1						= IIF(i.CALevel_CA1 = s.CALevel_CA1, 'Match', 'Conflict')
				
				, i.CALevel_CA2						AS iCALevel_CA2
				, s.CALevel_CA2						AS sCALevel_CA2
				, cCALevel_CA2						= IIF(i.CALevel_CA2 = s.CALevel_CA2, 'Match', 'Conflict')

				, i.CALevel_CA3						AS iCALevel_CA3
				, s.CALevel_CA3						AS sCALevel_CA3
				, cCALevel_CA3						= IIF(i.CALevel_CA3 = s.CALevel_CA3, 'Match', 'Conflict')

				, i.CALevel_Intern					AS iCALevel_Intern
				, s.CALevel_Intern					AS sCALevel_Intern
				, cCALevel_Intern					= IIF(i.CALevel_Intern = s.CALevel_Intern, 'Match', 'Conflict')

				, i.CALevel_PGY4					AS iCALevel_PGY4
				, s.CALevel_PGY4					AS sCALevel_PGY4
				, cCALevel_PGY4						= IIF(i.CALevel_PGY4 = s.CALevel_PGY4, 'Match', 'Conflict')

				, i.CALevel_PGY5					AS iCALevel_PGY5
				, s.CALevel_PGY5					AS sCALevel_PGY5
				, cCALevel_PGY5						= IIF(i.CALevel_PGY5 = s.CALevel_PGY5, 'Match', 'Conflict')

				, i.CALevel_PGY6					AS iCALevel_PGY6
				, s.CALevel_PGY6					AS sCALevel_PGY6
				, cCALevel_PGY6						= IIF(i.CALevel_PGY6 = s.CALevel_PGY6, 'Match', 'Conflict')

				-- Tag Category: Capacity Tab
				, i.Capacity_Cardiothoracic			AS iCapacity_Cardiothoracic
				, s.Capacity_Cardiothoracic			AS sCapacity_Cardiothoracic
				, cCapacity_Cardiothoracic			= IIF(i.Capacity_Cardiothoracic = s.Capacity_Cardiothoracic, 'Match', 'Conflict')
				
				-- Tag Category: CRNA Type
				, i.CRNAType_FT						AS iCRNAType_FT
				, s.CRNAType_FT						AS sCRNAType_FT
				, cCRNAType_FT						= IIF(i.CRNAType_FT = s.CRNAType_FT, 'Match', 'Conflict')
	
				, i.CRNAType_PRN					AS iCRNAType_PRN
				, s.CRNAType_PRN					AS sCRNAType_PRN
				, cCRNAType_PRN						= IIF(i.CRNAType_PRN = s.CRNAType_PRN, 'Match', 'Conflict')
				
				-- Tag Category: Division
				, i.Division_Cardiothoracic			AS iDivision_Cardiothoracic
				, s.Division_Cardiothoracic			AS sDivision_Cardiothoracic
				, cDivision_Cardiothoracic			= IIF(i.Division_Cardiothoracic = s.Division_Cardiothoracic, 'Match', 'Conflict')
				
				, i.Division_CriticalCare			AS iDivision_CriticalCare
				, s.Division_CriticalCare			AS sDivision_CriticalCare
				, cDivision_CriticalCare			= IIF(i.Division_CriticalCare = s.Division_CriticalCare, 'Match', 'Conflict')

				, i.Division_CUHGeneralALL			AS iDivision_CUHGeneralALL
				, s.Division_CUHGeneralALL			AS sDivision_CUHGeneralALL
				, cDivision_CUHGeneralALL			 = IIF(i.Division_CUHGeneralALL = s.Division_CUHGeneralALL, 'Match', 'Conflict')

				, i.Division_CUHGeneralPrimary		AS iDivision_CUHGeneralPrimary
				, s.Division_CUHGeneralPrimary		AS sDivision_CUHGeneralPrimary
				, cDivision_CUHGeneralPrimary		= IIF(i.Division_CUHGeneralPrimary = s.Division_CUHGeneralPrimary, 'Match', 'Conflict')

				, i.Division_CUHOB					AS iDivision_CUHOB
				, s.Division_CUHOB					AS sDivision_CUHOB
				, cDivision_CUHOB					= IIF(i.Division_CUHOB = s.Division_CUHOB, 'Match', 'Conflict')
				
				, i.Division_Liver					AS iDivision_Liver
				, s.Division_liver					AS sDivision_Liver
				, cDivision_Liver					= IIF(i.Division_Liver = s.Division_Liver, 'Match', 'Conflict')

				, i.Division_Neuro					AS iDivision_Neuro
				, s.Division_Neuro					AS sDivision_Neuro
				, cDivision_Neuro					= IIF(i.Division_Neuro = s.Division_Neuro, 'Match', 'Conflict')

				, i.Division_OSCPrimary				AS iDivision_OSCPrimary
				, s.Division_OSCPrimary				AS sDivision_OSCPrimary
				, cDivision_OSCPrimary				= IIF(i.Division_OSCPrimary = s.Division_OSCPrimary, 'Match', 'Conflict')
				
				, i.Division_Pain					AS iDivision_Pain
				, s.Division_Pain					AS sDivision_Pain
				, cDivision_Pain					= IIF(i.Division_Pain = s.Division_Pain, 'Match', 'Conflict')

				, i.Division_Pediatrics				AS iDivision_Pediatrics
				, s.Division_Pediatrics				AS sDivision_Pediatrics
				, cDivision_Pediatrics				= IIF(i.Division_Pediatrics = s.Division_Pediatrics, 'Match', 'Conflict')

				, i.Division_PHHSGeneral			AS iDivision_PHHSGeneral
				, s.Division_PHHSGeneral			AS sDivision_PHHSGeneral
				, cDivision_PHHSGeneral				= IIF(i.Division_PHHSGeneral = s.Division_PHHSGeneral, 'Match', 'Conflict')

				, i.Division_PHHSOBHybrid			AS iDivision_PHHSOBHybrid
				, s.Division_PHHSOBHybrid			AS sDivision_PHHSOBHybrid
				, cDivision_PHHSOBHybrid			= IIF(i.Division_PHHSOBHybrid = s.Division_PHHSOBHybrid, 'Match', 'Conflict')

				, i.Division_PHHSOBPrimary			AS iDivision_PHHSOBPrimary
				, s.Division_PHHSOBPrimary			AS sDivision_PHHSOBPrimary
				, cDivision_PHHSOBPrimary			= IIF(i.Division_PHHSOBPrimary = s.Division_PHHSOBPrimary, 'Match', 'Conflict')

				, i.Division_PHHSRegional			AS iDivision_PHHSRegional
				, s.Division_PHHSRegional			AS sDivision_PHHSRegional
				, cDivision_PHHSRegional			= IIF(i.Division_PHHSRegional = s.Division_PHHSRegional, 'Match', 'Conflict')

				, i.Division_UHRegional				AS iDivision_UHRegional
				, s.Division_UHRegional				AS sDivision_UHRegional
				, cDivision_UHRegional				= IIF(i.Division_UHRegional = s.Division_UHRegional, 'Match', 'Conflict')

				, i.Division_ZaleCall				AS iDivision_ZaleCall
				, s.Division_ZaleCall				AS sDivision_ZaleCall
				, cDivision_ZaleCall				= IIF(i.Division_ZaleCall = s.Division_ZaleCall, 'Match', 'Conflict')

				-- Tag Category: Employee
				, i.EmployeeType_APP				AS iEmployeeType_APP
				, s.EmployeeType_APP				AS sEmployeeType_APP
				, cEmployeeType_APP					= IIF(i.EmployeeType_APP = s.EmployeeType_APP, 'Match', 'Conflict')

				, i.EmployeeType_FacultyFullTime	AS iEmployeeType_FacultyFullTime
				, s.EmployeeType_FacultyFullTime	AS sEmployeeType_FacultyFullTime
				, cEmployeeType_FacultyFullTime		= IIF(i.EmployeeType_FacultyFullTime = s.EmployeeType_FacultyFullTime, 'Match', 'Conflict')

				, i.EmployeeType_FacultyPartTime	AS iEmployeeType_FacultyPartTime
				, s.EmployeeType_FacultyPartTime	AS sEmployeeType_FacultyPartTime
				, cEmployeeType_FacultyPartTime		= IIF(i.EmployeeType_FacultyPartTime = s.EmployeeType_FacultyPartTime, 'Match', 'Conflict')

				, i.EmployeeType_FacultyPTNB		AS iEmployeeType_FacultyPTNB
				, s.EmployeeType_FacultyPTNB		AS sEmployeeType_FacultyPTNB
				, cEmployeeType_FacultyPTNB			= IIF(i.EmployeeType_FacultyPTNB = s.EmployeeType_FacultyPTNB, 'Match', 'Conflict')

				, i.EmployeeType_FacultyTasks		AS iEmployeeType_FacultyTasks
				, s.EmployeeType_FacultyTasks		AS sEmployeeType_FacultyTasks
				, cEmployeeType_FacultyTasks		= IIF(i.EmployeeType_FacultyTasks = s.EmployeeType_FacultyTasks, 'Match', 'Conflict')

				, i.EmployeeType_NonClinicalTime	AS iEmployeeType_NonClinicalTime
				, s.EmployeeType_NonClinicalTime	AS sEMployeeType_NonClinicalTime
				, cEmployeeType_NonClinicalTIme		= IIF(i.EmployeeType_NonClinicalTime = s.EmployeeType_NonClinicalTime, 'Match', 'Conflict')

				, i.EmployeeType_Trainee			AS iEmployeeType_Trainee
				, s.EmployeeType_Trainee			AS sEmployeeType_Trainee
				, cEmployeeType_Trainee				= IIF(i.EmployeeType_Trainee = s.EmployeeType_Trainee, 'Match', 'Conflict')

				, i.EmployeeType_UTStaff			AS iEmployeeType_UTStaff
				, s.EmployeeType_UTStaff			AS sEmployeeType_UTStaff
				, cEmployeeType_UTStaff				= IIF(i.EmployeeType_UTStaff = s.EmployeeType_UTStaff, 'Match', 'Conflict')
				-- Tag Category: Integration
				, i.Integrations_Kronos				AS iIntegrations_Kronos
				, s.Integrations_Kronos				AS sIntegrations_Kronos
				, cIntegrations_Kronos				= IIF(i.Integrations_Kronos = s.Integrations_Kronos, 'Match', 'Conflict')

				-- Tag Category: Location
				, i.Location_CUH					AS iLocation_CUH
				, s.Location_CUH					AS sLocation_CUH
				, cLocation_CUH						= IIF(i.Location_CUH = s.Location_CUH, 'Match', 'Conflict')

				, i.Location_CUHCardiac				AS iLocation_CUHCardiac
				, s.Location_CUHCardiac				AS sLocation_CUHCardiac
				, cLocation_CUHCardiac				= IIF(i.Location_CUHCardiac = s.Location_CUHCardiac, 'Match', 'Conflict')

				, i.Location_CUHGeneral				AS iLocation_CUHGeneral
				, s.Location_CUHGeneral				AS sLocation_CUHGeneral
				, cLocation_CUHGeneral				= IIF(i.Location_CUHGeneral = s.Location_CUHGeneral, 'Match', 'Conflict')

				, i.Location_CUHNeuro				AS iLocation_CUHNeuro
				, s.Location_CUHNeuro				AS sLocation_CUHNeuro
				, cLocation_CUHNeuro				= IIF(i.Location_CUHNeuro = s.Location_CUHNeuro, 'Match', 'Conflict')

				, i.Location_FellowVacation			AS iLocation_FellowVacation
				, s.Location_FellowVacation			AS sLocation_FellowVacation
				, cLocation_FellowVacation			= IIF(i.Location_FellowVacation = s.Location_FellowVacation, 'Match', 'Conflict')

				, i.Location_ICU					AS iLocation_ICU
				, s.Location_ICU					AS sLocation_ICU
				, cLocation_ICU						= IIF(i.Location_ICU = s.Location_ICU, 'Match', 'Conflict')

				, i.Location_PainRoles				AS iLocation_PainRoles
				, s.Location_PainRoles				AS sLocation_PainRoles
				, cLocation_PainRoles				= IIF(i.Location_PainRoles = s.Location_PainRoles, 'Match', 'Conflict')

				, i.Location_PHHS					AS iLocation_PHHS
				, s.Location_PHHS					AS sLocation_PHHS
				, cLocation_PHHS					= IIF(i.Location_PHHS = s.Location_PHHS, 'Match', 'Conflict')

				, i.Location_UH						AS iLocation_UH
				, s.Location_UH						AS sLocation_UH
				, cLocation_UH						= IIF(i.Location_UH = s.Location_UH, 'Match', 'Conflict')

				, i.Location_UHOSC					AS iLocation_UHOSC
				, s.Location_UHOSC					AS sLocation_UHOSC
				, cLocation_UHOSC					= IIF(i.Location_UHOSC = s.Location_UHOSC, 'Match', 'Conflict')

				, i.Location_VA						AS iLocation_VA
				, s.Location_VA						AS sLocation_VA
				, cLocation_VA						= IIF(i.Location_VA = s.Location_VA, 'Match', 'Conflict')

				, i.Location_Zale					AS iLocation_Zale
				, s.Location_Zale					AS sLocation_Zale
				, cLocation_Zale					= IIF(i.Location_Zale = s.Location_Zale, 'Match', 'Conflict')

				-- Tag Category: N/D
				, i.ND_Day							AS iND_DAY
				, s.ND_Day							AS sND_Day
				, cND_Day							= IIF(i.ND_Day = s.ND_Day, 'Match', 'Conflict')

				, i.ND_Night						AS iND_Night
				, s.ND_Night						AS sND_Night
				, cND_Night							= IIF(i.ND_Night = s.ND_Night, 'Match', 'Conflict')

				-- Tag Category: Primary Site
				, i.PrimarySite_PHHS				AS iPrimarySite_PHHS
				, s.PrimarySite_PHHS				AS sPrimarySite_PHHS
				, cPrimarySite_PHHS					= IIF(i.PrimarySite_PHHS = s.PrimarySite_PHHS, 'Match', 'Conflict')

				, i.PrimarySite_UH					AS iPrimarySite_UH
				, s.PrimarySite_UH					AS sPrimarySite_UH
				, cPrimarySite_UH					= IIF(i.PrimarySite_UH = s.PrimarySite_UH, 'Match', 'Conflict')
				
				-- Tag Category: Provider Type
				, i.ProviderType_CRNA				AS iProviderType_CRNA
				, s.ProviderType_CRNA				AS sProviderType_CRNA
				, cProviderType_CRNA				= IIF(i.ProviderType_CRNA = s.ProviderType_CRNA, 'Match', 'Conflict')

				, i.ProviderType_Fellow				AS iProviderType_Fellow
				, s.ProviderType_Fellow				AS sProviderType_Fellow
				, cProviderType_Fellow				= IIF(i.ProviderType_Fellow = s.ProviderType_Fellow, 'Match', 'Conflict')

				, i.ProviderType_NP					AS iProviderType_NP
				, s.ProviderType_NP					AS sProviderType_NP
				, cProviderType_NP					= IIF(i.ProviderType_NP = s.ProviderType_NP, 'Match', 'Conflict')

				, i.ProviderType_PA					AS iProviderType_PA
				, s.ProviderType_PA					AS sProviderType_PA
				, cProviderType_PA					= IIF(i.ProviderType_PA = s.ProviderType_PA, 'Match', 'Conflict')
				
				, i.ProviderType_Physician			AS iProviderType_Physician
				, s.ProviderType_Physician			AS sProviderType_Physician
				, cProviderType_Physician			= IIF(i.ProviderType_Physician = s.ProviderType_Physician, 'Match', 'Conflict')

				, i.ProviderType_Resident			AS iProviderType_Resident
				, s.ProviderType_Resident			AS sProviderType_Resident
				, cProviderType_Resident			= IIF(i.ProviderType_Resident = s.ProviderType_Resident, 'Match', 'Conflict')

				, i.ProviderType_RRNA				AS iProviderType_RRNA
				, s.ProviderType_RRNA				AS sProviderType_RRNA
				, cProviderType_RRNA				= IIF(i.ProviderType_RRNA = s.ProviderType_RRNA, 'Match', 'Conflict')

				, i.ProviderType_UTStaff			AS iProviderType_UTStaff
				, s.ProviderType_UTStaff			AS sProviderType_UTStaff
				, cProviderType_UTStaff				= IIF(i.ProviderType_UTStaff = s.ProviderType_UTStaff, 'Match', 'Conflict')

				-- Tag Category: QGenda Admin Tags
				, i.QGendaAdminTags_Header			AS iQGendaAdminTags_Header
				, s.QGendaAdminTags_Header			AS sQGendaAdminTags_Header
				, cQGendaAdminTags_Header			= IIF(i.QGendaAdminTags_Header = s.QGendaAdminTags_Header, 'Match', 'Conflict')

				, i.QGendaAdminTags_LBL				AS iQGendaAdminTags_LBL
				, s.QGendaAdminTags_LBL				AS sQGendaAdminTags_LBL
				, cQGendaAdminTags_LBL				= IIF(i.QGendaAdminTags_LBL = s.QGendaAdminTags_LBL, 'Match', 'Conflict')

				-- Tag Category: Shift Length
				, i.ShiftLength_8hr					AS iShiftLength_8hr
				, s.ShiftLength_8hr					AS sShiftLength_8hr
				, cShiftLength_8hr					= IIF(i.ShiftLength_8hr = s.ShiftLength_8hr, 'Match', 'Conflict')

				, i.ShiftLength_10hr				AS iShiftLength_10hr
				, s.ShiftLength_10hr				AS sShiftLength_10hr
				, cShiftLength_10hr					= IIF(i.ShiftLength_10hr = s.ShiftLength_10hr, 'Match', 'Conflict')

				, i.ShiftLength_11hr				AS iShiftLength_11hr
				, s.ShiftLength_11hr				AS sShiftLength_11hr
				, cShiftLength_11hr					= IIF(i.ShiftLength_11hr = s.ShiftLength_11hr, 'Match', 'Conflict')

				, i.ShiftLength_12hr				AS iShiftLength_12hr
				, s.ShiftLength_12hr				AS sShiftLength_12hr
				, cShiftLength_12hr					= IIF(i.ShiftLength_12hr = s.ShiftLength_12hr, 'Match', 'Conflict')

				, i.ShiftLength_13hr				AS iShiftLength_13hr
				, s.ShiftLength_13hr				AS sShiftLength_13hr
				, cShiftLength_13hr					= IIF(i.ShiftLength_13hr = s.ShiftLength_13hr, 'Match', 'Conflict')

				, i.ShiftLength_14hr				AS iShiftLength_14hr
				, s.ShiftLength_14hr				AS sShiftLength_14hr
				, cShiftLength_14hr					= IIF(i.ShiftLength_14hr = s.ShiftLength_14hr, 'Match', 'Conflict')

				, i.ShiftLength_16hr				AS iShiftLength_16hr
				, s.ShiftLength_16hr				AS sShiftLength_16hr
				, cShiftLength_16hr					= IIF(i.ShiftLength_16hr = s.ShiftLength_16hr, 'Match', 'Conflict')

				, i.ShiftLength_24hr				AS iShiftLength_24hr
				, s.ShiftLength_24hr				AS sShiftLength_24hr
				, cShiftLength_24hr					= IIF(i.ShiftLength_24hr = s.ShiftLength_24hr, 'Match', 'Conflict')

				-- Tag Category: System Task Type
				, i.SystemTaskType_Unavailable		AS iSystemTaskType_Unavailable
				, s.SystemTaskType_Unavailable		AS sSystemTaskType_Unavailable
				, cSystemTaskType_Unavailable		= IIF(i.SystemTaskType_Unavailable = s.SystemTaskType_Unavailable, 'Match', 'Conflict')

				, i.SystemTaskType_Working			AS iSystemTaskType_Working
				, s.SystemTaskType_Working			AS sSystemTaskType_Working
				, cSystemTaskType_Working			= IIF(i.SystemTaskType_Working = s.SystemTaskType_Working, 'Match', 'Conflict')

				-- Tag Category: Task Grouping
				, i.TaskGrouping_AIC				AS iTaskGrouping_AIC
				, s.TaskGrouping_AIC				AS sTaskGrouping_AIC
				, cTaskgrouping_AIC					= IIF(i.TaskGrouping_AIC = s.TaskGrouping_AIC, 'Match', 'Conflict')

				, i.TaskGrouping_CIC				AS iTaskGrouping_CIC
				, s.TaskGrouping_CIC				AS sTaskGrouping_CIC
				, cTaskGrouping_CIC					= IIF(i.TaskGrouping_CIC = s.TaskGrouping_CIC, 'Match', 'Conflict')

				, i.TaskGrouping_Clinic				AS iTaskGrouping_Clinic
				, s.TaskGrouping_Clinic				AS sTaskGrouping_Clinic
				, cTaskGrouping_Clinic				= IIF(i.TaskGrouping_Clinic = s.TaskGrouping_Clinic, 'Match', 'Conflict')

				, i.TaskGrouping_ICU				AS iTaskGrouping_ICU
				, s.TaskGrouping_ICU				AS sTaskGrouping_ICU
				, cTaskGrouping_ICU					= IIF(i.TaskGrouping_ICU = s.TaskGrouping_ICU, 'Match', 'Conflict')
				
				, i.TaskGrouping_OR					AS iTaskGrouping_OR
				, s.TaskGrouping_OR					AS sTaskGrouping_OR
				, cTaskGrouping_OR					= IIF(i.TaskGrouping_OR = s.TaskGrouping_OR, 'Match', 'Conflict')

				, i.TaskGrouping_Procedure			AS iTaskGrouping_Procedure
				, s.TaskGrouping_Procedure			AS sTaskGrouping_Procedure
				, cTaskGrouping_Procedure			= IIF(i.TaskGrouping_Procedure = s.TaskGrouping_Procedure, 'Match', 'Conflict')

				, i.TaskGrouping_Telemedicine		AS iTaskGrouping_Telemedicine
				, s.TaskGrouping_Telemedicine		AS sTaskGrouping_Telemedicine
				, cTaskGrouping_Telemedicine		= IIF(i.TaskGrouping_Telemedicine = s.TaskGrouping_Telemedicine, 'Match', 'Conflict')

				-- Tag Category: Task Type1
				, i.TaskType1_Away					AS iTaskType1_Away
				, s.TaskType1_Away					AS sTaskType1_Away
				, cTaskType1_Away					= IIF(i.TaskType1_Away = s.TaskType1_Away, 'Match', 'Conflict')

				, i.TaskType1_Call					AS iTaskType1_Call
				, s.TaskType1_Call					AS sTaskType1_Call
				, cTaskType1_Call					= IIF(i.TaskType1_Call = s.TaskType1_Call, 'Match', 'Conflict')

				, i.TaskType1_Label					AS iTaskType1_Label
				, s.TaskType1_Label					AS sTaskType1_Label
				, cTaskType1_Label					= IIF(i.TaskType1_Label = s.TaskType1_Label, 'Match', 'Conflict')

				, i.TaskType1_NonClinical			AS iTaskType1_NonClinical
				, s.TaskType1_NonClinical			AS sTaskType1_NonClinical
				, cTaskType1_NonClinical			= IIF(i.TaskType1_NonClinical = s.TaskType1_NonClinical, 'Match', 'Conflict')

				, i.TaskType1_Working				AS iTaskType1_Working
				, s.TaskType1_Working				AS sTaskType1_Working
				, cTaskType1_Working				= IIF(i.TaskType1_Working = s.TaskType1_Working, 'Match', 'Conflict')
			INTO #Comparison
			FROM import.qdm_TaggedTask AS i
				INNER JOIN stage.qdm_TaggedTask AS s
					ON i.TaskKey = s.TaskKey;
			PRINT 'Comparison completed.';


			-- #CORRECTIONS
			PRINT 'Beginning corrections';
			DECLARE @RecordsToUpdate	TABLE (TaskKey UNIQUEIDENTIFIER)

			-- Tag Category: CALevel ---------------------------
			-- Correct changes to CALevel_CA1
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cCALevel_CA1 = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET CALevel_CA1 = i.CALevel_CA1
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to CALevel_CA2
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cCALevel_CA2 = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET CALevel_CA2 = i.CALevel_CA2
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END
		
			-- Correct changes to CALevel_CA3
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cCALevel_CA3 = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET CALevel_CA3 = i.CALevel_CA3
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to CALevel_Intern
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cCALevel_Intern = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET CALevel_Intern = i.CALevel_Intern
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END				

			-- Correct changes to CALevel_PGY4
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cCALevel_PGY4 = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET CALevel_PGY4 = i.CALevel_PGY4
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END
			
			-- Correct changes to CALevel_PGY5
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cCALevel_PGY5 = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET CALevel_PGY5 = i.CALevel_PGY5
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to CALevel_PGY6
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cCALevel_PGY6 = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET CALevel_PGY6 = i.CALevel_PGY6
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END	

		-- TAG CATEGORY: CRNAType
			-- Correct changes to Capacity_Cardiothoracic
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cCapacity_Cardiothoracic = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET Capacity_Cardiothoracic = i.Capacity_Cardiothoracic
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END	

			-- TAG CATEGORY: CRNAType
			-- Correct changes to CRNAType_FT
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cCRNAType_FT = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET CRNAType_FT = i.CRNAType_FT
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END	

			-- Correct changes to CRNAType_PRN
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cCRNAType_PRN = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET CRNAType_PRN = i.CRNAType_PRN
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- TAG CATEGORY: Division --------------------------
			-- Correct changes to Division_Cardiothoracic
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cDivision_Cardiothoracic = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET Division_Cardiothoracic = i.Division_Cardiothoracic
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to Division_CriticalCare
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cDivision_CriticalCare = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET Division_CriticalCare = i.Division_CriticalCare
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END	

			-- Correct changes to Division_CUHGeneralALL
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cDivision_CUHGeneralALL = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET Division_CUHGeneralALL = i.Division_CUHGeneralALL
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END	

			-- Correct changes to Division_CUHGeneralPrimary
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cDivision_CUHGeneralPrimary = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET Division_CUHGeneralPrimary = i.Division_CUHGeneralPrimary
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END	

			-- Correct changes to Division_CUHOB
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cDivision_CUHOB = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET Division_CUHOB = i.Division_CUHOB
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to Division_Liver
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cDivision_Liver = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET Division_Liver = i.Division_Liver
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to Division_Neuro
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cDivision_Neuro = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET Division_Neuro = i.Division_Neuro
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END	

			-- Correct changes to Division_OSCPrimary
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cDivision_OSCPrimary = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET Division_OSCPrimary = i.Division_OSCPrimary
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END	
				
			-- Correct changes to Division_Pain
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cDivision_Pain = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET Division_Pain = i.Division_Pain
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to Division_Pediatrics
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cDivision_Pediatrics = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET Division_Pediatrics = i.Division_Pediatrics
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to Division_PHHSGeneral
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cDivision_PHHSGeneral = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET Division_PHHSGeneral = i.Division_PHHSGeneral
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to Division_PHHSOBHybrid
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cDivision_PHHSOBHybrid = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET Division_PHHSOBHybrid = i.Division_PHHSOBHybrid
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END
					
			-- Correct changes to Division_PHHSOBPrimary
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cDivision_PHHSOBPrimary = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET Division_PHHSOBPrimary = i.Division_PHHSOBPrimary
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END
					
			-- Correct changes to Division_PHHSRegional
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cDivision_PHHSRegional = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET Division_PHHSRegional = i.Division_PHHSRegional
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END
					
			-- Correct changes to Division_UHRegional
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cDivision_UHRegional = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET Division_UHRegional = i.Division_UHRegional
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END
					
			-- Correct changes to Division_ZaleCall
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cDivision_ZaleCall = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET Division_ZaleCall = i.Division_ZaleCall
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- TAG CATEGORY: Employee Type ---------------------
			-- Correct changes to EmployeeType_APP
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cEmployeeType_APP = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET EmployeeType_APP = i.EmployeeType_APP
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to EmployeeType_FacultyFullTime
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cEmployeeType_FacultyFullTime = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET EmployeeType_FacultyFullTime = i.EmployeeType_FacultyFullTime
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to EmployeeType_FacultyPartTime
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cEmployeeType_FacultyPartTime = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET EmployeeType_FacultyPartTime = i.EmployeeType_FacultyPartTime
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END
			
			-- Correct changes to EmployeeType_FacultyPTNB
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cEmployeeType_FacultyPTNB = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET EmployeeType_FacultyPTNB = i.EmployeeType_FacultyPTNB
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END			
			
			-- Correct changes to EmployeeType_FacultyTasks
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cEmployeeType_FacultyTasks = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET EmployeeType_FacultyTasks = i.EmployeeType_FacultyTasks
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END	
				
			-- Correct changes to EmployeeType_NonClinicalTime
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cEmployeeType_NonClinicalTime = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET EmployeeType_NonClinicalTime = i.EmployeeType_NonClinicalTime
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END	
			
			-- Correct changes to EmployeeType_Trainee
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cEmployeeType_Trainee = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET EmployeeType_Trainee = i.EmployeeType_Trainee
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END
				
			-- Correct changes to EmployeeType_UTStaff
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cEmployeeType_UTStaff = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET EmployeeType_UTStaff = i.EmployeeType_UTStaff
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- TAG CATEGORY: Integrations ----------------------
			-- Correct changes to EmployeeType_APP
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cEmployeeType_APP = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET EmployeeType_APP = i.EmployeeType_APP
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- TAG CATEGORY: Location --------------------------
			-- Correct changes to Location_CUH
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cLocation_CUH = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET Location_CUH = i.Location_CUH
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to Location_CUHCardiac
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cLocation_CUHCardiac = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET Location_CUHCardiac = i.Location_CUHCardiac
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END
	
			-- Correct changes to Location_CUHGeneral
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cLocation_CUHGeneral = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET Location_CUHGeneral = i.Location_CUHGeneral
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END
    
			-- Correct changes to Location_CUHNeuro
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cLocation_CUHNeuro = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET Location_CUHNeuro = i.Location_CUHNeuro
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END
    
 			-- Correct changes to Location_FellowVacation
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cLocation_FellowVacation = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET Location_FellowVacation = i.Location_FellowVacation
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END
				
  			-- Correct changes to Location_ICU
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cLocation_ICU = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET Location_ICU = i.Location_ICU
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END
				
  			-- Correct changes to Location_PainRoles
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cLocation_PainRoles = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET Location_PainRoles = i.Location_PainRoles
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END
				
  			-- Correct changes to Location_PHHS
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cLocation_PHHS = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET Location_PHHS = i.Location_PHHS
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END    

  			-- Correct changes to Location_UH
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cLocation_UH = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET Location_UH = i.Location_UH
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END     

  			-- Correct changes to Location_UHOSC
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cLocation_UHOSC = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET Location_UHOSC = i.Location_UHOSC
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

  			-- Correct changes to Location_VA
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cLocation_VA = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET Location_VA = i.Location_VA
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END   

			-- Correct changes to Location_Zale	
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cLocation_Zale = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET Location_Zale = i.Location_Zale
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

		-- TAG CATEGORY: N/D -----------------------------------
			-- Correct changes to ND_Day
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cND_Day = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET ND_Day = i.ND_Day
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to ND_Night
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cND_Night = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET ND_Night = i.ND_Night
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

		-- TAG CATEGORY: Primary Site --------------------------
			-- Correct changes to PrimarySite_PHHS
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cPrimarySite_PHHS = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET PrimarySite_PHHS = i.PrimarySite_PHHS
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to PrimarySite_UH
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cPrimarySite_UH = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET PrimarySite_UH = i.PrimarySite_UH
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

		-- TAG CATEGORY: Provider Type -------------------------
			-- Correct changes to ProviderType_CRNA
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cProviderType_CRNA = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET ProviderType_CRNA = i.ProviderType_CRNA
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to ProviderType_Fellow
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cProviderType_Fellow = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET ProviderType_Fellow = i.ProviderType_Fellow
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to ProviderType_NP
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cProviderType_NP = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET ProviderType_NP = i.ProviderType_NP
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to ProviderType_PA
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cProviderType_PA = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET ProviderType_PA = i.ProviderType_PA
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to ProviderType_Physician
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cProviderType_Physician = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET ProviderType_Physician = i.ProviderType_Physician
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to ProviderType_Resident
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cProviderType_Resident = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET ProviderType_Resident = i.ProviderType_Resident
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to ProviderType_RRNA
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cProviderType_RRNA = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET ProviderType_RRNA = i.ProviderType_RRNA
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to ProviderType_UTStaff
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cProviderType_UTStaff = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET ProviderType_UTStaff = i.ProviderType_UTStaff
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

		-- TAG CATEGORY: QGenda Admin Tags ----------------------------
			-- Correct changes to QGendaAdminTags_Header
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cQGendaAdminTags_Header = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET QGendaAdminTags_Header = i.QGendaAdminTags_Header
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to QGendaAdminTags_LBL
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cQGendaAdminTags_LBL = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET QGendaAdminTags_LBL = i.QGendaAdminTags_LBL
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

		-- TAG CATEGORY: Shift Length ----------------------------
			-- Correct changes to ShiftLength_8hr
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cShiftLength_8hr = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET ShiftLength_8hr = i.ShiftLength_8hr
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to ShiftLength_10hr
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cShiftLength_10hr = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET ShiftLength_10hr = i.ShiftLength_10hr
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to ShiftLength_11hr
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cShiftLength_11hr = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET ShiftLength_11hr = i.ShiftLength_11hr
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to ShiftLength_12hr
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cShiftLength_12hr = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET ShiftLength_12hr = i.ShiftLength_12hr
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to ShiftLength_13hr
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cShiftLength_13hr = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET ShiftLength_13hr = i.ShiftLength_13hr
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to ShiftLength_14hr
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cShiftLength_14hr = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET ShiftLength_14hr = i.ShiftLength_14hr
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to ShiftLength_16hr
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cShiftLength_16hr = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET ShiftLength_16hr = i.ShiftLength_16hr
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to ShiftLength_24hr
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cShiftLength_24hr = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET ShiftLength_24hr = i.ShiftLength_24hr
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

		-- TAG CATEGORY: System Task Type ----------------------------
			-- Correct changes to SystemTaskType_Unavailable
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cSystemTaskType_Unavailable = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET SystemTaskType_Unavailable = i.SystemTaskType_Unavailable
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to SystemTaskType_Working
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cSystemTaskType_Working = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET SystemTaskType_Working = i.SystemTaskType_Working
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

		-- TAG CATEGORY: Task Grouping -------------------------
			-- Correct changes to TaskGrouping_AIC
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cTaskGrouping_AIC = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET TaskGrouping_AIC = i.TaskGrouping_AIC
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to TaskGrouping_CIC
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cTaskGrouping_CIC = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET TaskGrouping_CIC = i.TaskGrouping_CIC
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to TaskGrouping_Clinic
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cTaskGrouping_Clinic = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET TaskGrouping_Clinic = i.TaskGrouping_Clinic
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to TaskGrouping_ICU
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cTaskGrouping_ICU = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET TaskGrouping_ICU = i.TaskGrouping_ICU
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to TaskGrouping_OR
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cTaskGrouping_OR = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET TaskGrouping_OR = i.TaskGrouping_OR
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to TaskGrouping_Procedure
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cTaskGrouping_Procedure = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET TaskGrouping_Procedure = i.TaskGrouping_Procedure
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to TaskGrouping_Telemedicine
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cTaskGrouping_Telemedicine = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET TaskGrouping_Telemedicine = i.TaskGrouping_Telemedicine
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

		-- TAG CATEGORY: Task Type 1 ---------------------------
			-- Correct changes to TaskType1_Away
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cTaskType1_Away = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET TaskType1_Away = i.TaskType1_Away
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to TaskType1_Call
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cTaskType1_Call = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET TaskType1_Call = i.TaskType1_Call
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to TaskType1_Label
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cTaskType1_Label = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET TaskType1_Label = i.TaskType1_Label
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to TaskType1_NonClinical
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cTaskType1_NonClinical = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET TaskType1_NonClinical = i.TaskType1_NonClinical
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END

			-- Correct changes to TaskType1_Working
			INSERT INTO @RecordsToUpdate
				SELECT TaskKey
				FROM #Comparison
				WHERE cTaskType1_Working = 'Conflict';

			IF (SELECT COUNT(*) FROM @RecordsToUpdate) > 0
				BEGIN
					UPDATE stage.qdm_TaggedTask
					SET TaskType1_Working = i.TaskType1_Working
						, ETLCommand = 'Update'
					FROM stage.qdm_TaggedTask AS s
						INNER JOIN import.qdm_TaggedTask AS i
							ON s.TaskKey = i.TaskKey
						INNER JOIN @RecordsToUpdate AS f
							ON s.TaskKey = f.TaskKey;

					DELETE FROM @RecordsToUpdate;
				END
			PRINT 'Completed applying corrections.';


			-- #NEW RECORDS
			PRINT 'Inserting new records.'
			INSERT INTO stage.qdm_TaggedTask
				SELECT
					fltr.TaskKey
					, InvalidRecordFlag
    
					, CALevel_CA1
					, CALevel_CA2
					, CALevel_CA3
					, CALevel_Intern
					, CALevel_PGY4
					, CALevel_PGY5
					, CALevel_PGY6

					, Capacity_Cardiothoracic
    
					, CRNAType_FT
					, CRNAType_PRN
    
					, Division_Cardiothoracic
					, Division_CriticalCare
					, Division_CUHGeneralALL
					, Division_CUHGeneralPrimary
					, Division_CUHOB
					, Division_Liver
					, Division_Neuro
					, Division_OSCPrimary
					, Division_Pain
					, Division_Pediatrics
					, Division_PHHSGeneral
					, Division_PHHSOBHybrid
					, Division_PHHSOBPrimary
					, Division_PHHSRegional
					, Division_UHRegional
					, Division_ZaleCall
    
					, EmployeeType_APP
					, EmployeeType_FacultyFullTime
					, EmployeeType_FacultyPartTime
					, EmployeeType_FacultyPTNB
					, EmployeeType_FacultyTasks
					, EmployeeType_NonClinicalTime
					, EmployeeType_Trainee
					, EmployeeType_UTStaff
    
					, Integrations_Kronos
    
					, Location_CUH
					, Location_CUHCardiac
					, Location_CUHGeneral
					, Location_CUHNeuro
					, Location_FellowVacation
					, Location_ICU
					, Location_PainRoles
					, Location_PHHS
					, Location_UH
					, Location_UHOSC
					, Location_VA
					, Location_Zale
    
					, ND_Day
					, ND_Night
    
					, PrimarySite_PHHS
					, PrimarySite_UH
    
					, ProviderType_CRNA
					, ProviderType_Fellow
					, ProviderType_NP
					, ProviderType_PA
					, ProviderType_Physician
					, ProviderType_Resident
					, ProviderType_RRNA
					, ProviderType_UTStaff

					, QGendaAdminTags_Header
					, QGendaAdminTags_LBL

					, ShiftLength_8hr
					, ShiftLength_10hr
					, ShiftLength_11hr
					, ShiftLength_12hr
					, ShiftLength_13hr
					, ShiftLength_14hr
					, ShiftLength_16hr
					, ShiftLength_24hr

					, SystemTaskType_Unavailable
					, SystemTaskType_Working
    
					, TaskGrouping_AIC
					, TaskGrouping_CIC
					, TaskGrouping_Clinic
					, TaskGrouping_ICU
					, TaskGrouping_OR
					, TaskGrouping_Procedure
					, TaskGrouping_Telemedicine
    
					, TaskType1_Away
					, TaskType1_Call
					, TaskType1_Label
					, TaskType1_NonClinical
					, TaskType1_Working
					, ETLCommand = 'New'
				FROM import.qdm_TaggedTask AS d
					INNER JOIN @NewRecords AS fltr
						ON d.TaskKey = fltr.TaskKey
			PRINT 'Insertion of new records complete.'

			-- #CLEAN UP
			DROP TABLE #Comparison;
		END
END;	-- OF CREATE PROCEDURE CALL

-- END OF FILE --