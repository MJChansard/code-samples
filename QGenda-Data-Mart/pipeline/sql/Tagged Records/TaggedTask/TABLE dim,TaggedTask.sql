/*	FILE HEADER
 *		File Name:	TABLE dim,TaggedTask.sql
 *		Author:		Matt Chansard
 *		Project:	QGenda Data Mart
 *
 *	DESCRIPTION
 *		This file defines a database object designed to receive processed QGenda Task Tag records
 *		from ANES-ETL1.  This table is used for reporting purposes.
 */

-- Connect to ANESCore
USE QGenda;
DROP TABLE IF EXISTS dim.TaggedTask;
CREATE TABLE dim.TaggedTask
(
	TaskKey							UNIQUEIDENTIFIER
	, InvalidRecordFlag				TINYINT
	-- Tag Category: CA Level
	, CALevel_CA1					TINYINT
	, CALevel_CA2					TINYINT
	, CALevel_CA3					TINYINT
	, CALevel_Intern				TINYINT
	, CALevel_PGY4					TINYINT
	, CALevel_PGY5					TINYINT
	, CALevel_PGY6					TINYINT
	-- Tag Category: Capacity Tab
	, Capacity_Cardiothoracic		TINYINT
	-- Tag Category: CRNA Type
	, CRNAType_FT					TINYINT
	, CRNAType_PRN					TINYINT
	-- Tag Category: Division
	, Division_Cardiothoracic		TINYINT		-- 1
	, Division_CriticalCare			TINYINT
	, Division_CUHGeneralALL		TINYINT
	, Division_CUHGeneralPrimary	TINYINT
	, Division_CUHOB				TINYINT
	, Division_Liver				TINYINT		-- 5
	, Division_Neuro				TINYINT
	, Division_OSCPrimary			TINYINT
	, Division_Pain					TINYINT
	, Division_Pediatrics			TINYINT
	, Division_PHHSGeneral			TINYINT		-- 10
	, Division_PHHSOBHybrid			TINYINT
	, Division_PHHSOBPrimary		TINYINT
	, Division_PHHSRegional			TINYINT
	, Division_UHRegional			TINYINT		-- 15
	, Division_ZaleCall				TINYINT
	-- Tag Category: Employee Type
	, EmployeeType_APP				TINYINT
	, EmployeeType_FacultyFullTime	TINYINT
	, EmployeeType_FacultyPartTime	TINYINT
	, EmployeeType_FacultyPTNB		TINYINT
	, EmployeeType_FacultyTasks		TINYINT
	, EmployeeType_NonClinicalTime	TINYINT
	, EmployeeType_Trainee			TINYINT
	, EmployeeType_UTStaff			TINYINT
	-- Tag Category: Integrations
	, Integrations_Kronos			TINYINT
	-- Tag Category: Location
	, Location_CUH					TINYINT
	, Location_CUHCardiac			TINYINT
	, Location_CUHGeneral			TINYINT
	, Location_CUHNeuro				TINYINT
	, Location_FellowVacation		TINYINT
	, Location_ICU					TINYINT
	, Location_PainRoles			TINYINT
	, Location_PHHS					TINYINT
	, Location_UH					TINYINT	
	, Location_UHOSC				TINYINT
	, Location_VA					TINYINT
	, Location_Zale					TINYINT
	-- Tag Category: N/D
	, ND_Day						TINYINT
	, ND_Night						TINYINT
	-- Tag Category: Primary Site
	, PrimarySite_PHHS				TINYINT
	, PrimarySite_UH				TINYINT
	-- Tag Category: Provider Type
	, ProviderType_CRNA				TINYINT
	, ProviderType_Fellow			TINYINT
	, ProviderType_NP				TINYINT
	, ProviderType_PA				TINYINT
	, ProviderType_Physician		TINYINT
	, ProviderType_Resident			TINYINT
	, ProviderType_RRNA				TINYINT
	, ProviderType_UTStaff			TINYINT
	-- Tag Category: QGenda Admin Tags
	, QGendaAdminTags_Header		TINYINT
	, QGendaAdminTags_LBL			TINYINT
	-- Tag Category: Shift Length
	, ShiftLength_8hr				TINYINT
	, ShiftLength_10hr				TINYINT
	, ShiftLength_11hr				TINYINT
	, ShiftLength_12hr				TINYINT
	, ShiftLength_13hr				TINYINT
	, ShiftLength_14hr				TINYINT
	, ShiftLength_16hr				TINYINT
	, ShiftLength_24hr				TINYINT
	-- Tag Category: System Task Type
	, SystemTaskType_Unavailable	TINYINT
	, SystemTaskType_Working		TINYINT
	-- Tag Category: Task Grouping
	, TaskGrouping_AIC				TINYINT
	, TaskGrouping_CIC				TINYINT
	, TaskGrouping_Clinic			TINYINT
	, TaskGrouping_ICU				TINYINT
	, TaskGrouping_OR				TINYINT
	, TaskGrouping_Procedure		TINYINT
	, TaskGrouping_Telemedicine		TINYINT
	-- Tag Category: Task Type 1
	, TaskType1_Away				TINYINT
	, TaskType1_Call				TINYINT
	, TaskType1_Label				TINYINT
	, TaskType1_NonClinical			TINYINT
	, TaskType1_Working				TINYINT
);
	-- 85 columns
-- END OF FILE --