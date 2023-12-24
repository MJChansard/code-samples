/*	FILE HEADER
 *		File Name:	TABLE TaggedStaff ETL.sql
 *		Author:		Matt Chansard
 *		Project:	QGenda Data Mart
 *
 *	DESCRIPTION
 *		This file contains database object definitions related to the ETL of QGenda StaffMember
 *		records and how they are tagged.  These objects are deployed on ANES-ETL1.swmed.org.
 */


-- Connect to ANES-ETL1
USE StagingQGenda;
DROP TABLE IF EXISTS stage.TaggedStaff;					-- Previous name of object
DROP TABLE IF EXISTS import.qdm_TaggedStaff;
CREATE TABLE import.qdm_TaggedStaff
(
	StaffKey						UNIQUEIDENTIFIER
	, InvalidRecordFlag				TINYINT
	-- Tag Category: CA Level
	, CALevel_CA1					TINYINT
	, CALevel_CA2					TINYINT
	, CALevel_CA3					TINYINT
	, CALevel_Intern				TINYINT
	, CALevel_PGY4					TINYINT
	, CALevel_PGY5					TINYINT
	, CALevel_PGY6					TINYINT
	-- Tag Category: Capacity
	, Capacity_Cardiothoracic		TINYINT
	-- Tag Category: CRNA Type
	, CRNAType_FT					TINYINT
	, CRNAType_PRN					TINYINT
	-- Tag Category: CUH
	, CUH_CUH10hr					TINYINT
	, CUH_CUH13hr					TINYINT
	-- Tag Category: Division
	, Division_Cardiothoracic		TINYINT		-- 1
	, Division_CriticalCare			TINYINT
	, Division_CUHGeneralALL		TINYINT
	, Division_CUHGeneralPrimary	TINYINT
	, Division_CUHOB				TINYINT		-- 5
	, Division_Liver				TINYINT		
	, Division_Neuro				TINYINT
	, Division_OSCPrimary			TINYINT
	, Division_Pain					TINYINT
	, Division_Pediatrics			TINYINT		-- 10
	, Division_PHHSGeneral			TINYINT		
	, Division_PHHSOBCUHCore		TINYINT		
	, Division_PHHSOBHybrid			TINYINT
	, Division_PHHSOBPrimary		TINYINT
	, Division_PHHSRegional			TINYINT		-- 15
	, Division_UHRegional			TINYINT		
	, Division_ZaleCall				TINYINT
	-- Tag Category: Employee
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
	-- Tag Category: MD - Simulation
	, MDSimulation_SIM				TINYINT
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
	-- Tag Category: Staff Primary Location
	, StaffPrimaryLocation_CUH		TINYINT
	, StaffPrimaryLocation_Zale		TINYINT
	-- Tag Category: TTCM Mock Punch
	, TTCMMockPunch_MDs				TINYINT
);

DROP TABLE IF EXISTS stage.qdm_TaggedStaff;
CREATE TABLE stage.qdm_TaggedStaff
(
	StaffKey						UNIQUEIDENTIFIER
	, InvalidRecordFlag				TINYINT
	-- Tag Category: CA Level
	, CALevel_CA1					TINYINT
	, CALevel_CA2					TINYINT
	, CALevel_CA3					TINYINT
	, CALevel_Intern				TINYINT
	, CALevel_PGY4					TINYINT
	, CALevel_PGY5					TINYINT
	, CALevel_PGY6					TINYINT
	-- Tag Category: Capacity
	, Capacity_Cardiothoracic		TINYINT
	-- Tag Category: CRNA Type
	, CRNAType_FT					TINYINT
	, CRNAType_PRN					TINYINT
	-- Tag Category: CUH
	, CUH_CUH10hr					TINYINT
	, CUH_CUH13hr					TINYINT
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
	, Division_PHHSOBCUHCore		TINYINT			
	, Division_PHHSOBPrimary		TINYINT
	, Division_PHHSRegional			TINYINT
	, Division_UHRegional			TINYINT		-- 15
	, Division_ZaleCall				TINYINT
	-- Tag Category: Employee
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
	-- Tag Category: MD - Simulation
	, MDSimulation_SIM				TINYINT
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
	-- Tag Category: Staff Primary Location
	, StaffPrimaryLocation_CUH		TINYINT
	, StaffPrimaryLocation_Zale		TINYINT
	-- Tag Category: TTCM Mock Punch
	, TTCMMockPunch_MDs				TINYINT
	, ETLCommand					NVARCHAR(6)		
);

-- END OF FILE --