/*	FILE HEADER
 *		File Name:	TABLE ProviderCaseLOG ETL.sql
 *		Author:		Matt C
 *		Project:	Epic Pipeline - Provider Case Logs
 *
 *	DESCRIPTION
 *		This file contains the definition of a database objects (Tables) to handle the ETL of 
 *		clinical case records from Epic Clarity.
 *	
 *	COLUMN DICTIONARY
 *		ProviderType
 *		 - 1: Anesthesiologist
 *		 - 2: ANESTHESIOLOGY FELLOW
 *		 - 3: Anesthesiology Resident
 *		 - 4: CRNA
 *		 - 5: Perfusionist
 *		 - 6: RESIDENT REGISTERED NURSE ANESTHETIST
 */


DROP TABLE import.ProviderCaseLog_OpTime;
CREATE TABLE import.ProviderCaseLog_OpTime
(
	CaseID						INT				-- LOG_ID
	, LineNumber				TINYINT
	, CaseDate					DATE			-- AN_DATE
	, ProviderEpicID			NVARCHAR(12)	-- PROV_ID
	, ProviderType				TINYINT			-- dim.ZC_OR_ANSTAFF_TYPE.NAME
	, ProviderAnesthesiaStart	DATETIME		-- AN_BEGIN_LOCAL_DTTM
	, ProviderAnesthesiaStop	DATETIME		-- AN_END_LOCAL_DTTM
	, PatientDischarged			DATETIME		-- anes.vw_OR_LogTrackingTimes.PatientDischarged_DateTime
	, LogStatus					TINYINT			-- OR_LOG.STATUS_C
	, CaseStatus				TINYINT			-- OR_LOG.OR_TIME_EVTS_ENT_C
	, LastUpdate				DATETIME		-- anes.F_AN_RECORD_SUMMARY.UPDATE_DATE
);

DROP TABLE import.ProviderCaseLog_Cadence;
CREATE TABLE import.ProviderCaseLog_Cadence
(
	CaseID						INT				-- LOG_ID
	, CaseDate					DATE			-- AN_DATE
	, ProviderEpicID			NVARCHAR(12)	-- PROV_ID
	, ProviderType				TINYINT			-- dim.ZC_OR_ANSTAFF_TYPE.NAME
	, ProviderAnesthesiaStart	DATETIME		-- AN_BEGIN_LOCAL_DTTM
	, ProviderAnesthesiaStop	DATETIME		-- AN_END_LOCAL_DTTM
	, LogStatus					TINYINT			-- OR_LOG.STATUS_C
	, CaseStatus				TINYINT			-- OR_LOG.OR_TIME_EVTS_ENT_C
	, LastUpdate				DATETIME		-- anes.F_AN_RECORD_SUMMARY.UPDATE_DATE
);

DROP TABLE stage.ProviderCaseLog;
CREATE TABLE stage.ProviderCaseLog
(
	CaseID						INT				-- LOG_ID
	, LineNumber				TINYINT
	, CaseDate					DATE			-- AN_DATE
	, ProviderEpicID			NVARCHAR(12)	-- PROV_ID
	, ProviderType				TINYINT			-- dim.ZC_OR_ANSTAFF_TYPE.NAME
	, ProviderAnesthesiaStart	DATETIME		-- AN_BEGIN_LOCAL_DTTM
	, ProviderAnesthesiaStop	DATETIME		-- AN_END_LOCAL_DTTM
	, PatientDischarged			DATETIME		-- anes.vw_OR_LogTrackingTimes.PatientDischarged_DateTime
	, LogStatus					TINYINT			-- OR_LOG.STATUS_C
	, CaseStatus				TINYINT			-- OR_LOG.OR_TIME_EVTS_ENT_C
	, LastUpdate				DATETIME		-- anes.F_AN_RECORD_SUMMARY.UPDATE_DATE
	, ETLCommand				NVARCHAR(10)	-- UNCHANGED, UPDATE, NEW
);

-- END OF FILE