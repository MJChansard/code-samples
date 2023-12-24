/*	FILE HEADER
 *		File Name:	TABLE dbo,ProviderCaseLog.sql
 *		Author:		Matt C
 *		Project:	Epic Pipeline - Provider Case Logs
 *
 *	DESCRIPTION
 *		This file contains the definition of a Tables to house the clinical case activity of 
 *		providers.  These records are the received from ETLServer
 *	
 *	COLUMN DICTIONARY
 *		ProviderType
 *		 - 1: Anesthesiologist
 *		 - 2: ANESTHESIOLOGY FELLOW
 *		 - 3: Anesthesiology Resident
 *		 - 4: CRNA
 *		 - 5: Perfusionist
 *		 - 6: RESIDENT REGISTERED NURSE ANESTHETIST
 *
 *		LogStatus
 *		 - 1: Missing Information
 *		 - 2: Posted
 *		 - 3: Unposted
 *		 - 4: Voided
 *		 - 5: Completed
 *		 - 6: Canceled
 *
 *		CaseStatus
 *		 - 0: Not Started
 *		 - 1: In Progress
 *		 - 2: Completed
 */
DROP TABLE dbo.ProviderCaseLog;
CREATE TABLE dbo.ProviderCaseLog
(
	CaseID						INT				-- case_id
	, LineNumber				TINYINT
	, CaseDate					DATE			-- F_AN_RECORD_SUMMARY.AN_DATE
	, ProviderEpicID			NVARCHAR(12)	-- prov_id
	, ProviderType				TINYINT			-- Anes_Role
	, ProviderAnesthesiaStart	DATETIME
	, ProviderAnesthesiaStop	DATETIME
	, PatientDischarged			DATETIME		-- anes.vw_OR_LogTrackingTimes.PatientDischarged_DateTime
	, LogStatus					TINYINT			-- OR_LOG.STATUS_C
	, CaseStatus				TINYINT			-- OR_LOG.OR_TIME_EVTS_ENT_C
	, LastUpdate				DATETIME
);




-- VALIDATION QUERIES ---------
-- Connect to EDW.ClarityMirror
SELECT
	COUNT(DISTINCT CASE_ID) AS CaseCount
FROM anes.F_AN_RECORD_SUMMARY
WHERE AN_DATE BETWEEN '2022-01-01' AND '2022-04-30';

SELECT
	MIN(LEN(CASE_ID)) AS MinCaseIdLen
	, MAX(LEN(CASE_ID)) AS MaxCaseIdLen
FROM anes.F_AN_RECORD_SUMMARY

SELECT
	CASE_ID					AS OriginalCaseId
	, CAST(CASE_ID AS INT)	AS NewCaseId
FROM anes.F_AN_RECORD_SUMMARY
WHERE AN_DATE BETWEEN '2015-01-01' AND '2022-04-30';

-- Validating [PROV_ID]
SELECT
	MIN(LEN(PROV_ID)) AS MinProvIdLen
	, MAX(LEN(PROV_ID)) AS MaxProvIdLen
FROM dim.CLARITY_SER
WHERE NOT PROV_ID LIKE '%@/dev/pts/%'

SELECT
	PROV_ID							AS OriginalProvId
	, CAST(PROV_ID AS NVARCHAR(12))	AS NewProvId
FROM dim.CLARITY_SER
WHERE ACTIVE_STATUS = 'Active'

SELECT DISTINCT PROV_ID
FROM dim.CLARITY_SER
WHERE LEN(PROV_ID) = 12

-- END OF FILE --