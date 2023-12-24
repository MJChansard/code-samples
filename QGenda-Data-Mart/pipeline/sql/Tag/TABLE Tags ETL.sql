/*	FILE HEADER
 *		File Name:	TABLE Tags ETL.sql
 *		Author:		Matt Chansard
 *		Project:	QGenda Data Mart
 *
 *	DESCRIPTION
 *		This file contains database object definitions related to the ETL of QGenda Tag records.
 *		These objects are deployed on ANES-ETL1.swmed.org.
 */

-- Connect to ANES-ETL1
USE StagingQGenda;

DROP TABLE IF EXISTS import.Tags;                       -- Previous name of object
DROP TABLE IF EXISTS import.qdm_Tag;
CREATE TABLE import.qdm_Tag
(
	CategoryKey							BIGINT
	, CategoryName						NVARCHAR(30)
	, CategoryCreatedDateTime			DATETIME
	, CategoryModifiedDateTime			DATETIME
	, TagKey							BIGINT
	, TagName							NVARCHAR(30)
	, TagCreatedDateTime				DATETIME
	, TagModifiedDateTime				DATETIME
	, IsAvailableForCreditAllocation	NCHAR(1)
	, IsAvailableForHoliday				NCHAR(1)
	, IsAvailableForLocation			NCHAR(1)
    , IsAvailableForProfile				NCHAR(1)
    , IsAvailableForRequestLimit		NCHAR(1)
    , IsAvailableForScheduleEntry		NCHAR(1)
    , IsAvailableForSeries				NCHAR(1)
    , IsAvailableForStaff				NCHAR(1)
    , IsAvailableForStaffLocation		NCHAR(1)
    , IsAvailableForStaffTarget			NCHAR(1)
    , IsAvailableForTask				NCHAR(1)
    , IsFilterOnAdmin					NCHAR(1)
    , IsFilterEverywhereExceptAdmin		NCHAR(1)
    , IsPermissionCategory				NCHAR(1)
    , IsSingleTaggingOnly				NCHAR(1)
    , IsTTCMCategory					NCHAR(1)
    , IsUsedForFiltering				NCHAR(1)
    , IsUsedForStats					NCHAR(1)
);
	-- Deployed 07-20-2022
	-- 26 columns

DROP TABLE IF EXISTS stage.Tags;                        -- Previous name of object
DROP TABLE IF EXISTS stage.qdm_Tag;
CREATE TABLE stage.qdm_Tag
(
	CategoryKey							BIGINT
	, CategoryName						NVARCHAR(30)
	, CategoryCreatedDateTime			DATETIME
	, CategoryModifiedDateTime			DATETIME
	, TagKey							BIGINT
	, TagName							NVARCHAR(30)
	, TagCreatedDateTime				DATETIME
	, TagModifiedDateTime				DATETIME
	, IsAvailableForCreditAllocation	NCHAR(1)
	, IsAvailableForHoliday				NCHAR(1)
	, IsAvailableForLocation			NCHAR(1)
    , IsAvailableForProfile				NCHAR(1)
    , IsAvailableForRequestLimit		NCHAR(1)
    , IsAvailableForScheduleEntry		NCHAR(1)
    , IsAvailableForSeries				NCHAR(1)
    , IsAvailableForStaff				NCHAR(1)
    , IsAvailableForStaffLocation		NCHAR(1)
    , IsAvailableForStaffTarget			NCHAR(1)
    , IsAvailableForTask				NCHAR(1)
    , IsFilterOnAdmin					NCHAR(1)
    , IsFilterEverywhereExceptAdmin		NCHAR(1)
    , IsPermissionCategory				NCHAR(1)
    , IsSingleTaggingOnly				NCHAR(1)
    , IsTTCMCategory					NCHAR(1)
    , IsUsedForFiltering				NCHAR(1)
    , IsUsedForStats					NCHAR(1)
	, ETLCommand						NCHAR(6)
);
	-- Deployed 07-20-2022
	-- 27 columns

-- END OF FILE --
