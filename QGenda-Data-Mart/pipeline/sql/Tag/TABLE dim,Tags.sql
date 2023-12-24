/*	FILE HEADER
 *		File Name:	TABLE dim,Tags.sql
 *		Author:		Matt Chansard
 *		Project:	QGenda Data Mart
 *
 *	DESCRIPTION
 *		This file defines a database object designed to receive processed QGenda Tag records from
 *		ANES-ETL1.  This table is used for reporting purposes.
 */


-- Connect to ANESCore
USE QGenda;

DROP TABLE IF EXISTS dim.Tag;
GO
CREATE TABLE dim.Tag
(
	CategoryKey							BIGINT
	, CategoryName						NVARCHAR(30)
	, CategoryCreatedDateTime			DATETIME
	, CategoryModifiedDateTime			DATETIME
	, TagKey							BIGINT			PRIMARY KEY NONCLUSTERED
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
	-- Deployed 2022-07-20
	-- 26 columns

-- END OF FILE --