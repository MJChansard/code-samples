/*	FILE HEADER
 *		File Name:	TABLE import,TagsAPI.sql
 *		Author:		Matt Chansard
 *		Project:	QGenda Data Mart
 *
 *	DESCRIPTION
 *		This file contains database object definitions related to the ETL of QGenda StaffMember
 *		and Task tag records.  These objects are deployed on ANES-ETL1.swmed.org.
 */


-- Connect to ANES-ETL1
DROP TABLE IF EXISTS import.QGendaAPI;		-- Previous name of object
DROP TABLE IF EXISTS import.TagsAPI;
CREATE TABLE import.TagsAPI
(
	EntityKey		NVARCHAR(40)
	, EntityType	NVARCHAR(5)
	, Tags			NVARCHAR(MAX)
);

-- END OF FILE
