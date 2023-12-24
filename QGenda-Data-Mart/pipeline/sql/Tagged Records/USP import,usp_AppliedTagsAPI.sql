/*	FILE HEADER
 *		File Name:	USP import,usp_AppliedTagsAPI.sql
 *		Author:		Matt Chansard
 *		Project:	QGenda Data Mart
 *
 *	DESCRIPTION
 *		This file contains the definition of a User-Defined Stored Procedure (USP) which accepts
 *		JSON data returned from calls to the QGenda API.
 *
 *		This USP accepts two parameters
 *			- @type : 'Staff' or 'Task'
 *			- @json : a JSON string
 *
 *		Depending on whether @type = 'Staff' or 'Task', the JSON string in @json is read into
 *		[import.TagsAPI] differently.
 */

ALTER PROCEDURE import.usp_AppliedTagsAPI
(
	@type NVARCHAR(5) = N'',
	@json NVARCHAR(MAX)
)
AS
BEGIN
	SET NOCOUNT ON;
	
	IF (SELECT COUNT(*) FROM import.TagsAPI) > 0
		TRUNCATE TABLE import.TagsAPI;

	IF @type = 'Task'
		INSERT INTO import.TagsAPI (EntityKey, EntityType, Tags)
			SELECT
				tasks.TaskKey
				, 'Task'
				, tasks.Tags
			FROM OPENJSON(@json)
			WITH 
			(	TaskKey			NVARCHAR(40)	'$.TaskKey'
				, Tags			NVARCHAR(MAX)	'$.Tags' AS JSON
			) AS tasks;


	IF @type = 'Staff'
		INSERT INTO import.TagsAPI (EntityKey, EntityType, Tags)
			SELECT
				staff.StaffKey
				, 'Staff'
				, staff.Tags
			FROM OPENJSON(@json)
			WITH 
			(	StaffKey			NVARCHAR(40)	'$.StaffKey'
				, Tags				NVARCHAR(MAX)	'$.Tags' AS JSON
			) AS staff;
END;
-- END OF FILE
