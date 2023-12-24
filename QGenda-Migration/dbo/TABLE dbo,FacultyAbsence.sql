USE Reports;

/* FILE HEADER
 *	File Name:  TABLE dbo,FacultyAbsence.py
 *	Author:     Matt Chansard
 *	Project:    QGenda Migration (MTQT)
 *
 *	DESCRIPTION
 *	  This TABLE is houses UH, PH, and CMC Faculty absences.  Records are source from Excel flat
 *	  files (CMC) as well as the QGenda Data Mart (UH, PH).
 *
 *	TECHNICAL COMMENTS
 *	 - Deployed to the Reports databse on ANESCore.swmed.org
*/

CREATE TABLE dbo.FacultyAbsence
(
	PersonID			INT				NOT NULL
	, FacultyFullName	NVARCHAR(30)
	, ScheduleDate		DATE			NOT NULL
	, PayCode			NVARCHAR(15)	NOT NULL
	, ImportPayHours	NVARCHAR(5)
	, ReportPayHours	NVARCHAR(5)
	, FTE				DECIMAL(2,1)
	, FacultyGroup		NVARCHAR(3)
	, RecordAddedDate	DATETIME		DEFAULT GETDATE()
	, RecordType		NVARCHAR(15)
	, ReportMonth		TINYINT			
	, ReportYear		SMALLINT		
	, ProducedErrorYN	NCHAR(1)		DEFAULT 'N'
	, ErrorSource		NCHAR(3)		
	, ErrorComment		NVARCHAR(200)
);

-- END OF FILE --