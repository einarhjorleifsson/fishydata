# ICES_VMS_QC
Private Repository for internal QC checks for VMS

## QC workflow
1. Find the DATSU submission session IDs [here](https://datsu.ices.dk/web/ListSessions.aspx). Table1 corresponds to VE or VMS data, table2 corresponds to LE or logbook data.
2. Open the Exel workbook (*Submission status & session IDs 20xx.xlsx*) for the current year ([located here](https://icesit.sharepoint.com/sites/ICESsecretariat/Shared%20Documents/Forms/AllItems.aspx?id=%2Fsites%2FICESsecretariat%2FShared%20Documents%2FVMS%20and%20logbook)) and enter the session ID for the current run (update resubmissions when necessary)
3. In your R environment, comment/uncomment the relevant country in *config.json* file before running the scripts.
4. Input the relevant country's session IDs in the SQL query in *config.json* before extraction. For countries with session IDs for each year (i.e. France, UK) list all IDs separated with comma. 
5. Run *source("QC/data.R")* to extract the VE and LE files from the database SQL10, the two CSV files should be found in the folder *data/data_20xx/..*
6. Run the QC report script with *source("QC/report_edit.R")*.
   ###### *NOTE The first time running the QC report markdown script, ensure the correct wd is specified from line #29 of report-QC_format2024editNC.Rmd*
7. When the report is done, copy the QC file in \\\adminweb04\QCreports$
8. Update SQL database with this query: update [tblFileScreening] set QCReportURL = 'https://data.ices.dk/vms/qcreports/NameOfQC.html' where [DATSUSessionID] in (SessionID LE file, SessionID VE file) or by using alternate query (see below)
9. Update the Submission  Status & session IDs Excel file from point 2 [found here](https://icesit.sharepoint.com/sites/ICESsecretariat/Shared%20Documents/Forms/AllItems.aspx?id=%2Fsites%2FICESsecretariat%2FShared%20Documents%2FVMS%20and%20logbook)
10. Send an email to the submitter with the QC link 
11. Once the submitters are ok with the QC, upload the QC report to the WGSFD SharePoint QC reports [Current Year] https://community.ices.dk/ExpertGroups/wgsfd/_layouts/15/start.aspx#/

    *Remember to create a new folder for QC reports on Sharepoint each year: "Site Contents -> Add an App -> Document Library -> name: QC reports [Current Year]"*
    
13. Email the Chair(s) of WGSFD with the link to the QC report


#### *Alternate SQL query for entering QC report URL:*
SELECT [QCReportURL], *  FROM VMS.[dbo].[tblFileScreening]
where [QCReportURL] is not null
update VMS.[dbo].[tblFileScreening] set [QCReportURL] = 'Put the URL here' where DATSUSessionID = 0
