/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/*                            Download WRDS dataset                           */
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------- */
/*                                  Preamble                                  */
/* -------------------------------------------------------------------------- */

/* ----------------------------- User parameters ---------------------------- */

** IMPORTANT: Please change the two parameters to match your setup;
** All other file references are relative to these two parameters;

%let PROJECT_FOLDER = "E:\Dropbox\Work\Research\COVID and EA Timing\covid-empirical";
%let REMOTE_FOLDER = '/home/washington/tcjkok/'; 

/* ----------------------------- File parameters ---------------------------- */

%let LOCAL_STORAGE_FOLDER = "%SYSFUNC(DEQUOTE(&PROJECT_FOLDER.))\2_pipeline\b_1_download_wrds_data";

/* ------------------------ Change working directory ------------------------ */

  data _null_; 
      rc=dlgcdir(&PROJECT_FOLDER);
      put rc=;
   run;

/* --------------------------- Set project folder --------------------------- */

libname STORAGE &LOCAL_STORAGE_FOLDER;

/* ------------------------------- Set up WRDS ------------------------------ */

%let wrds = wrds.wharton.upenn.edu 4016; 
options comamid=TCP remote=WRDS;
signon username=_prompt_;
Libname rwork slibref=work server=wrds;

%SYSLPUT REMOTE_FOLDER = &REMOTE_FOLDER;

rsubmit;
	libname  home &REMOTE_FOLDER;
	libname temp '/sastemp1';
endrsubmit;

rsubmit log=purge;
	%include '/wrds/lib/utility/wrdslib.sas';
	options sasautos=('/wrds/wrdsmacros/', SASAUTOS) MAUTOSOURCE;
endrsubmit; 

/* ---------------------------- Freeze parameters --------------------------- */

%let todaysDate = %sysfunc(today(), mmddyyn8.);
%SYSLPUT todaysDate = &todaysDate;
%put WARNING: The data string used to save the data is: &todaysDate;

/* -------------------------------------------------------------------------- */
/*                                Download data                               */
/* -------------------------------------------------------------------------- */

/* -------------------------------- Compustat ------------------------------- */

** Submit job remotely;

rsubmit;
	data compustat (keep= DATADATE GVKEY Cal_year cik FYEARQ EPSPXQ FQTR RDQ PRCCQ CSHOQ CSHOPQ EXCHG CAPXY PPENTQ ATQ CEQQ IBQ NIQ DPQ OANCFY 
	XRDQ FCAQ FCAY SALEQ FIC CURCDQ AQCY DATACQTR DATAFQTR CEQQ TXDBQ DLCQ DLTTQ PRCCQ EPSPIQ EPSFIQ APDEDATEQ EPSFIQ EPSFXQ CONM
	GDWLIPQ WDPQ SPIQ AQPQ GLPQ GDWLIPQ SETPQ RCPQ WDPQ DTEPQ RDIPQ SPIOPQ XOPRQ ATQ SALEQ RECCHY INVCHY APALCHY TXACHY AOLOCHY NIQ CEQQ FYR DATAFQTR);
		set compd.fundq;
		where (indfmt='INDL') and (datafmt='STD') and (popsrc='D') and (consol='C') and (CURCDQ='USD');
		if  FIC = 'USA';
		if year(datadate)>2010;
		Cal_year=year(datadate);
	run;
endrsubmit;

** Download data to local;

rsubmit;
	proc download data=compustat out=STORAGE.compustat&todaysDate.;
	run;
endrsubmit;

/* ------------------------------ Zipcode data ------------------------------ */

** Submit job remotely;

rsubmit;
    data company;
        set comp.company;
        zipcode=substr(addzip, 1, 5);
		if not(verify(trim(left(zipcode)),'0123456789')) eq 0 then delete;
        zipcode_num = input(zipcode, 5.);
        format zipcode_num z5.;
    run;
endrsubmit; 

** Download data to local;

rsubmit;
	proc download data=company out=STORAGE.company&todaysDate.;
	run;
endrsubmit;

/* ---------------------- SASHELP file for county name ---------------------- */

** Submit job remotely;

rsubmit;
    data sashelpzipcode;
        set sashelp.zipcode;
    run;
endrsubmit;

** Download data to local;

rsubmit;
    proc download data=sashelpzipcode out=STORAGE.sashelpzipcode&todaysDate.;
    run;
endrsubmit;

/* --------------------------- Industry SIC Codes --------------------------- */

** Submit job remotely;

rsubmit;
    data sic_codes (keep = datadate gvkey sich);
    set comp.funda;
    run;
endrsubmit;

** Download data to local;

rsubmit;
    proc download data=sic_codes out=STORAGE.sic_codes&todaysDate.;
    run;
endrsubmit;

/* --------------------- PERMNO from CRSP Linking Table --------------------- */

** Submit job remotely;

rsubmit;
    data ccmxpf_lnkhist;
        set crsp.ccmxpf_lnkhist;
    run;
endrsubmit;

** Download data to local;

rsubmit;
    proc download data=ccmxpf_lnkhist out=STORAGE.ccmxpf_lnkhist&todaysDate.;
    run;
endrsubmit;

/* ------------------ IBES Tickers - Security Table method ------------------ */

** Submit job remotely;

rsubmit;
    data compustatibes;
        set comp.security;
    run;
endrsubmit;

** Download data to local;

rsubmit;
    proc download data=compustatibes out=STORAGE.compustatibes&todaysDate.;
    run;
endrsubmit;

/* ------------------- IBES Tickers - ICLINK Macro method ------------------- */

** Submit job remotely;

rsubmit;
    %ICLINK (IBESID=IBES.ID,CRSPID=CRSP.STOCKNAMES,OUTSET=ICLINK);
endrsubmit;

** Download data to local;

rsubmit;
    proc download data=ICLINK out=STORAGE.ICLINK&todaysDate.;
    run;
endrsubmit;

/* -------------------------------- CRSP Data ------------------------------- */

rsubmit;
    /* Step1. Specifying Options */
    /* Select Date Ranges for CRSP and Thomson Data                   */
    %let begdate = 01JAN2008;
    %let enddate = 31DEC2024;

    /* Create a CRSP Subsample with Monthly Stock and Event Variables */
    /* Restriction on the type of shares (common stocks only)         */
    %let sfilter = (shrcd in (10,11));

    /* Selected variables from the CRSP monthly data file (crsp.msf)  */
    %let msfvars = prc ret shrout cfacpr cfacshr vol date permno;

    /* Selected variables from the CRSP monthly event file (crsp.mse) */
    %let msevars = ncusip exchcd shrcd ;

    /* This procedure creates a Monthly CRSP dataset named "CRSP_D"   */
    %crspmerge(s=d,start=&begdate,end=&enddate,sfvars=&msfvars,sevars=&msevars,filters=&sfilter);
endrsubmit;

** Download data to local;

rsubmit;
    proc download data=crsp_d out=STORAGE.crsp_d&todaysDate.;
    run;
endrsubmit;

/* ------------------------------ Trading days ------------------------------ */

** Submit job remotely;

rsubmit;
    data tradingdays (keep=caldt);
        set crsp.dsix;
    run;
endrsubmit;

** Download data to local;

rsubmit;
    proc download data=tradingdays out=STORAGE.tradingdays&todaysDate.;
    run;
endrsubmit;

/* ------------------ Earnings announcement dates from IBES ----------------- */

** Submit job remotely;

rsubmit;
    data actuals;
        set ibes.ACTU_EPSUS;
        if PDICITY='QTR';
        if value ne .;
    run;
endrsubmit;

** Download data to local;

rsubmit;
    proc download data=actuals out=STORAGE.actuals&todaysDate.;
    run;
endrsubmit;

/* --------------- Unexpected earnings from IBES Summary File --------------- */

** Submit job remotely;

rsubmit;
    data ibes_SUM_QTR;
        set ibes.STATSUM_EPSUS;
        if ticker ne " ";
        *if anndats ne .;
        if FPI="6";
        month_sum= month(statpers);
        year_sum= year(statpers);	
    run;
endrsubmit;

** Download data to local;

rsubmit;
    proc download data=ibes_SUM_QTR out=STORAGE.ibes_SUM_QTR&todaysDate.;
    run;
endrsubmit;

/* ------------------------ Market returns from CSRP ------------------------ */

** Submit job remotely;

rsubmit;
    data crspdsi;
        set crsp.dsi;
    run;
endrsubmit;

** Download data to local;

rsubmit;
    proc download data=crspdsi out=STORAGE.crspdsi&todaysDate.;
    run;
endrsubmit;

/* ---------------------------- Delisting returns --------------------------- */

** Submit job remotely;

rsubmit;
    proc sql;
        create table rvtemp as
        select * from crsp.dse
        where dlstcd > 199 and 1960 le year(DATE) le 2020
        order by dlstcd;
        * modify year range as needed;
    quit;
endrsubmit;

** Download data to local;

rsubmit;
    proc download data=rvtemp out=STORAGE.rvtemp&todaysDate.;
    run;
endrsubmit;

/* ------------------------------ CSRP Monthly ------------------------------ */

** Submit job remotely;

rsubmit;
    /* Step1. Specifying Options */
    /* Select Date Ranges for CRSP and Thomson Data                   */
    %let begdate = 01JAN2008;
    %let enddate = 31DEC2024;

    /* Create a CRSP Subsample with Monthly Stock and Event Variables */
    /* Restriction on the type of shares (common stocks only)         */
    %let sfilter = (shrcd in (10,11));

    /* Selected variables from the CRSP monthly data file (crsp.msf)  */
    %let msfvars = prc ret shrout cfacpr cfacshr vol date permno;

    /* Selected variables from the CRSP monthly event file (crsp.mse) */
    %let msevars = ncusip exchcd shrcd ;

    /* This procedure creates a Monthly CRSP dataset named "CRSP_M"   */
    %crspmerge(s=m,start=&begdate,end=&enddate,sfvars=&msfvars,sevars=&msevars,filters=&sfilter);
endrsubmit;

** Download data to local;

rsubmit;
    proc download data=crsp_m out=STORAGE.crsp_m&todaysDate.;
    run;
endrsubmit;

/* --------- Obtain historical CIK from the GVKEY-CIK linking table --------- */

** Submit job remotely;

rsubmit;
    data ciklink;
        set wrdssec.wciklink_gvkey;
        if flag=2 or flag=3;
        if SOURCE="COMPN" then SOURCE2=4;
        if SOURCE="CUSIP" then SOURCE2=3;
        if SOURCE="FDATE" then SOURCE2=2;
        if SOURCE="COMPH" then SOURCE2=1;	
    run;
endrsubmit;

** Download data to local;

rsubmit;
    proc download data=ciklink out=STORAGE.ciklink&todaysDate.;
    run;
endrsubmit;

/* --------------------------- 10-Q/K filing dates -------------------------- */

** Submit job remotely;

rsubmit;
    proc sql;
        create	table sec1 as
        select	*
        from	wrdssec.exhibits
        where	form in("10-Q","10-K")
        and 	sequence=1
        and		fdate >= "01JAN2010"d;
    quit;
endrsubmit;

** Download data to local;

rsubmit;
    proc download data=sec1 out=STORAGE.sec1&todaysDate.;
    run;
endrsubmit;

/* --------------- Retrieve NT filings from WRDS SEC Analytics -------------- */

** Submit job remotely;

rsubmit;
    proc sql;
        create	table sec1NT as
        select	*
        from	wrdssec.exhibits
        where	form in("NT 10-Q","NT 10-K")
        and 	sequence=1
        and		fdate >= "01JAN2010"d;
    quit;
endrsubmit;

** Download data to local;

rsubmit;
    proc download data=sec1NT out=STORAGE.sec1NT&todaysDate.;
    run;
endrsubmit;

/* ----------- Retrieve 10-Q/K Amendments from WRDS SEC Analytics ----------- */

** Submit job remotely;

rsubmit;
    proc sql;
        create	table sec1Am as
        select	*
        from	wrdssec.exhibits
        where	form in("10-Q/A","10-K/A")
        and 	sequence=1
        and		fdate >= "01JAN2010"d;
    quit;
endrsubmit;

** Download data to local;

rsubmit;
    proc download data=sec1Am out=STORAGE.sec1Am&todaysDate.;
    run;
endrsubmit;

/* -------------------- Filer status from Audit Analytics ------------------- */

** Submit job remotely;

rsubmit;
    data auditfiler;
        set audit.accfiler;
        if year(PE_DATE) > 2010;
    run;
endrsubmit;

** Download data to local;

rsubmit;
    proc download data=auditfiler out=STORAGE.auditfiler&todaysDate.;
    run;
endrsubmit;

/* --------------------------- MSF Data from CRSP --------------------------- */

** Submit job remotely;

rsubmit;
    data msfcrsp;
        set crsp.msf;
        if year(date)>2010;
    run;
endrsubmit;

** Download data to local;

rsubmit;
    proc download data=msfcrsp out=STORAGE.msfcrsp&todaysDate.;
    run;
endrsubmit;

/* --------------------- Analyst guidance data from IBES -------------------- */

** Submit job remotely;

rsubmit;
    data ibes_guidance;
        set ibes.det_guidance;
        year= year(anndats);
        month= month(anndats);
        year_month= year || month;
        if measure='EPS';
        count_guidance=1;
        if ticker=' ' then delete;
        if USfirm=1;
    run;
endrsubmit;

** Download data to local;

rsubmit;
    proc download data=ibes_guidance out=STORAGE.ibes_guidance&todaysDate.;
    run;
endrsubmit;

/* ------------------ Restatement data from Audit Analytics ----------------- */

** Submit job remotely;

rsubmit;
    data auditnonreli;
        set audit.auditnonreli;
    run;
endrsubmit;

** Download data to local;

rsubmit;
    proc download data=auditnonreli out=STORAGE.auditnonreli&todaysDate.;
    run;
endrsubmit;

/* ------------------------------ Snapshot data ----------------------------- */

** Submit job remotely;

rsubmit;
    data wrds_csq_pit (keep=gvkey datadate rdq FDATEQ PDATEQ UPDQ EPSPIY EPSFIY EPSPIQ EPSFIQ);
        set compsnap.wrds_csq_pit;
        where (indfmt='INDL') and (datafmt='STD') and (popsrc='D') and (consol='C') and (CURCDQ='USD');
        if year(datadate)>2015;
    run;
endrsubmit;

** Download data to local;

rsubmit;
    proc download data=wrds_csq_pit out=STORAGE.wrds_csq_pit&todaysDate.;
    run;
endrsubmit;

/* ---------------------- Institutional Ownership data ---------------------- */
** Note, requires WRDS macro;

** Submit job remotely;

%include "1_code\sas_macros\sas_macro_io.sas"; 

** Download data to local;

rsubmit;
    proc download data=IO_TimeSeries out=STORAGE.IO_TimeSeries&todaysDate.;
    run;
endrsubmit;
