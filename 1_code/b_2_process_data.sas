/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/*                            Download WRDS dataset                           */
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------- */
/*                                  Preamble                                  */
/* -------------------------------------------------------------------------- */

/* ----------------------------- User parameters ---------------------------- */

** IMPORTANT: Please change these parameters to match your setup;

%let PROJECT_FOLDER = "E:\Dropbox\Work\Research\COVID and EA Timing\covid-empirical";
%let freezeDate = 07212022;   

/* ----------------------------- File parameters ---------------------------- */

%let OUT_FOLDER = "%SYSFUNC(DEQUOTE(&PROJECT_FOLDER.))\2_pipeline\b_2_process_data";
%let DATA_FOLDER = "%SYSFUNC(DEQUOTE(&PROJECT_FOLDER.))\2_pipeline\b_1_download_wrds_data";

/* ------------------------ Change working directory ------------------------ */

  data _null_; 
      rc=dlgcdir(&PROJECT_FOLDER);
      put rc=;
   run;

/* ------------------------------ Set libraries ----------------------------- */

libname OUT &OUT_FOLDER;
libname DATA &DATA_FOLDER;

** Set freeze date to use for loading data;
/* ----------------- Set freeze date to use for loading data ---------------- */

%put WARNING: The data string used to load the data is: &freezeDate;

/* ---------------------------- Load macro files ---------------------------- */

%include "1_code\sas_macros\sas_macro_rollingreg.sas";
%include "1_code\sas_macros\sas_macro_wt.sas";

/* --------------------------- Define user macros --------------------------- */

** Macro to return mumber of observations of a dataset;

%macro num_obs(mydata);
    %let mydataID=%sysfunc(OPEN(&mydata.,IN));
    %let NOBS=%sysfunc(ATTRN(&mydataID,NOBS));
    %let RC=%sysfunc(CLOSE(&mydataID));
    &NOBS
%mend;

** Macro to verify the number of observations is the same;

%macro verify_same_size(left, right);
	** Count the number of obervations in each dataset;
    %let leftNOBS= %num_obs(&left.);
	%let rightNOBS= %num_obs(&right.);
	
	** Compare and log the results;
	%if &leftNOBS. = &rightNOBS. 
	%then %do; %put NOTE: Left and Right have the same number of observations (&leftNOBS); %end;
	%else %do; %put WARNING: Left (&leftNOBS) and Right (&rightNOBS) have a different number of observations!; %end;
%mend;

** Macro to test if a macro variable is empty;

%macro isBlank(param);
  %sysevalf(%superq(param)=,boolean)
%mend isBlank;

/* -------------------------------------------------------------------------- */
/*                            Load Python Data                                */
/* -------------------------------------------------------------------------- */

** IMPORTANT NOTE, IF .XLSX FILES ARE GENERATED STRAIGHT FROM PYTHON, FOLLOW THESE STEPS:;
** Step 1: open up each .xlsx file manually using Excel;
** Step 2: convert the columns with dates to "short date";
** Step 3: save the Excel sheet and overwrite the existing one; 

/* ----------------------------- Filing dataset ----------------------------- */

proc import datafile = '2_pipeline\a_1_generate_filing_dataset\out\full_17to21_df.xlsx'
    out = work.full_17to21_df
	dbms = xlsx replace;
run;

data work.full_17to21_df;
    set work.full_17to21_df;
    format AcceptDate filingDate reportdate date9.;
run;

/* ------------------ COVID Exemptions based on 8-K Filings ----------------- */

proc import datafile = '2_pipeline\a_2_identify_exemption_filers\out\exemptions_8k.xlsx'
    out = work.exemptions_8k_formatted 
    dbms = xlsx replace;
run;

/* --------------------------- SEC Deadline file ---------------------------- */

proc import datafile = '2_pipeline\a_3_download_filing_deadlines\out\deadline_df.xlsx'
    out = work.deadline_df 
    dbms = xlsx replace;
run;

/* ---------------------------- Auditor variables --------------------------- */

proc import datafile = '2_pipeline\a_5_calculate_auditor_variables\out\auditor_variables.xlsx'
    out = work.auditor_variables
    dbms = xlsx replace;
run;

/* -------------------- Calendar Rotation Bias estimates -------------------- */

proc import datafile = '2_pipeline\a_4_calculate_calendar_bias\out\calendar_bias_per_day.xlsx'
    out = work.calendar_bias_per_day
    dbms = xlsx replace;
run;

/* ------------------------- Textual characteristics ------------------------ */

proc import datafile = '2_pipeline\a_6c_calculate_text_metrics\out\text_statistics.xlsx'
    out = work.text_statistics
    dbms = xlsx replace;
run;

/* -------------------------------- SPAC data ------------------------------- */

proc import datafile = '0_data\external\spac_main.xlsx'
    out = work.spac_main 
    dbms = xlsx replace;
run;

/* -------------------------------------------------------------------------- */
/*                               Load WRDS Data                               */
/* -------------------------------------------------------------------------- */

/* -------------------------------- Compustat ------------------------------- */

data work.compustat;
	set DATA.compustat&freezeDate;
run;

/* ------------------------------ Zipcode data ------------------------------ */

data work.company;
	set DATA.company&freezeDate;
run;

/* ---------------------- SASHELP file for county name ---------------------- */

data work.sashelpzipcode;
	set DATA.sashelpzipcode&freezeDate;
run;

/* --------------------------- Industry SIC Codes --------------------------- */

data work.sic_codes;
	set DATA.sic_codes&freezeDate;
run;

/* --------------------- PERMNO from CRSP Linking Table --------------------- */

data work.ccmxpf_lnkhist;
	set DATA.ccmxpf_lnkhist&freezeDate;
run;

/* ------------------ IBES Tickers - Security Table method ------------------ */

data work.compustatibes;
	set DATA.compustatibes&freezeDate;
run;

/* ------------------- IBES Tickers - ICLINK Macro method ------------------- */

data work.ICLINK;
	set DATA.ICLINK&freezeDate;
run;

/* -------------------------------- CRSP Data ------------------------------- */

data work.crsp_d;
	set DATA.crsp_d&freezeDate;
run;

/* ------------------------------ Trading days ------------------------------ */

data work.tradingdays;
	set DATA.tradingdays&freezeDate;
run;

/* ------------------ Earnings announcement dates from IBES ----------------- */

data work.actuals;
	set DATA.actuals&freezeDate;
run;

/* --------------- Unexpected earnings from IBES Summary File --------------- */

data work.ibes_SUM_QTR;
	set DATA.ibes_SUM_QTR&freezeDate;
run;

/* ------------------------ Market returns from CSRP ------------------------ */

data work.crspdsi;
	set DATA.crspdsi&freezeDate;
run;

/* ---------------------------- Delisting returns --------------------------- */

data work.rvtemp;
	set DATA.rvtemp&freezeDate;
run;

/* ------------------------------ CSRP Monthly ------------------------------ */

data work.crsp_m;
	set DATA.crsp_m&freezeDate;
run;

/* --------- Obtain historical CIK from the GVKEY-CIK linking table --------- */

data work.ciklink;
	set DATA.ciklink&freezeDate;
run;

/* --------------------------- 10-Q/K filing dates -------------------------- */

data work.sec1;
	set DATA.sec1&freezeDate;
run;

/* --------------- Retrieve NT filings from WRDS SEC Analytics -------------- */

data work.sec1NT;
	set DATA.sec1NT&freezeDate;
run;

/* ----------- Retrieve 10-Q/K Amendments from WRDS SEC Analytics ----------- */

data work.sec1Am;
	set DATA.sec1Am&freezeDate;
run;

/* -------------------- Filer status from Audit Analytics ------------------- */

data work.auditfiler;
	set DATA.auditfiler&freezeDate;
run;

/* --------------------------- MSF Data from CRSP --------------------------- */

data work.msfcrsp;
	set DATA.msfcrsp&freezeDate;
run;

/* --------------------- Analyst guidance data from IBES -------------------- */

data work.ibes_guidance;
	set DATA.ibes_guidance&freezeDate;
run;

/* ------------------ Restatement data from Audit Analytics ----------------- */

data work.auditnonreli;
	set DATA.auditnonreli&freezeDate;
run;

/* ------------------------------ Snapshot data ----------------------------- */

data work.wrds_csq_pit;
	set DATA.wrds_csq_pit&freezeDate;
run;

/* ---------------------------------- IO data ------------------------------- */

data work.IO_TimeSeries;
	set DATA.IO_TimeSeries&freezeDate;
run;

/* -------------------------------------------------------------------------- */
/*                                Process data                                */
/* -------------------------------------------------------------------------- */

/* ---------------- Merge zipcode information with compustat ---------------- */

proc sql;
    create table compustat_state
    as select a.*, b.state, b.city, b.zipcode, b.county, b.zipcode_num
    from work.compustat a left join work.company b
    on a.gvkey=b.gvkey;
quit;

/* ----------------- Create state variable based on zipcodes ---------------- */


options nosource nonotes; ** Disable the log printing here because zip functions print errors when they can't convert, which is fine;
data compustat_state2 / NOLIST;
  set compustat_state;

  **Label observerations with known bad zipcodes;
  if zipcode eq . then bad_zip = 1;
  zipcode = trim(zipcode);
  if length(zipcode) ne 5 then bad_zip = 1;

  if bad_zip ne 1 then CityState = zipcity(zipcode);
  if bad_zip ne 1 then StateName = zipname(zipcode);
  if bad_zip ne 1 then State2 = zipstate(zipcode);
  if bad_zip ne 1 then FIPS = zipfips(zipcode);
  if bad_zip ne 1 then StateNameL = zipnamel(zipcode);
run;
options source notes;


/* -------------- Merge county name with main Compustat dataset -------------- */

proc sql;
    create table compustat_state3
    as select a.*,b.COUNTYNM
    from compustat_state2 a left join work.sashelpzipcode b
    on a.zipcode_num=b.ZIP
    and a.state2=b.STATECODE;
quit;

proc sort data=compustat_state3;
    by gvkey datadate;
run;

/* -------------------------- Create quarterly lags ------------------------- */
** For: total assets, datadate and share price;

proc sql;
  create table compustat_lags 
  as select a.*, b.atq as lagATQ, b. datadate as lagdatadate, b.PRCCQ as lagPRCCQ
  from compustat_state3 a left join work.compustat  b
  on a.gvkey=b.gvkey
  and ((a.fyearq=b.fyearq and a.fqtr=b.fqtr+1) OR (a.fyearq=b.fyearq+1 and a.fqtr=b.fqtr-3))
  and a.fyearq ne .
  and b.fyearq ne .
  and a.fqtr ne .
  and b.fqtr ne .
  order by gvkey, datadate;
quit;

/* ---------------------- Create seasonal lags (1-year) --------------------- */
** For: datadate, share price, total assets, sales and expenses;

proc sql;
  create table compustat_lags_1 
  as select a.*, b. datadate as lag4datadate, b.PRCCQ as lag4PRCCQ, b.atq as lag4atq, b.SALEQ as lag4SALEQ, 
  b.XOPRQ as lag4XOPRQ
  from compustat_lags a left join work.compustat  b
  on a.gvkey=b.gvkey
  and (a.fyearq=b.fyearq+1 and a.fqtr=b.fqtr)
  and a.fyearq ne .
  and b.fyearq ne .
  and a.fqtr ne .
  and b.fqtr ne .
  order by gvkey, datadate;
quit;

proc sort data=compustat_lags_1 nodupkey;
    by gvkey datadate;
run;

/* ---------------------- Create seasonal lags (2-year) --------------------- */
** For: datadate, share price, total assets, sales and expenses;

proc sql;
  create table compustat_lags_2 
  as select a.*, b. datadate as lag8datadate, b.PRCCQ as lag8PRCCQ, b.atq as lag8atq, b.SALEQ as lag8SALEQ, 
  b.XOPRQ as lag8XOPRQ
  from compustat_lags_1 a left join work.compustat  b
  on a.gvkey=b.gvkey
  and (a.fyearq=b.fyearq+2 and a.fqtr=b.fqtr)
  and a.fyearq ne .
  and b.fyearq ne .
  and a.fqtr ne .
  and b.fqtr ne .
  order by gvkey, datadate;
quit;

proc sort data=compustat_lags_2 nodupkey;
    by gvkey datadate;
run;

/* --------------------- Obtain one-quarter ahead leads --------------------- */
** For: total assets, datadate, and share price;

proc sql;
  create table compustat_lags_3 
  as select a.*, b.atq as leadATQ, b. datadate as leaddatadate, b.PRCCQ as leadPRCCQ
  from compustat_lags_2  a left join compustat  b
  on a.gvkey=b.gvkey
  and ((a.fyearq=b.fyearq and a.fqtr=b.fqtr-1) OR (a.fyearq=b.fyearq-1 and a.fqtr=b.fqtr+3))
  and a.fyearq ne .
  and b.fyearq ne .
  and a.fqtr ne .
  and b.fqtr ne .
  order by gvkey, datadate;
quit;

/* ------------------ Obtain seasonal (1-year ahead) leads ------------------ */
** For: total assets, datadate, and share price;

proc sql;
  create table compustat_lead 
  as select a.*, b.atq as lead4ATQ, b. datadate as lead4datadate, b.PRCCQ as lead4PRCCQ
  from compustat_lags_3  a left join work.compustat  b
  on a.gvkey=b.gvkey
   and (a.fyearq=b.fyearq-1 and a.fqtr=b.fqtr)
  and a.fyearq ne .
  and b.fyearq ne .
  and a.fqtr ne .
  and b.fqtr ne .
  order by gvkey, datadate;
quit;

/* ------------------------- Create date identifiers ------------------------ */

data compustat_date;
	set compustat_lead;
	Cal_year=year(datadate);
	Cal_month=month(datadate);
	if 1 le cal_month le 3 then calendar_qtr=MDY(3,31,cal_year);
	if 4 le cal_month le 6 then calendar_qtr=MDY(6,30,cal_year);
	if 7 le cal_month le 9 then calendar_qtr=MDY(9,30,cal_year);
	if 10 le cal_month le 12 then calendar_qtr=MDY(12,31,cal_year);
	format 	CALENDAR_QTR YYMMDDN8.;
run;

proc sort data=compustat_date nodupkey;
    by gvkey datadate;
run;

/* -- Create firm characteristic and special item variables from Compustat -- */

data compustat_date_a;
	set compustat_date;

	** Replace missing values with 0;
	array var_array TXDBQ DLCQ GDWLIPQ SPIQ WDPQ AQPQ GLPQ SETPQ DTEPQ RDIPQ SPIOPQ RCPQ;
	do over var_array;
		if var_array=. then var_array = 0;
	end;

	**Book-to-market, leverage,market-to-book, and size;
	if PRCCQ*CSHOQ ne 0 then BM=(CEQQ + TXDBQ)/(PRCCQ*CSHOQ);
	if BM<0 then Neg_BM=1; else Neg_BM=0;
	if BM<0 then BM=0;

	if CEQQ + TXDBQ ne 0 then MB=(PRCCQ*CSHOQ)/(CEQQ + TXDBQ);
	if ATQ ne 0 then DA= (DLCQ + DLTTQ)/ATQ;
	ME= (PRCCQ*CSHOQ);
	if ME > 0 then Log_ME= log(ME);

	**Loss firms based on IBQ- Income before extraordinary items;
	if IBQ<0 then Loss=1; else Loss=0;

	**Loss firms based on EPS- ;
	if EPSPXQ<0 then LossEPS=1; else LossEPS=0;

	**ROA;
	if lagATQ ne 0 then ROA= IBQ/lagATQ;

	**Create Q4 indicator variable**;
	if fqtr=4 then Q4_ind=1; else Q4_ind=0;
	if fqtr=. then  Q4_ind=.;

	**Signed Goodwill and absolute value of special items;
	if lag4atq ne 0 then GWImp_s=(-GDWLIPQ)/lag4atq;
	if lag4atq ne 0 then absSPIQ_s=abs(SPIQ)/lag4atq;

	**Signed Asset Write-offs;
	if lag4atq ne 0 then WD_s=(-WDPQ)/lag4atq;

	**Signed Restructuring Charges;
	if lag4atq ne 0 then RCPQ_s=(-RCPQ)/lag4atq;

	**Absolute Value of other special items;
	absOtherSPI=abs(AQPQ + GLPQ + SETPQ + DTEPQ + RDIPQ + SPIOPQ);
	if lag4atq ne 0 then absOtherSPI_s=absOtherSPI/lag4atq;

	**Indicator variable for Goodwill Impairments;
	if GDWLIPQ ne . and GDWLIPQ ne 0 then  GWImp_Ind=1; else GWImp_Ind=0;

	**Indicator variable for Asset Write-offs;
	if WDPQ ne . and WDPQ ne 0 then WDPQ_Ind=1; else WDPQ_Ind=0;

	**Indicator variable for Restructuring Charges;
	if RCPQ ne . and RCPQ ne 0 then RCPQ_Ind=1; else RCPQ_Ind=0;

	**Indicator variable for all other special items (AQPQ, GLPQ, SETPQ, DTEPQ, RDIPQ, SPIOPQ);
	if (AQPQ ne . and AQPQ ne 0) or (GLPQ ne . and GLPQ ne 0)
	or (SETPQ ne . and SETPQ ne 0) or (DTEPQ ne . and DTEPQ ne 0)
	or (RDIPQ ne . and RDIPQ ne 0) or (SPIOPQ ne . and SPIOPQ ne 0) then OtherSPI_Ind=1; else OtherSPI_Ind=0;

	**Indicator variable for total special items**;
	if abs(SPIQ) ne . and abs(SPIQ) ne 0 then absSPIQ_INd=1; else absSPIQ_INd=0;
RUN;

/* ---------------------------- Process SIC codes --------------------------- */

proc sql;
	create table sic_codes_a
	as select a.*, b.sic
	from sic_codes a left join work.company b
	on a.gvkey=b.gvkey;
quit;

data sic_codes_b;
	set sic_codes_a;
	if sich = . then sich = sic*1;
run;

data sic_codes_c;
	set sic_codes_b;
	sic2=int(sich/100);
	sic3=int(sich/10);
	Cal_year=year(datadate);
run;

proc sort data=sic_codes_c nodupkey;
	by gvkey Cal_year;
run;

/* ----------- Merge Industry codes to Compustat quarterly dataset ---------- */

proc sql;
	create table compustat_e
	as select a.*, b.SIC2, b.SIC3, b.sich
	from compustat_date_a a left join sic_codes_c b
	on a.gvkey=b.gvkey 
	and a.Cal_year=b.Cal_year;
quit;

proc sort data=compustat_e nodupkey;
	by gvkey datadate;
run;

/* ------------------------------- Add PERMNO ------------------------------- */

proc sql;
create table compustat_f as
  select a.*, b.lpermno as permno, b. lpermco as permco, b.linkprim
  from compustat_e a left join work.ccmxpf_lnkhist b
  on not missing(a.gvkey) and a.gvkey=b.gvkey 
     and b.LINKPRIM in ('P', 'C')
     and b.LINKTYPE in ('LU', 'LC')
	 and not missing(datadate)
     and (b.LINKDT<=a.datadate or missing(b.LINKDT))
     and (a.datadate<=b.LINKENDDT or missing(b.LINKENDDT));
quit;

/* ------------------ Delete observations without permnos. ------------------ */

data compustat_g;
	set compustat_f;
	if permno=. then delete;
run;

/* --------------------------- Process IBES ticker -------------------------- */

proc sql;
 create table compustat_h
  as select a.*, b.ibtic
  from compustat_g a left join
      (select distinct gvkey, ibtic from work.compustatibes
       where not missing(gvkey) and not missing(ibtic) and iid='01') b
  on a.gvkey=b.gvkey
  order by gvkey, datadate;
quit;

/* --- Update the obervations with no ticker match using the iclink table --- */

data noticker; 
    set compustat_h;
    where not missing(permno) and missing(ibtic);
    drop ibtic;
run;

proc sort data=work.iclink (where=(score in (0,1,2))) out=ibeslink;
    by permno ticker score;
run;
 
data ibeslink; 
    set ibeslink;
    by permno ticker; 
    if first.permno;
run;

proc sql;
    create table noticker1
    as select a.*, b.ticker as ibtic, b.ticker as iclink
    from noticker a left join ibeslink b
    on a.permno=b.permno
    order by gvkey, datadate;
quit;

data compustat_l;
    set compustat_h (where=(missing(permno) or not missing(ibtic))) noticker1;
    label ibtic='IBES Ticker';
run;

/* ------ Restrict sample to firms with share codes 10 and 11 from CRSP ----- */

proc sort data=work.crsp_d out=work.crsp_d_sorted nodupkey;
	by permno;
run;

proc sql; 
	create table compustat_l_2 as select
	a.*, b.shrcd
	from compustat_l as a left join work.crsp_d_sorted as b
	on a.permno eq b.permno
	order by gvkey, datadate;
quit;

proc sort data=compustat_l_2 nodupkey;
	by gvkey datadate;
run;

data compustat_l_3;
	set compustat_l_2;
	if shrcd=. then delete;
run;

/* -------------------------------------------------------------------------- */
/*                        Create file with Trading days                       */
/* -------------------------------------------------------------------------- */
** Period: 1926 to 2021;

%let start_date=01Jan1926;
%let end_date=31Dec2021;

data Calendardays;
	date="&start_date"d;
	do while (date<="&end_date"d);
	    output;
	    date=intnx('day', date, 1, 's');
	end;
	format date date9.;
run;

/* --------------------------- Obtain trading days -------------------------- */

proc sql; 
	create table tradingDays2 
	as select distinct a. caldt 
	from work.tradingdays as a; 
quit;

data Tradingdays2;
	set Tradingdays2;
	rename caldt=tradedate;
run;

data Tradingdays2;
	set Tradingdays2;
	label tradedate="tradedate";
run;

proc sort data=Tradingdays2 nodupkey;
	by descending tradedate;
run;

/* ------------- Add lag (plus) variables for different windows ------------- */

%macro add_lag_var(windows, label=);
	%do index = 1 %to %sysfunc(countw(&windows., %str( )));
	%let I =%scan(&windows, &index,%str( ));
		day_&label._&I = lag&I(tradedate);
		format day_&label._&I DATE9.;
		%put Variable created: day_&label._&I;
	%END;
%MEND;

%let lag_windows_plus = 1 2 3 4 5 6 7 8 9 10 12 15 30 60 75 90 120 125 150 180 250 500;

data Tradingdays3;
	set Tradingdays2;
	%add_lag_var(&lag_windows_plus., label=plus);
run;

/* ------------- Add lag (minus) variables for different windows ------------ */

proc sort data=Tradingdays3 nodupkey;
	by tradedate;
run;

%let lag_windows_minus = 1 2 3 4 5 6 7 8 9 10 11 12 15 30 51 60 75 90 120 125 150 180 250 500;

data Tradingdays4;
	set Tradingdays3;
	%add_lag_var(&lag_windows_minus., label=minus);
run;

/* -------------------------------------------------------------------------- */
/*               Combine trading with calendar days - START                   */
/* -------------------------------------------------------------------------- */

proc sql;
	create table Calendar_trading_days
	as select a.*, b.tradedate
	from Calendardays a left join Tradingdays4 b
	on a.date = b.tradedate;
quit;

proc sort data=Calendar_trading_days;
	by descending date;
run;

%macro add_var(windows);
	%do index = 1 %to %sysfunc(countw(&windows.,%str( )));
	%let I =%scan(&windows.,&index,%str( ));
		lead_&I._tradedate = lag&I(tradedate);
		format lead_&I._tradedate DATE9.;
		%put Variable created: lead_&I._tradedate;
	%END;
%MEND;

%let calendar_windows = 1 2 3 4 5 6 7 8 9 10 11 12 15;

data Calendar_trading_days2;
	set Calendar_trading_days;
	%add_var(&calendar_windows.);
run;

/* ---------------- Replace missing dates with lead variables --------------- */

proc sort data=Calendar_trading_days2;
	by  date;
run;

%macro add_var(windows);
	%do index = 1 %to %sysfunc(countw(&windows., %str( )));
		%let I =%scan(&windows., &index.,%str( ));
		if tradedate=. then tradedate=lead_&I._tradedate;
	%END;
%MEND;

%let cal_windows_lead = 1 2 3 4 5 6 7 8 9 10 11 12;  
data Calendar_trading_days3;
	set Calendar_trading_days2;
	%add_var(&cal_windows_lead.);
run;

/* ------------- Combine day_minus, day_plus, and lead variables ------------ */

proc sql;
	create table Calendar_trading_days4
	as select a.*, b.day_plus_500, b.day_plus_250, b. day_plus_125, b. day_plus_1, b. day_plus_2,
	b. day_plus_3, b. day_plus_4, b. day_plus_5, b. day_plus_6, b. day_plus_7, b. day_plus_8, b. day_plus_9, b. day_plus_10,
	b. day_plus_12, b.day_plus_15,
	b. day_plus_30, b. day_plus_60, b. day_plus_90, b. day_plus_75, b. day_plus_120, b. day_plus_150, b. day_plus_180,
	b.day_minus_500, b. day_minus_250, b. day_minus_125, b. day_minus_1, b. day_minus_2,
	b. day_minus_3, b. day_minus_4, b. day_minus_5, b. day_minus_6, b. day_minus_7, b. day_minus_8, b. day_minus_9, b. day_minus_10,
	b. day_minus_11,
	b. day_minus_12, b. day_minus_30,b. day_minus_51,
	b. day_minus_60 , b. day_minus_90 , b. day_minus_120 , b. day_minus_150, b. day_minus_180, b.day_minus_15
	from Calendar_trading_days3 a left join Tradingdays4 b
	on a.tradedate = b.tradedate;
quit;

data Calendar_trading_days5;
	set Calendar_trading_days4;
	drop lead_1_tradedate lead_2_tradedate lead_3_tradedate lead_4_tradedate lead_5_tradedate lead_6_tradedate lead_7_tradedate
	lead_8_tradedate lead_9_tradedate lead_10_tradedate lead_11_tradedate lead_12_tradedate;
run;

proc sort data=Calendar_trading_days5;
	by date;
run;

data work.Cal_trd_days_1926_2021;
	set Calendar_trading_days5;
run;

/* -------------------------------------------------------------------------- */
/*                  Combine trading with calendar days - END                  */
/* -------------------------------------------------------------------------- */

/* --------------- Bring earnings announcement dates from IBES -------------- */

proc sort data=work.actuals nodupkey;
	by ticker pends;
run;

/* ----- Bring IBES Actual EPS and IBES RDQ dates for the current period ---- */

proc sql;
  create table compu_ibes_act
  as select a.*, b.value as IBES_EPS, b.anndats as rdq_ibes
  from compustat_l_3 a left join work.actuals b
  on a.ibtic=b.ticker
  and a.ibtic ne ' '
  and b.ticker ne ' '
  and a.datadate = b.pends
  order by gvkey, datadate;
quit;

proc sort data=compu_ibes_act nodupkey;
	by gvkey datadate;
run;

/* ---------------------- Take earlier date of the two ---------------------- */
** Taking the earlier of the IBES or Compustat dates maximizes sample size;

data forlags_only;
	set compu_ibes_act;
	year_month=Cal_year || Cal_month;
	**Use one of the other**;
	rdq_orig=rdq;
	if rdq=. and rdq_IBES ne . then rdq=rdq_IBES;
	diff_IBQ=rdq-rdq_IBES;
	if diff_IBQ>0 then rdq=rdq_ibes;
	format rdq_orig date9.;
	if fqtr =. then delete;
	if rdq=. then delete;
	if APDEDATEQ=. then delete;
run;

/* ------------------------ Create EADelays Variables ----------------------- */

data compu_ibes_act2;
	set forlags_only;
	EADelay=rdq-APDEDATEQ;
	EADelay_dat=rdq-datadate;
	rdq_minus_90days= intnx('day',rdq,-90);
run;

/* ------------- Get Trading days around EA date to calculate UE ------------ */

proc sql;
  create table compu_ibes_act2wtrading
  as select a.*, b.tradedate as EA_0, b.day_plus_2 as EA_2, b.day_minus_2 as EA_minus_2, b.day_plus_1 as EA_1, 
  b.day_minus_1 as EA_minus_1
  from compu_ibes_act2 a left join work.Cal_trd_days_1926_2021 b
  on a.rdq=b.date
  and a.rdq ne .
  and b.date ne . 
  order by gvkey, datadate, rdq;
quit;

/* ----------------------- [-2,2 window] UE Consensus ----------------------- */

proc sql;
  create table compu_ibes_act2aprior
  as select a.*, b.statpers as EPS_consen_date_EA_m2_2, b.medest as EPS_consen_EA_m2_2, b.numest
  from compu_ibes_act2wtrading a left join work.ibes_sum_qtr b
  on a.ibtic=b.ticker
  and a.rdq gt b.statpers
  and b.statpers ge a.rdq_minus_90days
  and b.statpers le a.EA_minus_2
  and b.ticker ne ' '
  and a.ibtic ne ' '
  and a.rdq ne .
  and rdq_minus_90days ne .
  and EA_minus_2 ne .
  and b.statpers ne .
  order by gvkey, datadate;
quit;

/* ---------------------------- Sort observations --------------------------- */

proc sort data=compu_ibes_act2aprior;
	by gvkey datadate rdq descending EPS_consen_date_EA_m2_2;
run;

/* -- Only keep the first implied forecast before an earnings announcements - */

proc sort data=compu_ibes_act2aprior nodupkey;
	by gvkey datadate rdq;
run;

/* ----------------------- Define unexpected earnings ----------------------- */

data compu_ibes_act2aprior2;
	set compu_ibes_act2aprior;

	**UE;
	UE_m2_2= IBES_EPS- EPS_consen_EA_m2_2;
	UE_m2_2_s=(IBES_EPS- EPS_consen_EA_m2_2)/prccq;

	**Absolute Value UE**;
	absUE_m2_2=abs(UE_m2_2);
	absUE_m2_2_s=abs(UE_m2_2_s);

	**Unexpected Earnings x Absolute Unexpected Earnings;
	Nonlinear=UE_m2_2_s*absUE_m2_2_s;

	**# of Analyst Following;
	Lognumest=log(1+numest);
run;

/* ------------------- Generate lags for previous quarters ------------------ */
** Generating the 8 previous lags over the prior 8 quarters for unexpected earnings;

%macro create_lag_datasets(list=, ds_list=, verbose=0);
	%do index = 1 %to %sysfunc(countw(&list., %str( )));
		%let I = %scan(&list., &index., %str( ));
		%let source =%scan(&ds_list., &index., %str( ));
		proc sql;
			create table lag&I.
			as select a.*, b.UE_m2_2_s as lag&I.UE_m2_2_s
			from &source. a left join compu_ibes_act2aprior2 b
			on a.gvkey=b.gvkey
			%if &I. eq 1 %then %do; and ((a.fyearq=b.fyearq and a.fqtr=b.fqtr+1) OR (a.fyearq=b.fyearq+1 and a.fqtr=b.fqtr-3)) %end;
			%else %if &I. eq 2 %then %do; and ((a.fyearq=b.fyearq and a.fqtr=b.fqtr+2) OR (a.fyearq=b.fyearq+1 and a.fqtr=b.fqtr-2)) %end;
			%else %if &I. eq 3 %then %do; and ((a.fyearq=b.fyearq and a.fqtr=b.fqtr+3) OR (a.fyearq=b.fyearq+1 and a.fqtr=b.fqtr-1)) %end;
			%else %if &I. eq 4 %then %do; and (a.fyearq=b.fyearq+1 and a.fqtr=b.fqtr)  %end;
			%else %if &I. eq 5 %then %do; and ((a.fyearq=b.fyearq+1 and a.fqtr=b.fqtr+1) OR (a.fyearq=b.fyearq+2 and a.fqtr=b.fqtr-3)) %end;
			%else %if &I. eq 6 %then %do; and ((a.fyearq=b.fyearq+1 and a.fqtr=b.fqtr+2) OR (a.fyearq=b.fyearq+2 and a.fqtr=b.fqtr-2)) %end;
			%else %if &I. eq 7 %then %do; and ((a.fyearq=b.fyearq+1 and a.fqtr=b.fqtr+3) OR (a.fyearq=b.fyearq+2 and a.fqtr=b.fqtr-1)) %end;
			%else %if &I. eq 8 %then %do; and (a.fyearq=b.fyearq+2 and a.fqtr=b.fqtr)  %end;
			and a.fyearq ne .
			and b.fyearq ne .
			and a.fqtr ne .
			and b.fqtr ne .
			order by gvkey, datadate;
		quit;

		proc sort data=lag&I. nodupkey;
			by gvkey datadate;
		run;
		%if &verbose. > 0 %then %put WARNING: Dataset created: lag&I.;
		
	%END;
%MEND;

%let lags = 1 2 3 4 5 6 7 8;
%let datasets = compu_ibes_act2aprior2 lag1 lag2 lag3 lag4 lag5 lag6 lag7 lag8;
%create_lag_datasets(list=&lags., ds_list=&datasets., verbose=1);

/* --------------------- Calculate variance of the lags --------------------- */

data varianceofUE;
	set lag8;
	abslag1UE_m2_2_s=abs(lag1UE_m2_2_s);
	abslag2UE_m2_2_s=abs(lag2UE_m2_2_s);
	abslag3UE_m2_2_s=abs(lag3UE_m2_2_s);
	abslag4UE_m2_2_s=abs(lag4UE_m2_2_s);
	abslag5UE_m2_2_s=abs(lag5UE_m2_2_s);
	abslag6UE_m2_2_s=abs(lag6UE_m2_2_s);
	abslag7UE_m2_2_s=abs(lag7UE_m2_2_s);
	abslag8UE_m2_2_s=abs(lag8UE_m2_2_s);
	Predict = VAR(abslag1UE_m2_2_s,abslag2UE_m2_2_s, abslag3UE_m2_2_s, abslag4UE_m2_2_s, abslag5UE_m2_2_s,
	abslag6UE_m2_2_s, abslag7UE_m2_2_s, abslag8UE_m2_2_s); 
run;

/* --------------------------- Calculate Firm Beta -------------------------- */

data prep_beta (keep=gvkey datadate rdq permno rdq_minus_1yr EA_minus_2);
	set varianceofUE;
	rdq_minus_1yr=intnx('day',rdq,-365);
	if Cal_year<2022;
	if Cal_year=2021 and Cal_month>7 then delete;
	format rdq_minus_1yr date9.;
run;

proc sql; 
	create table beta as select
	a.gvkey,a.permno, a.datadate, a.rdq, b.ret, b.date format date9.
	from prep_beta as a left join work.crsp_d as b
	on a.permno eq b.permno
	and a.rdq_minus_1yr <= b.date <= a.EA_minus_2
	order by gvkey, datadate, date;
quit;

/* ----------------------- Bring in the market return ----------------------- */

proc sql; 
	create table beta2 as select
	a.*, b.vwretd
	from beta a left join work.crspdsi b
	on a.date = b.date;
quit;

data beta2a;
	set beta2;
	if ret=. then delete;
	if ret=.B or ret=.C or ret=.R or ret=.T or ret=.P then delete;
run;

proc sort data=beta2a;
	by gvkey datadate;
run;

proc reg data=beta2a outest=beta3_params tableout noprint rsquare;
	model ret= vwretd;
	by gvkey datadate;
	output out=beta3;
run;

DATA beta3_params2;
	SET beta3_params;
	IF _TYPE_='PARMS';
RUN;

/* ---------------- Merge beta variable back to main dataset ---------------- */

proc sql; 
	create table datasetwbeta as select
	a.*, b.vwretd as beta
	from varianceofUE as a left join beta3_params2 as b
	on a.gvkey eq b.gvkey
	and a.datadate eq b.datadate
	order by gvkey, datadate;
quit;

proc sort data=datasetwbeta nodupkey;
	by gvkey datadate;
run;

/* --------------------- Calculate Earnings Persistence --------------------- */

data persistence (keep=gvkey datadate rdq Cal_year fyearq fqtr EPSPXQ);
	set datasetwbeta;
run;

/* --------------- Calculate lags of the earnings persistence --------------- */

** First lag; 
proc sql;
  create table persistence2
  as select a.*, b. EPSPXQ as lagEPSPXQ
  from persistence a left join work.compustat b
  on a.gvkey=b.gvkey
  and ((a.fyearq=b.fyearq and a.fqtr=b.fqtr+1) OR (a.fyearq=b.fyearq+1 and a.fqtr=b.fqtr-3))
  and a.fyearq ne .
  and b.fyearq ne .
  and a.fqtr ne .
  and b.fqtr ne .
  order by gvkey, datadate;
quit;

** Fifth lag;
proc sql;
  create table persistence3
  as select a.*, b. EPSPXQ as lag5EPSPXQ
  from persistence2 a left join work.compustat b
  on a.gvkey=b.gvkey
  and ((a.fyearq=b.fyearq+1 and a.fqtr=b.fqtr+1) OR (a.fyearq=b.fyearq+2 and a.fqtr=b.fqtr-3))
  and a.fyearq ne .
  and b.fyearq ne .
  and a.fqtr ne .
  and b.fqtr ne .
  order by gvkey, datadate;
quit;

proc sort data=persistence3 nodupkey;
	by gvkey datadate;
run;

data persistence4;
	set persistence3;
	if lagEPSPXQ=. then delete;
	if lag5EPSPXQ=. then delete;
run;

/* ------------------------ Run a rolling regression ------------------------ */

%rollingreg(data=persistence4, out_ds=persistence5,
id=gvkey, date=datadate,
model_equation= lagEPSPXQ= lag5EPSPXQ,
start_date= , end_date=,
freq=quarter, s=1, n=8);

data persistence6;
	set persistence5;
	date2_minus_2months= intnx('month',date2,-2,'beginning');
	format date2_minus_2months date9. ;
run;

proc sql;
  create table datasetwpersist
  as select a.*, b. lag5EPSPXQ as persistence
   from datasetwbeta a left join persistence6 b
	on a.gvkey= b.gvkey
	and year(a.datadate)=year(b.date2)
	and b.date2_minus_2months<=a.datadate<=b.date2
	and regobs>2
  order by gvkey, datadate;
quit;

proc sort data=datasetwpersist nodupkey;
	by gvkey datadate;
run;

/* -------- Earnings Announcement Window Returns for ERC Regressions -------- */

data MARKET_TESTS2B;
	set datasetwpersist;
	Datadate_minus_1yr=intnx('month', datadate,-12,'beg');
	format  Datadate_minus_1yr date9.;
run;

/* -------------------------------------------------------------------------- */
/*                              Delisting Returns                             */
/* -------------------------------------------------------------------------- */

/* -------- Compute replacement values for missing delisting returns -------- */

proc univariate data=work.rvtemp noprint;
    var dlret;
    output out=rv mean=mean_dlret probt=mean_pvalue;
    by dlstcd;
run;

/* ------- require replacement values to be statistically significant ------- */

data rv;
    set rv;
    if mean_pvalue le 0.05 then rv = mean_dlret; 
    else rv = 0; 
    keep dlstcd rv;
run;

/* ------------- Merge replacement values with delisting returns ------------ */

proc sql;
    create table delist as
	select a.*, b.rv
	from rvtemp a left join rv b
	on a.dlstcd = b.dlstcd;
quit;

/* ----------------------- (-1,1) short window returns ---------------------- */

proc sql; 
	create table compret_m1_1 as select
	a.gvkey, a.permno, a.datadate, a.rdq, b.ret, (b.vol*100)/(b.shrout*1000) as turnover, b.date format date9.
	from MARKET_TESTS2B as a left join work.crsp_d as b
	on a.permno eq b.permno
	and a.EA_minus_1 <= b.date <= a.EA_1
	order by gvkey, datadate, date;
quit;

/* ---------------------------- Delisting returns --------------------------- */

proc sql; 
	create table compret_m1_1_delisting as select
	a.*, b.rv, b.dlret, b.date as dldate, b.dlstcd, b.dlpdt
	from  compret_m1_1 as a left join delist as b
	on a.permno eq b.permno
	and a.date eq b.date
	order by gvkey, datadate, date;
quit;

data compret_m1_1_delistingb;
    set compret_m1_1_delisting;
    ret_orig = ret;
    ** First, use replacement values where necessary;
    if not missing(dlstcd) and missing(dlret) then dlret=rv;
    
    ** Second, incorporate delistings into daily return measure;
    if not missing(dlstcd) and missing(ret) then ret=dlret;
    else if not missing(dlstcd) and not missing(ret) then ret=(1+ret)*(1+dlret)-1;
	ret_d=ret;
run;

/* -------------------------- 12-months to datadate ------------------------- */

proc sql; 
	create table compret_m12_dat as select
	a.gvkey, a.permno, a.datadate, a.rdq, b.ret, (b.vol*100)/(b.shrout*1000) as turnover, (b.vol*100) as mvol, 
	(b.shrout*1000) as mshrout,
	b.date format date9.
	from MARKET_TESTS2B as a left join work.crsp_m as b
	on a.permno eq b.permno
	and a.Datadate_minus_1yr <= b.date <= a.datadate
	order by gvkey, datadate, date;
quit;

/* ----------------------- Bring in the market return ----------------------- */

proc sql; 
	create table compret_m1_1ma as select
	a.*, b.vwretd, b.ewretd
	from compret_m1_1_delistingb a left join work.crspdsi b
	on a.date = b.date;
quit;

proc sort data=compret_m1_1ma;
	by gvkey date;
run;

* Calculate returns to merge to dataset;

* (m1,1);

proc sql; 
	create table adjret_m1_1 as select distinct
	gvkey, datadate, exp(sum(log(1+ret_orig)))-1 as annret,
	exp(sum(log(1+ret_d)))-1 as annret_d,
	exp(sum(log(1+ret_orig))) - exp(sum(log(1+vwretd))) as bhar,
	exp(sum(log(1+ret_d))) - exp(sum(log(1+vwretd))) as bhar_d,
	exp(sum(log(1+ret_d))) - exp(sum(log(1+ewretd))) as bhar_e,
	mean(turnover) as mturnover,
	count(ret_orig) as n, std(ret_d) as std_ret_d, var(ret_d) as var_ret_d
	from compret_m1_1ma 
	group by gvkey, datadate
	order by gvkey, datadate;
quit;

* 12-months to datadate;

proc sql; 
	create table adjret_m12_dat as select distinct
	gvkey, datadate, 
	mean(turnover) as mturnover,  mean(mvol)/mean(mshrout) as mturnoveravg,
	count(turnover) as n
	from compret_m12_dat 
	group by gvkey, datadate
	order by gvkey, datadate;
quit;

* Merge back to dataset;

* (m1,1);

proc sql; 
	create table MARKET_TESTS2C as select
	a.*, b.annret AS annret_m1_to_1 , b.bhar as bhar_m1_to_1, b.bhar_e as bhar_e_m1_to_1, b.mturnover as mturnover_m1_to_1,
	b.bhar_d as bhar_d_m1_to_1, b.annret_d AS annret_d_m1_to_1, b. std_ret_d as std_ret_d_m1_to_1,
 	b. var_ret_d as var_ret_d_m1_to_1, b.n as n_m1_to_1
	from MARKET_TESTS2B a left join adjret_m1_1 b
	on a.gvkey eq b.gvkey
	and a.datadate eq b.datadate
	and rdq ne .
	order by gvkey, datadate;
quit;

proc means data=MARKET_TESTS2C;
	var bhar_m1_to_1;
run;

* 12-months to datadate;

proc sql; 
	create table MARKET_TESTS2D as select
	a.*, b.mturnover as mturnover_m12_to_dat, b.mturnoveravg as mturnoveravg_m12_to_dat
	from MARKET_TESTS2C a left join adjret_m12_dat b
	on a.gvkey eq b.gvkey
	and a.datadate eq b.datadate
	and rdq ne .
	order by gvkey, datadate;
quit;

proc means data=MARKET_TESTS2D;
	var mturnover_m12_to_dat mturnoveravg_m12_to_dat;
run;

/* -------------------------------------------------------------------------- */
/*            Prepare Variables to Merge with SEC Analytics Suite;            */
/* -------------------------------------------------------------------------- */

/* - Create variables sorrounding 10 days around the fiscal period end date - */

data compu_ibes_act2a;
	set MARKET_TESTS2D;
	APDEDATEQm10=intnx('day',APDEDATEQ,-10);
	APDEDATEQp10=intnx('day',APDEDATEQ,10);
	format APDEDATEQm10 APDEDATEQp10 date9.;
run;

proc sort data=compu_ibes_act2a nodupkey;
	by gvkey datadate;
run;

/* -------------------------------------------------------------------------- */
/*                              Add hisotric CIKs                             */
/* -------------------------------------------------------------------------- */

/* Logic
There are cases where the linking table has multiple GVKEY - CIK combinations for a given time period.
To reflect this, we allow for an N amount of matches, for example, with N=5 we'd end up with 5 columns:

cik_historical_1
cik_historical_2
cik_historical_3
cik_historical_4
cik_historical_5

In a situation where a company observation has 3 plausible GVKEY-CIK combinations, we rank them by source, flag, and date and then the data would look like:

cik_historical_1    --> First preference CIK
cik_historical_2   --> Second preference CIK
cik_historical_3   --> Third preference CIK
cik_historical_4   --> Missing, because only 3 possible CIK
cik_historical_5   --> Missing, because only 3 possible CIK

We also create `cik_historical` which is the best possible CIK based on our ranking (i.e., `cik_historical == cik_historical_1` in this example). 

---

When we need to merge on CIK we go through the columns and match on all possible matches.
These then get collapsed into a single column for each variable we are joining in and collaps it to the "best" available value. 
*/

/* - Sort the sources of GVKEY-CIK matches by source number, flag, and date - */

proc sort data=work.ciklink out=ciklink2;
	by gvkey descending flag descending SOURCE2 descending LNDATE ;
run;

/* -------- Loop through top N combinations and generate CIK columns -------- */

%macro create_cik_columns(col_prefix=, starting_data=, output_dataset=, max_cols=10, tmp_data_prefix=_tmp_cik, verbose=0);
	%let prev_col_list = ;
	%let to_del_tracker = _tmp;
	%do index=1 %to &max_cols.;
	
		%let col_name = &col_prefix._&index.;
		%if &index. > 1 %then %do; 
			%let prev_i = %sysevalf(&index-1, integer); 
			%let prev_col_name = &col_prefix._&prev_i.;
			
			%if &verbose. > 0 %then %put WARNING: Prev col: &prev_col_name.;
		%end;
		
		%if &verbose. > 0 %then %put WARNING: &col_name; 

		proc sql;
			create table &tmp_data_prefix._&index. as
			  select a.*, b.cik as &col_name.
			  %if &index. = 1 %then %do; from &starting_data. a left join ciklink2 b %end;
			  %else %do; from &tmp_data_prefix._&prev_i. a left join ciklink2 b  %end;
			  on not missing(a.gvkey) and a.gvkey=b.gvkey 
				 and not missing(datadate)
			     and (b.FNDATE<=a.rdq or missing(b.FNDATE))
			     and (a.rdq<=b.LNDATE or missing(b.LNDATE))
				 %if &index. > 1 %then %do; 
				 	%do index = 1 %to %sysfunc(countw(&prev_col_list,%str( )));
						%let prev_col =%scan(&prev_col_list,&index,%str( ));
						and a.&prev_col. ne b.cik 
					%end;
				%end;
				 ;
		quit;
		
		** Sort and remove duplicates (important!);
		proc sort data= &tmp_data_prefix._&index. nodupkey;
			by gvkey datadate;
		run;
	
		%if &index. = 1 %then %do; %verify_same_size(&starting_data., &tmp_data_prefix._&index.); %end;
		%else %do; %verify_same_size(&tmp_data_prefix._&prev_i., &tmp_data_prefix._&index.);  %end;

		** Add data to the trackers;

		** Check if nothing adding anymore;
		proc sql;
			create table _tmp as 
			select * from &tmp_data_prefix._&index.
			where &col_name. ne '';
		quit;

		%let num_hits = %num_obs(_tmp);

		%if &num_hits. eq 0 %then %do; 
			** Remove the last column from the dataset;
			data &tmp_data_prefix._&index.(DROP = &col_name.);
				set &tmp_data_prefix._&index.;
			run;

			** End loop;
			%put WARNING: Ending the loop early, no more hits after &col_name.;
			%goto leave; 
		%end; %else %do; 
			%let prev_col_list = &prev_col_list &col_name.;
			%let to_del_tracker = &to_del_tracker &tmp_data_prefix._&index.;
		%end;

	%end;
	%leave:

	** Find best CIK;
	
	** Note, this goes backwards so lower priority variables are overwritten by higher priority;
	
	%do rev_index=%sysfunc(countw(&prev_col_list.,%str( ))) %to 1 %by -1;
		%let prev_col =%scan(&prev_col_list.,&rev_index,%str( ));

		%if &verbose. > 0 %then %put WARNING: Setting &col_prefix. to &prev_col. if exists.;
		
		
		data &tmp_data_prefix._&index.;
			set &tmp_data_prefix._&index.;
			if not(missing(&prev_col.)) then &col_prefix. = &prev_col.; 
		run;

	%end;

	** Assign final dataset;

	data &output_dataset.;
		set &tmp_data_prefix._&index.;
	run;

	** Remove tmp dataset;
	proc datasets library=Work nolist;
	    delete &to_del_tracker.;
	quit;
%mend;

%create_cik_columns(
	col_prefix = cik_historical, 
	starting_data = compu_ibes_act2a, 
	output_dataset = compu_ibes_with_cik,
	verbose = 1
);

/* -------------------------------------------------------------------------- */
/*                        Merge in data from databases                        */
/* -------------------------------------------------------------------------- */

/* -------------------------- Define macro function ------------------------- */

%macro merge_in_variables_on_cik(
	var_list =,
	var_dataset =,
	start_dataset =,
	var_prefix =,
	output_dataset =,
	custom_sql =,
	cik_prefix = cik_historical,
	cik_num = 5,
	tmp_table = _tmp,
	verbose = 0
);
	** Trackers;

	%let col_to_delete =;

	** Loop over every variable;
 	%do index = 1 %to %sysfunc(countw(&var_list,%str( )));
		%let var =%scan(&var_list,&index,%str( ));
		
		%if &verbose. > 0 %then %put WARNING: Variable: &var.;
		
		%if %isBlank(&var_prefix.) = 1 %then %do;
			%let full_var_prefix = &var.; 
		%end; %else %do; 
			%let full_var_prefix = &var._&var_prefix.; 
		%end;
			
		** Loop over CIK combinations;
		%do cik_i=1 %to &cik_num.;
			%let cik_var = &cik_prefix._&cik_i.;

			%let var_name_i = &full_var_prefix._&cik_i.; 
			%if &verbose. > 0 %then %put WARNING: Variable + CIK: &var_name_i.;
			
			proc sql undo_policy=none;
				create	table &tmp_table. as
				select	a.*, b.&var. as &var_name_i.

				%if &index. = 1 and &cik_i. = 1 %then %do;
					 from	&start_dataset. as a left join &var_dataset. as b
				%end; %else %do;
					from	&tmp_table. as a left join &var_dataset. as b
				%end;

				on		a.&cik_var. = b.cik
				%if %isBlank(&custom_sql.) = 0 %then %do;
					&custom_sql.
				%end;
				;
			quit;

			** Remove duplicates (important);

			proc sort data=&tmp_table. nodupkey;
				by gvkey datadate;
			run;

			** Add to tracker;
			%let col_to_delete = &col_to_delete. &var_name_i.;

		%end;

		** Reconcile the variable options into a single variable;
	
		** Note, this goes backwards so lower priority variables are overwritten by higher priority;
		
		%do rev_index=&cik_num. %to 1 %by -1;
			%if &verbose. > 0 %then %put WARNING: Setting &full_var_prefix. to &full_var_prefix._&rev_index. if exists.;

			data &tmp_table.;
				set &tmp_table.;
				if not(missing(&full_var_prefix._&rev_index.)) then &full_var_prefix. = &full_var_prefix._&rev_index.; 
				**format &var. DATE9.; **This requires a check because not all columns will be dates...;
			run;

		%end;
		
	%end;

	** Create final dataset;
	%if &verbose. > 0 %then %put WARNING: Deleting these columnms: &col_to_delete.;
	

	data &output_dataset.(DROP = &col_to_delete.);
			set &tmp_table.;
		run;
	
	** Delete TMP dataset;
	%if &verbose = 0 %then %do;
		proc datasets library=Work nolist;
		    delete &tmp_table.;
		quit;
	%end; 
%mend;



/* --------------- Merge in variables from primary SEC dataset -------------- */

%let sql_sec_wrds = 	and 	APDEDATEQm10<=(b.rdate)<= APDEDATEQp10
						and 	a.APDEDATEQ ne .
						and 	b.secadate ne . 
			    		and 	b.fdate ne . 
						and 	b.rdate ne .;

%merge_in_variables_on_cik(
	var_list = fdate fname form rdate secadate,
	var_dataset = sec1,
	start_dataset = compu_ibes_with_cik,
	output_dataset = compu_ibes_with_var,
	custom_sql = &sql_sec_wrds.,
	verbose = 1
);

/* ------------------- Merge in variables from NT dataset ------------------- */

%let sql_sec_wrds = 	and 	APDEDATEQm10<=(b.rdate)<= APDEDATEQp10
						and 	a.APDEDATEQ ne .
						and 	b.secadate ne . 
			    		and 	b.fdate ne . 
						and 	b.rdate ne .;

%merge_in_variables_on_cik(
	var_list = fdate form secadate,
	var_dataset = sec1NT,
	start_dataset = compu_ibes_with_var,
	var_prefix = NT,
	output_dataset = compu_ibes_with_var,
	custom_sql = &sql_sec_wrds.,
	verbose = 1
);

/* ------------- Merge in variables from the amendments dataset ------------- */

%let sql_sec_wrds = 	and 	APDEDATEQm10<=(b.rdate)<= APDEDATEQp10
						and 	a.APDEDATEQ ne .
						and 	b.secadate ne . 
			    		and 	b.fdate ne . 
						and 	b.rdate ne .;

%merge_in_variables_on_cik(
	var_list = fdate form secadate,
	var_dataset = sec1Am,
	start_dataset = compu_ibes_with_var,
	var_prefix = AM,
	output_dataset = compu_ibes_with_var,
	custom_sql = &sql_sec_wrds.,
	verbose = 1
);

/* ------- Merge in variables from the Python (SEC EDGAR API) dataset ------- */

%let python_sql = 	and 	APDEDATEQm10<=(b.reportDate)<= APDEDATEQp10
					and 	a.APDEDATEQ ne .
					and 	b.reportDate ne .;

%merge_in_variables_on_cik(
	var_list = filingDate AcceptDate form cik,
	var_dataset = full_17to21_df,
	start_dataset = compu_ibes_with_var,
	var_prefix = Python,
	custom_sql = &python_sql.,
	output_dataset = compu_ibes_with_var_python,
	verbose = 1
);

/* --------------------------- Calculate sec delay -------------------------- */

data compu_ibes_with_var_python;
	set compu_ibes_with_var_python;
	sec_delay = fdate - APDEDATEQ;
	sec_delay_Python = filingDate_Python - APDEDATEQ;
run;


/* ----------------- Reconcile WRDS data with EDGAR API data ---------------- */

data compu_ibes_with_var_rec;
	set compu_ibes_with_var_python;

	** Secadate;
	if secadate ne . then secadate_final=secadate;
	if secadate_final =. and AcceptDate_Python ne . then secadate_final = AcceptDate_Python;

	** Fdate & CIK;
	if fdate ne . then fdate_final=fdate;
	if fdate_final =. and filingDate_Python ne . then fdate_final = filingDate_Python;
	if fdate_final =. and filingDate_Python ne . then cik_historical = cik_Python; 

	format fdate_final secadate_final date9.;
 
	** Form;
	if form ne " " then form_final=form;
	if form_final =" " and form_Python ne " " then form_final=form_Python;

	** SEC_delay;
	if sec_delay ne . then sec_delay_final=sec_delay;
	if sec_delay_final =. and sec_delay_Python ne . then sec_delay_final=sec_delay_Python;

	** Fill CIK;
	if cik_historical =. and cik ne . then cik_historical = cik; 
run;

proc sort data=compu_ibes_with_var_rec nodupkey;
	by gvkey datadate;
run;

/* -------------------------------------------------------------------------- */
/*                     Merge in Audit Analytics variables                     */
/* -------------------------------------------------------------------------- */
* Primary objective: add filer status information to the data set.;

/* --------- Create 10 days variable relative to the SEC filing date -------- */

data compu_ibes_10k2;
	set compu_ibes_with_var_rec;
	fdatem10=intnx('day',fdate_final,-10);
	fdatep10=intnx('day',fdate_final,10);
	if 	fdate_final=. then fdatem10=.;
	if 	fdate_final=. then fdatep10=.;
	format fdatem10 fdatep10 date9.;
run;

proc sql;
	create table compu_ibes_10k3
	as select a.*, b. HST_IS_ACCEL_FILER, b.HST_IS_LARGE_ACCEL, b. HST_IS_SHELL_CO, 
	b. HST_IS_SMALL_REPORT, b.HST_IS_VOLUNTARY_FILER, b.FILE_DATE, b.company_fkey, b.PE_Date, b.FORM_FKEY
	from compu_ibes_10k2  a left join work.auditfiler b
	on not missing(b.company_fkey) and b.company_fkey=a.cik_historical
	and 	a.fdatem10<=(b.FILE_DATE)<= a.fdatep10
	and a.fdatem10 ne .
	and a.fdatep10 ne .
	and b. FILE_DATE ne .;
run;

proc sort data=compu_ibes_10k3 nodupkey;
	by gvkey datadate;
run;

/* ----------- Backfill missing AA data using previous year's data ---------- */
* Audit Analytics does not have full coverage for 2021, so we backfil those missing data with the assumption the filing status didn't change;

data compu_ibes_10k3a;
	set compu_ibes_10k3;

	**lag1**;
	if HST_IS_ACCEL_FILER=. then AuditAnalytics_Miss=1; else AuditAnalytics_Miss=0;
	lagHST_IS_ACCEL_FILER=lag(HST_IS_ACCEL_FILER);
	lagHST_IS_LARGE_ACCEL=lag(HST_IS_LARGE_ACCEL);
	lagHST_IS_SMALL_REPORT=lag(HST_IS_SMALL_REPORT);
	laggvkey=lag(gvkey);

	**lag2**;
	lag2HST_IS_ACCEL_FILER=lag2(HST_IS_ACCEL_FILER);
	lag2HST_IS_LARGE_ACCEL=lag2(HST_IS_LARGE_ACCEL);
	lag2HST_IS_SMALL_REPORT=lag2(HST_IS_SMALL_REPORT);
	lag2gvkey=lag2(gvkey);

	**lag3**;
	lag3HST_IS_ACCEL_FILER=lag3(HST_IS_ACCEL_FILER);
	lag3HST_IS_LARGE_ACCEL=lag3(HST_IS_LARGE_ACCEL);
	lag3HST_IS_SMALL_REPORT=lag3(HST_IS_SMALL_REPORT);
	lag3gvkey=lag3(gvkey);

	**lag4**;
	lag4HST_IS_ACCEL_FILER=lag4(HST_IS_ACCEL_FILER);
	lag4HST_IS_LARGE_ACCEL=lag4(HST_IS_LARGE_ACCEL);
	lag4HST_IS_SMALL_REPORT=lag4(HST_IS_SMALL_REPORT);
	lag4gvkey=lag4(gvkey);

	**Brings lags because database is not fully updated*;
	if HST_IS_ACCEL_FILER=. and gvkey=laggvkey then HST_IS_ACCEL_FILER=lagHST_IS_ACCEL_FILER;
	if HST_IS_LARGE_ACCEL=. and gvkey=laggvkey then HST_IS_LARGE_ACCEL=lagHST_IS_LARGE_ACCEL;
	if HST_IS_SMALL_REPORT=. and gvkey=laggvkey then HST_IS_SMALL_REPORT=lagHST_IS_SMALL_REPORT;
	if HST_IS_ACCEL_FILER=. and lagHST_IS_ACCEL_FILER=. and gvkey=lag2gvkey then HST_IS_ACCEL_FILER=lag2HST_IS_ACCEL_FILER;
	if HST_IS_LARGE_ACCEL=. and lagHST_IS_LARGE_ACCEL=. and gvkey=lag2gvkey then HST_IS_LARGE_ACCEL=lag2HST_IS_LARGE_ACCEL;
	if HST_IS_SMALL_REPORT=. and lagHST_IS_SMALL_REPORT=. and gvkey=lag2gvkey then HST_IS_SMALL_REPORT=lag2HST_IS_SMALL_REPORT;

	**if lag1 and lag2 missing**;
	if HST_IS_ACCEL_FILER=. and lagHST_IS_ACCEL_FILER=. and lag2HST_IS_ACCEL_FILER=. 
	and gvkey=lag3gvkey then HST_IS_ACCEL_FILER=lag3HST_IS_ACCEL_FILER;
	if HST_IS_LARGE_ACCEL=. and lagHST_IS_LARGE_ACCEL=. and lag2HST_IS_LARGE_ACCEL=. 
	and gvkey=lag3gvkey then HST_IS_LARGE_ACCEL=lag3HST_IS_LARGE_ACCEL;
	if HST_IS_SMALL_REPORT=. and lagHST_IS_SMALL_REPORT=. and lag2HST_IS_SMALL_REPORT=. 
	and gvkey=lag3gvkey then HST_IS_SMALL_REPORT=lag3HST_IS_SMALL_REPORT;

	**if lag1 and lag2 and lag3 and lag4 missing*;
	if HST_IS_ACCEL_FILER=. and lagHST_IS_ACCEL_FILER=. and lag2HST_IS_ACCEL_FILER=. and  lag3HST_IS_ACCEL_FILER=.
	and gvkey=lag4gvkey then HST_IS_ACCEL_FILER=lag4HST_IS_ACCEL_FILER;
	if HST_IS_LARGE_ACCEL=. and lagHST_IS_LARGE_ACCEL=. and lag2HST_IS_LARGE_ACCEL=. and  lag3HST_IS_LARGE_ACCEL=.
	and gvkey=lag4gvkey then HST_IS_LARGE_ACCEL=lag4HST_IS_LARGE_ACCEL;
	if HST_IS_SMALL_REPORT=. and lagHST_IS_SMALL_REPORT=. and lag2HST_IS_SMALL_REPORT=. and  lag3HST_IS_SMALL_REPORT=.
	and gvkey=lag4gvkey then HST_IS_SMALL_REPORT=lag4HST_IS_SMALL_REPORT;
run;

data compu_ibes_10k3aa;
	set compu_ibes_10k3a;
run;

/* ------------------- Create indicators for filer status ------------------- */
* Three indicators: Large_Accelerated vs. Accelerated vs. Non-Accelerated Filers;
* Indicates how the registrant identified their large accelerated filer status.;
* (1= Yes; *2 = Did not disclose; *0 = No);

data  compu_ibes_10k4;
	set compu_ibes_10k3aa;
	**Large Acc- Audit Analytics**;
	if HST_IS_LARGE_ACCEL=1 then Large_Acc=1; else Large_Acc=0;
	if HST_IS_LARGE_ACCEL=. then Large_Acc=.;

	**Acc Filer- Audit Analytics**;
	if HST_IS_ACCEL_FILER=1 then Acc=1; else Acc=0;
	if HST_IS_ACCEL_FILER=. then Acc=.;

	**NonAcc- Audit Analytics**;
	if Large_Acc=0 and Acc=0 then NonAcc=1; else NonAcc=0;
	if  Large_Acc=. or Acc=. then NonAcc=.;

	**Large Acc and Acc overlap in Audit Analytics**;

	**Acc Filer- Final**;
	if Large_Acc=0 and Acc=1 then Acc_f=1; else Acc_f=0;
	if Large_Acc=. or Acc=. then Acc_f=.;

	**Smaller Reporting Company**;
	if HST_IS_SMALL_REPORT=1 then SRC=1; else SRC=0;
	if HST_IS_SMALL_REPORT=. then SRC=.;
run;

/* -------------------------------------------------------------------------- */
/*                      Create filing deadline variables                      */
/* -------------------------------------------------------------------------- */
/*
    ** Form 10-K:
    Large Accelerated-60 days
    Accelerated- 75 days
    Non-Accelerated- 75 days
    Smaller Reporting Company- 90 days

    ** Form 10-Q:
    Large Accelerated-40 days
    Accelerated- 40 days
    Non-Accelerated- 45 days
    Smaller Reporting Company- 45 days
*/

data  compu_ibes_10k5;
	set compu_ibes_10k4;

	**Create SEC deadlines- Audit Analytics**;
	if (form_final="10-K" and Large_Acc=1) then sec_deadline=intnx('day',APDEDATEQ,+60);
	if (form_final="10-Q" and Large_Acc=1) then sec_deadline=intnx('day',APDEDATEQ,+40);
	if (form_final="10-K" and Acc_f=1) then sec_deadline=intnx('day',APDEDATEQ,+75);
	if (form_final="10-Q" and Acc_f=1) then sec_deadline=intnx('day',APDEDATEQ,+40);
	if (form_final="10-K" and NonAcc=1) then sec_deadline=intnx('day',APDEDATEQ,+90);
	if (form_final="10-Q" and NonAcc=1) then sec_deadline=intnx('day',APDEDATEQ,+45);
	format sec_deadline date9.;
run;

/* -------------------- Adjust SEC deadlines for weekends ------------------- */

data  compu_ibes_10k6;
	set  compu_ibes_10k5;

	**Analytics**;
	WeekDay=weekday(sec_deadline);
	if Weekday=6 then Friday=1; else Friday=0;
	if Weekday=1 then Sunday=1; else Sunday=0;
	if Weekday=7 then Saturday=1; else Saturday=0;
run;

data compu_ibes_10k7;
	set compu_ibes_10k6;

	**Audit Analytics***;
	sec_deadline1=sec_deadline;
	if Saturday=1 then sec_deadline1=intnx('day',sec_deadline,+2);
	if Sunday=1 then sec_deadline1=intnx('day',sec_deadline,+1);
	format sec_deadline1 date9.;
run;

/* --------- Manually set the SEC Holidays and adjust SEC deadlines --------- */
* Based on the SEC Calendar: https://www.sec.gov/edgar/filer-information/calendar;

data compu_ibes_10k8;
	set compu_ibes_10k7;
	**2020 Holidays**;
	Holiday1 = '01jan2020'd;
	Holiday2 = '20jan2020'd;
	Holiday3 = '17feb2020'd;
	Holiday4 = '25may2020'd;
	Holiday5 = '03jul2020'd; *Friday*;
	Holiday6 = '07sep2020'd;
	Holiday7 = '12oct2020'd;
	Holiday8 = '11nov2020'd;
	Holiday9 = '26nov2020'd;
	Holiday10 = '25dec2020'd; *Friday*;
	format Holiday1 Holiday2 Holiday3 Holiday4 Holiday5 Holiday6 Holiday7 Holiday8 Holiday9 Holiday10 date9.;
	**2019 Holidays**;
	Holiday11 = '01jan2019'd;
	Holiday12 = '21jan2019'd;
	Holiday13 = '18feb2019'd;
	Holiday14 = '27may2019'd;
	Holiday15 = '04jul2019'd;
	Holiday16 = '02nov2019'd;
	Holiday17 = '14oct2019'd;
	Holiday18 = '11nov2019'd;
	Holiday19 = '28nov2019'd;
	Holiday20 = '25dec2019'd;
	format Holiday11 Holiday12 Holiday13 Holiday14 Holiday15 Holiday16 Holiday17 Holiday18 Holiday19 Holiday20 date9.;
	**2021 Holidays**;
	Holiday21 = '01jan2021'd; *Friday*;
	Holiday22 = '18jan2021'd;
	Holiday23 = '15feb2021'd;
	Holiday24 = '31may2021'd;
	Holiday25 = '05jul2021'd;
	Holiday26 = '06sep2021'd;
	Holiday27 = '11oct2021'd;
	Holiday28 = '11nov2021'd;
	Holiday29 = '25nov2021'd;
	Holiday30 = '24dec2021'd; *Friday*;
	format Holiday21 Holiday22 Holiday23 Holiday24 Holiday25 Holiday26 Holiday27 Holiday28 Holiday29 Holiday30 date9.;
	**2018 Holidays**;
	Holiday31 = '01jan2018'd;
	Holiday32 = '15jan2018'd;
	Holiday33 = '19feb2018'd;
	Holiday34 = '28may2018'd;
	Holiday35 = '04jul2018'd;
	Holiday36 = '03sep2018'd;
	Holiday37 = '08oct2018'd;
	Holiday38 = '12nov2018'd;
	Holiday39 = '22nov2018'd;
	Holiday40 = '25dec2018'd;
	format Holiday31 Holiday32 Holiday33 Holiday34 Holiday35 Holiday36 Holiday37 Holiday38 Holiday39 Holiday40 date9.;
	**2017 Holidays**;
	Holiday41 = '02jan2017'd;
	Holiday42 = '16jan2017'd;
	Holiday43 = '20feb2017'd;
	Holiday44 = '29may2017'd;
	Holiday45 = '04jul2017'd;
	Holiday46 = '04sep2017'd;
	Holiday47 = '09oct2017'd;
	Holiday48 = '10nov2017'd; *Friday*;
	Holiday49 = '23nov2017'd;
	Holiday50 = '25dec2017'd;
	format Holiday41 Holiday42 Holiday43 Holiday44 Holiday45 Holiday46 Holiday47 Holiday48 Holiday49 Holiday50 date9.;
run;

**Adjust SEC deadline for SEC holidays;

data compu_ibes_10k9;
	set compu_ibes_10k8;
	sec_deadline_final=sec_deadline1;

    /* ---------------------------------- 2020 ---------------------------------- */

	* Adjust non-Friday 2020 holidays;
	if sec_deadline1=Holiday1 or sec_deadline1=Holiday2 or sec_deadline1=Holiday3 or sec_deadline1=Holiday4
	or sec_deadline1=Holiday6 or sec_deadline1=Holiday7 or sec_deadline1=Holiday8 or sec_deadline1=Holiday9 then 
	sec_deadline_final=intnx('day',sec_deadline1,+1);

	* Adjust Friday 2020 holidays;
	if sec_deadline1=Holiday5 then sec_deadline_final=intnx('day',sec_deadline1,+3);
	if sec_deadline1=Holiday10 then sec_deadline_final=intnx('day',sec_deadline1,+3);

    /* ---------------------------------- 2019 ---------------------------------- */

	* Adjust non-Friday 2019 holidays;
	if sec_deadline1=Holiday11 or sec_deadline1=Holiday12 or sec_deadline1=Holiday13 or sec_deadline1=Holiday14 or  sec_deadline1=Holiday15
	or sec_deadline1=Holiday16 or sec_deadline1=Holiday17 or sec_deadline1=Holiday18 or sec_deadline1=Holiday19 or  sec_deadline1=Holiday20 then 
	sec_deadline_final=intnx('day',sec_deadline1,+1);
	format sec_deadline_final date9.;

    /* ---------------------------------- 2021 ---------------------------------- */

	* Adjust non-Friday 2021 holidays;
	if sec_deadline1=Holiday22 or sec_deadline1=Holiday23 or sec_deadline1=Holiday24 or sec_deadline1=Holiday25
	or sec_deadline1=Holiday26 or sec_deadline1=Holiday27 or sec_deadline1=Holiday28 or sec_deadline1=Holiday29 then 
	sec_deadline_final=intnx('day',sec_deadline1,+1);

	* Adjust Friday 2021 holidays;
	if sec_deadline1=Holiday21 then sec_deadline_final=intnx('day',sec_deadline1,+3);
	if sec_deadline1=Holiday30 then sec_deadline_final=intnx('day',sec_deadline1,+3);

    /* ---------------------------------- 2018 ---------------------------------- */

	* Adjust non-Friday 2021 holidays;
	if sec_deadline1=Holiday31 or sec_deadline1=Holiday32 or sec_deadline1=Holiday33 or sec_deadline1=Holiday34 or sec_deadline1=Holiday35
	or sec_deadline1=Holiday36 or sec_deadline1=Holiday37 or sec_deadline1=Holiday38 or sec_deadline1=Holiday39 or sec_deadline1=Holiday40 then 
	sec_deadline_final=intnx('day',sec_deadline1,+1);

    /* ---------------------------------- 2017 ---------------------------------- */

	* Adjust non-Friday 2017 holidays;
	if sec_deadline1=Holiday41 or sec_deadline1=Holiday42 or sec_deadline1=Holiday43 or sec_deadline1=Holiday44 or sec_deadline1=Holiday45
	or sec_deadline1=Holiday46 or sec_deadline1=Holiday47 or sec_deadline1=Holiday49 or sec_deadline1=Holiday50 then 
	sec_deadline_final=intnx('day',sec_deadline1,+1);

	* Adjust Friday 2017 holidays;
	if sec_deadline1=Holiday48 then sec_deadline_final=intnx('day',sec_deadline1,+3);
run;

data compu_ibes_10k10;
	set compu_ibes_10k9;
run;

/* -------------------------------------------------------------------------- */
/*                          Add COVID 8-K Exemptions.                         */
/* -------------------------------------------------------------------------- */

proc sort data=exemptions_8k_formatted nodupkey;
	by cik;
run;

data exemptions_8k_formatted2;
	set exemptions_8k_formatted;
	COVID_8K=1;
run;

proc sql; 
	create table combined_wcontrols_8k
		as select distinct a.*, b.COVID_8K, b.file_date as fdate_COVID_8K, b.period_ending as period_ending_COVID_8K
		from compu_ibes_10k10 a left join exemptions_8k_formatted2 b
		on a.cik_historical=b.cik;
quit;

/* ------------------------- Add indicator variables ------------------------ */

data combined_wcontrols_8ka;
	set combined_wcontrols_8k;
	/* 
        COVID Extension indicator variable:

        Defined as indicator variable equal to one if COVID 8-K exemption
        is between fiscal period end date and 10 days after SEC deadline
    */

	if COVID_8K=. then COVID_8K=0;
	if APDEDATEQ<=fdate_COVID_8K<=sec_deadline_final+10 then COVID_Extension2=1; else COVID_Extension2=0;
	if APDEDATEQ<=period_ending_COVID_8K<=sec_deadline_final+10 then COVID_Extension2=1;

	* If filing date is after July 1st then it's likely an error so we code as zero; 
	COVID8K_Deadline = '01jul2020'd;
	if fdate_COVID_8K>COVID8K_Deadline then COVID_Extension2=0;
run;

proc means data=combined_wcontrols_8ka n mean;
	var COVID_Extension2 COVID_8K;
run;

/* ------- Adjust SEC deadline for NT Notices and COVID 8-K Exemptions ------ */

data combined_wcontrols_8k2;
	set combined_wcontrols_8ka;
	sec_deadline_final2=sec_deadline_final;

	**Adjust deadlines for NT Extensions**;
	if form_NT = "NT 10-Q" then sec_deadline_final2=sec_deadline_final+5;
	if form_NT = "NT 10-K" then sec_deadline_final2=sec_deadline_final+15;

	**Adjust deadline for COVID Extensions**;
	if COVID_Extension2=1 then sec_deadline_final2=sec_deadline_final+45;
	format sec_deadline_final2 date9.;
run;

/* ----------------------- Create Late Filer Variables ---------------------- */

data combined_wcontrols_8k5;
	set combined_wcontrols_8k2;

	**Late Filer NT;
	if fdate_final>sec_deadline_final and fdate_final<=sec_deadline_final2 and form_NT ne " " 
	and COVID_Extension2 ne 1 then LateFilerNT=1;else LateFilerNT=0;
	if sec_deadline_final2=. then LateFilerNT=.;

	**Late Filer COVID**;
	if fdate_final>sec_deadline_final and fdate_final<=sec_deadline_final2 and COVID_Extension2=1 
	then LateFilerCOVID=1; else LateFilerCOVID=0;
	if sec_deadline_final2=. then LateFilerCOVID=.;

	**Late Filer No Excuse**;
	if fdate_final>sec_deadline_final2 then LateFiler=1;else Latefiler=0;
	if sec_deadline_final2=. then LateFiler=.;

	**Timely Filer**;
	if fdate_final<=sec_deadline_final2 and LateFiler=0 and LateFilerNT=0 and LateFilerCOVID=0 then 
	TimelyFiler=1; else TimelyFiler=0;
	if sec_deadline_final2=. then TimelyFiler=.;
run;

/* ------------- Verify the validyt of the SEC deadline variable ------------ */
* We verify our manual deadline with the deadline retrieved from an external website;
* See `a_3_download_filing_deadlines.ipynb` for details;

data deadline_df2;
	set deadline_df;
	periodEnd2= input(periodEnd,mmddyy10.);
	deadline2=input(deadline,mmddyy10.);
	format periodEnd2 deadline2 date9.;
run;

* Large Accelerated Filer;
data LAF;
	set deadline_df2;
	if filerType="laf";
run;

* Accelerated Filer;
data AF;
	set deadline_df2;
	if filerType="af";
run;

* Non-Accelerated Filer;
data nAF;
	set deadline_df2;
	if filerType="naf";
run;

* Merge LAF;
proc sql;
  create table data_2020_Python3
  as select a.*, b.deadline2 as sec_deadline_Python_LAF
  from combined_wcontrols_8k5 a left join LAF b
  on a.APDEDATEQ=b.periodEnd2
  and a.Large_Acc=1 
  and a.form_final=b.formType
  order by gvkey, datadate;
quit;

proc sort data=data_2020_Python3 nodupkey;
	by gvkey datadate;
run;

* Merge LAF;
proc sql;
  create table data_2020_Python4
  as select a.*, b.deadline2 as sec_deadline_Python_AF
  from data_2020_Python3 a left join AF b
  on a.APDEDATEQ=b.periodEnd2
  and a.Acc_f=1 
  and a.form_final=b.formType
  order by gvkey, datadate;
quit;

proc sort data=data_2020_Python4 nodupkey;
	by gvkey datadate;
run;

* Merge NAF;

proc sql;
	create table data_2020_Python5
	as select a.*, b.deadline2 as sec_deadline_Python_NAF
	from data_2020_Python4 a left join NAF b
	on a.APDEDATEQ=b.periodEnd2
	and a. NonAcc=1 
	and a.form_final=b.formType
	order by gvkey, datadate;
quit;

proc sort data=data_2020_Python5 nodupkey;
	by gvkey datadate;
run;

data data_2020_Python6;
	set data_2020_Python5;
	sec_deadline_Python=sec_deadline_Python_LAF;
	if sec_deadline_Python_AF ne . then sec_deadline_Python=sec_deadline_Python_AF;
	if sec_deadline_Python_NAF ne . then sec_deadline_Python=sec_deadline_Python_NAF;
	format sec_deadline_Python date9.;
	format secadate_final date9.;
run;

proc print data=data_2020_Python6 (obs=100);
	var gvkey datadate sec_deadline_final sec_deadline_Python NonAcc Acc_f Large_Acc sec_deadline_Python_AF
	sec_deadline_Python_LAF;
run;

data check (keep=gvkey datadate APDEDATEQ rdq sec_deadline_final sec_deadline_Python);
	set data_2020_Python6;
	if sec_deadline_final ne sec_deadline_Python;
	if year(APDEDATEQ)>2017 and year(APDEDATEQ)<2022;
run;

/* -------------------------------------------------------------------------- */
/*                 Define concurrent vs. non-concurrent filers                */
/* -------------------------------------------------------------------------- */
* Following Arif et al. 2019;

data data_2020_Python7;
	set data_2020_Python6;
	if (fdate_final-1)<=rdq<=(fdate_final) then Concurrent=1; else Concurrent=0;
	if fdate_final=. then Concurrent=.;
	if rdq=. then Concurrent=.;
run;

proc means data=data_2020_Python7 n mean;
	var Concurrent;
	class Cal_year;
run;

data data_lte_2020;
	set data_2020_Python7;
run;

/* -------------------------------------------------------------------------- */
/*                       Bring in audit office variables                      */
/* -------------------------------------------------------------------------- */
* Calculated in file `a_5_calculate_auditor_variables.ipynb` for years 2018, 2019, 2020;

proc sort data=auditor_variables;
	by company_fkey fiscal_year;
run;

proc sql;
	create table data_lte_2020_audit
	as select a.*, b.choi_ofsize1, b.choi_ofsize2, b.choi_large_office_by_fees, b.choi_large_office_by_clients,
	b.beck_large_office, b.beck_ln_office_size, b.new_auditor_office,
	b.ege_ln_office_size, b.ln_audit_fees, b.total_audit_fees, b.num_clients, b.is_big_4, 
	b.auditor_tenure, b.new_auditor
	from data_lte_2020 a left join auditor_variables b
	on a.cik_historical=b.company_fkey
	and a.fyearq=fiscal_year
	and not missing(b.company_fkey)
	and not missing(a.cik_historical)
	order by gvkey, datadate;
quit;

proc sort data=data_lte_2020_audit nodupkey;
	by gvkey datadate;
run;

/* -------------------------------------------------------------------------- */
/*                      Calculate discretionary accruals                      */
/* -------------------------------------------------------------------------- */

/* ---------------- Set up the data with compustat as a start --------------- */

data dis_acc_qtrly;
	set work.compustat;
	if atq ne . ;
	if atq > 10;
	if datafqtr ne "";
run;

data dis_acc_qtrlyb;
	set dis_acc_qtrly;
	length fquarter 8.;
	length fyear 8.;
	fquarter=substr(DATAFQTR,6,1);
	fyear=substr(DATAFQTR,1,4);
	quarter_counter = (fyear-1991)*4+fquarter;
	if RECCHY*INVCHY*APALCHY*TXACHY ne . and AOLOCHY = . then AOLOCHY = 0;
	if AOLOCHY ne . and RECCHY = . then RECCHY = 0;
	if AOLOCHY ne . and INVCHY = . then INVCHY = 0;
	if AOLOCHY ne . and APALCHY = . then APALCHY = 0;
	if AOLOCHY ne . and TXACHY = . then TXACHY = 0;
	if RECCHY = . AND INVCHY = . AND APALCHY = . and TXACHY = . and AOLOCHY = . then delete;
run;

proc sql;
create table dis_acc_qtrly2
	as select a.*, b.fquarter as lag_fquarter,
	b.RECCHY as lag_RECCHY, b.INVCHY as lag_INVCHY,
	b.APALCHY as lag_APALCHY, b.TXACHY as lag_TXACHY, b.AOLOCHY as lag_AOLOCHY
	from dis_acc_qtrlyb a left join dis_acc_qtrlyb b
	on a.gvkey=b.gvkey and a.quarter_counter=b.quarter_counter+1;
quit;

proc sort data=dis_acc_qtrly2 nodupkey;
	by gvkey datafqtr;
run;

data dis_acc_qtrly2b;
	set dis_acc_qtrly2;
	if fquarter = 1 then lag_RECCHY = 0;
	if fquarter = 1 then lag_INVCHY = 0;
	if fquarter = 1 then lag_APALCHY= 0;
	if fquarter = 1 then lag_TXACHY = 0;
	if fquarter = 1 then lag_RECCHY = 0;
	if fquarter = 1 then Q1 = 1;
	else Q1 = 0;
	if fquarter = 2 then Q2 = 1;
	else Q2 = 0;
	if fquarter = 3 then Q3 = 1;
	else Q3 = 0;
	if fquarter = 4 then Q4 = 1;
	else Q4 = 0;
run;

data dis_acc_qtrly3;
	set dis_acc_qtrly2b;
	CHGAR = RECCHY - lag_RECCHY;
	CHGINV = INVCHY - lag_INVCHY;
	CHGAP = APALCHY - lag_APALCHY;
	CHGTAX = TXACHY - lag_TXACHY;
	CHGOTH = AOLOCHY - lag_AOLOCHY;
	quarter_counter = (fyear-1991)*4+fquarter;
run;

data dis_acc_qtrly4;
	set dis_acc_qtrly3;
	acc = -1*(CHGAR+CHGINV+CHGAP+CHGTAX+CHGOTH);
run;

proc sql;
create table dis_acc_qtrly4b
	as select a.*, b.acc as lag_acc_tm4
	from dis_acc_qtrly4 a left join dis_acc_qtrly4 b
	on a.gvkey=b.gvkey 
	and a.fquarter=b.fquarter 
	and a.fyear=b.fyear+1;
quit;

/* ---------------------------- Get the SIC codes --------------------------- */

data sic_codes;
	set work.sic_codes;
	year=year(datadate);
run;

proc sql;
	create table sic_codes_a
	as select a.*, b.sic
	from sic_codes a left join work.company b
	on a.gvkey=b.gvkey;
quit;

data sic_codes_b;
	set sic_codes_a;
	if sich = . then sich = sic*1;
run;

data sic_codes_c;
	set sic_codes_b;
	sic2=int(sich/100);
	year=year(datadate);
run;

proc sort data=sic_codes_c nodupkey;
	by gvkey year;
run;

proc sql;
create table dis_acc_qtrly5
	as select a.*, SIC2
	from dis_acc_qtrly4b a left join sic_codes_c b
	on a.gvkey=b.gvkey 
	and a.fyear=b.year;
quit;

proc sort data=dis_acc_qtrly5 nodupkey;
	by gvkey datadate;
run;

proc sql;
create table dis_acc_qtrly5b
	as select a.*, b.saleq as lag_saleq, a.saleq - b.saleq as delta_sales,
	b.ATQ as lag_ATQ
	from dis_acc_qtrly5 a left join dis_acc_qtrly5 b
	on a.gvkey=b.gvkey 
	and a.quarter_counter=b.quarter_counter+1;
quit;

proc sort data=dis_acc_qtrly5b nodupkey;
	by gvkey datadate;
run;

proc sql;
create table dis_acc_qtrly5c
	as select a.*, (a.saleq - b.saleq)/ b.saleq as SA_SG
	from dis_acc_qtrly5b a left join dis_acc_qtrly5b b
	on a.gvkey=b.gvkey 
	and a.quarter_counter=b.quarter_counter+4;
quit;

proc sort data=dis_acc_qtrly5c nodupkey;
	by gvkey datadate;
run;

/* ------------------- Link in Market-to-book using PERMNO ------------------ */

proc sql;
	create table dis_acc_qtrly5c2 as
	  select a.*, b.lpermno as permno, b. lpermco as permco, b.linkprim
	  from dis_acc_qtrly5c a left join work.ccmxpf_lnkhist b
	  on not missing(a.gvkey) and a.gvkey=b.gvkey 
	     and b.LINKPRIM in ('P', 'C')
	     and b.LINKTYPE in ('LU', 'LC')
		 and not missing(datadate)
	     and (b.LINKDT<=a.datadate or missing(b.LINKDT))
	     and (a.datadate<=b.LINKENDDT or missing(b.LINKENDDT));
quit;

/* ------------------- Delete observations without permnos ------------------ */
* PERMNO is required to link to CRSP;

data dis_acc_qtrly6;
	set dis_acc_qtrly5c2;
	if permno=. then delete;
run;

/* ------------------- Get and download MSF data from CRSP ------------------ */

proc sql;
    create table dis_acc_qtrly6a as
    select a.*, PRC, SHROUT
    from dis_acc_qtrly6 a left join work.msfcrsp b
    on a.permno=b.permno 
	and month(a.datadate) = month(b.date)
	and year (a.datadate) = year(b.date)
	and a.permno ne .
	and b.permno ne .;
quit;

proc print data=dis_acc_qtrly6a (obs=10);
	var gvkey datadate rdq permno PRC SHROUT;
run;

proc sort data=dis_acc_qtrly6a nodupkey;
	by gvkey datadate;
run;

data dis_acc_qtrly6b;
	set dis_acc_qtrly6a;
	MV = abs(PRC*SHROUT);
	lag_MV = abs(lag(PRC))*lag(SHROUT);
	if MV=. then delete;
	roaq = niq / lag_atq;
	gvkey_date = gvkey||datadate;
run;

proc sort data=dis_acc_qtrly6b nodup;
	by gvkey datadate;
run;

data dis_acc_qtrly6c;
	set dis_acc_qtrly6b;
	if gvkey_date = lag(gvkey_date) then MV = MV + lag_MV;
	if CEQQ ne 0 then mb = MV / CEQQ;
	count + 1;
	if roaq * mb * SA_SG = . then delete;
run;

proc sort data=dis_acc_qtrly6c;
	by descending count;
run;

data dis_acc_qtrly6d;
	set dis_acc_qtrly6c;
	if gvkey_date = lag(gvkey_date) then delete;
run;

data dis_acc_qtrly7;
	set dis_acc_qtrly6d;
	roaq_rank = roaq;
	mb_rank = mb;
	SA_SG_rank = SA_SG;
	if sic2 = . then delete;
	acc = acc/ lag_atq;
	lag_acc_tm4 = lag_acc_tm4 / lag_atq;
	delta_sales = (delta_sales-CHGAR) / lag_atq;
	if acc * lag_acc_tm4 * delta_sales = . then delete;
	Cal_year=year(datadate);
run;

proc sort data=dis_acc_qtrly7;
	by Cal_year;
run;

proc rank data=dis_acc_qtrly7 groups=5 out=dis_acc_qtrly8;
	by Cal_year;
	var roaq_rank mb_rank SA_SG_rank;
run;

proc sort data=dis_acc_qtrly8 nodupkey;
	by gvkey datadate;
run;

proc sort data=dis_acc_qtrly8;
	by gvkey datadate;
run;

data dis_acc_qtrly9;
	set dis_acc_qtrly8 (keep = gvkey datadate sic2 fyear acc lag_acc_tm4
	delta_sales CHGAR lag_atq roaq_rank mb_rank SA_SG_rank q1 q2 q3 q4);
	if roaq_rank = 0 then roaq1 = 1;
	else roaq1 = 0;
	if roaq_rank = 1 then roaq2 = 1;
	else roaq2 = 0;
	if roaq_rank = 3 then roaq4 = 1;
	else roaq4 = 0;
	if roaq_rank = 4 then roaq5 = 1;
	else roaq5 = 0;

	if mb_rank = 0 then mbq1 = 1;
	else mbq1 = 0;
	if mb_rank = 1 then mbq2 = 1;
	else mbq2 = 0;
	if mb_rank = 3 then mbq4 = 1;
	else mbq4 = 0;
	if mb_rank = 4 then mbq5 = 1;
	else mbq5 = 0;

	if SA_SG_rank = 0 then sgq1 = 1;
	else sgq1 = 0;
	if SA_SG_rank = 1 then sgq2 = 1;
	else sgq2 = 0;
	if SA_SG_rank = 3 then sgq4 = 1;
	else sgq4 = 0;
	if SA_SG_rank = 4 then sgq5 = 1;
	else sgq5 = 0;
	sic2year = sic2||fyear;
run;

/* -------------------------------- Winsorize ------------------------------- */

proc sort data = dis_acc_qtrly9;
	by fyear;
run;

%WT(data = dis_acc_qtrly9, out = dis_acc_qtrly10, byvar = fyear, vars
= acc lag_acc_tm4 delta_sales, type = W, pctl = 1 99)

proc sort data = dis_acc_qtrly10;
	by sic2year;
run;

/* ------------------ Get the residual from the regression ------------------ */

proc reg data = dis_acc_qtrly10 noprint;
	by sic2year;
	model acc = q1 q2 q3 q4 delta_sales lag_acc_tm4 sgq1 sgq2 sgq4 sgq5
	mbq1 mbq2 mbq4 mbq5 roaq1 roaq2 roaq4 roaq5;
	output out=dis_acc_qtrly11 r=disc_acc;
run;

data work.dis_acc_qtrly11;
	set dis_acc_qtrly11;
run;

/* ----------------- Merge discretionary accruals to sample ----------------- */

proc sql;
  create table datawAudit2
  as select a.*, b. disc_acc
  from data_LTE_2020_Audit a left join work.dis_acc_qtrly11 b
  on a.gvkey=b.gvkey
  and a.datadate=b.datadate
  order by gvkey, datadate, rdq;
quit;

/* -------------------------------------------------------------------------- */
/*                        Calculate guidance variables                        */
/* -------------------------------------------------------------------------- */

data Guidance;
	set datawAudit2;
run;

proc sql;
  create table Guidance2
  as select a.*, b. tradedate as EA_0, b. day_plus_1 as EA_1, b. day_plus_2 as EA_2, b. day_plus_3 as EA_3, 
  b. day_plus_4 as EA_4, b. day_plus_5 as EA_5, b. day_plus_6 as EA_6, b. day_plus_7 as EA_7,
  b. day_plus_8 as EA_8, b. day_plus_9 as EA_9, b. day_plus_10 as EA_10, b. day_plus_120 as EA_120,
  b. day_plus_75 as EA_75, b. day_plus_60 as EA_60, b.day_plus_90 as EA_90,
  b. day_plus_250 as EA_250, b.day_minus_250 as EA_minus_250,b.day_minus_1 as EA_minus_1, b.day_minus_2 as EA_minus_2, 
  b. day_minus_3 as EA_minus_3,  b.day_minus_5 as EA_minus_5, b.day_minus_6 as EA_minus_6
  from Guidance a left join work.Cal_trd_days_1926_2021 b
  on a.rdq=b.date
  and a.rdq ne .
  and b.date ne . 
  order by gvkey, datadate, rdq;
quit;

/* -------------------- Retrieve guidance data from IBES -------------------- */

data work.ibes_guidance;
	set ibes_guidance;
run;

proc sql;
  create table guidance3
  as select a.*, b.val_1, b.val_2, b. anndats as guidance_date, b.pdicity as guidance_pdcity 
  from Guidance2 a left join work.ibes_guidance b
  on a.ibtic=b.ticker
  and a.ibtic ne ' ' and b. ticker ne ' '
  and a.EA_minus_2 <= b.anndats <= a.EA_2
  order by gvkey, datadate, guidance_date;
quit;

proc sort data=guidance3;
	by gvkey datadate guidance_date;
run;

proc sort data=guidance3 nodupkey;
	by gvkey datadate;
run;

data guidance3a;
	set guidance3;

	**Guidance Variables;
	if val_1 ne . or val_2 ne . then Guidance_Ind=1; else Guidance_Ind=0;
	if val_2 ne . and Guidance_Ind=1 then Guidance_Range=1; else Guidance_Range=0;
	if Guidance_Ind=0 then Guidance_Range=.;
	if guidance_pdcity="ANN" and Guidance_Ind=1 then guidance_annual=1; else guidance_annual=0;
	if guidance_pdcity="QTR" and Guidance_Ind=1 then guidance_qtr=1; else guidance_qtr=0;
	if guidance_pdcity="SAN" and Guidance_Ind=1 then guidance_SAN=1; else guidance_SAN=0;
run;

/* -------------- Bring 2019 Lags of Guidance Indicator to 2020 ------------- */

proc sql;
  create table guidance3a2
  as select a.*, b.Guidance_Ind as Guidance_Ind_2019a
  from guidance3a a left join guidance3a b
  on a.gvkey=b.gvkey
  and year(a.datadate)=year(b.datadate)+1
  and month(a.datadate)=month(b.datadate)
  and a.fqtr=b.fqtr
  and a.datadate ne .
  and b.datadate ne .
  and b.Cal_year=2019
  and a.Cal_year=2020
  order by gvkey, datadate;
quit;

**Bring 2019 Lags of Guidance Indicator to 2021**;

proc sql;
  create table guidance3a3
  as select a.*, b.Guidance_Ind as Guidance_Ind_2019b
  from guidance3a2 a left join guidance3a b
  on a.gvkey=b.gvkey
  and year(a.datadate)=year(b.datadate)+2
  and month(a.datadate)=month(b.datadate)
  and a.fqtr=b.fqtr
  and a.datadate ne .
  and b.datadate ne .
  and b.Cal_year=2019
  and a.Cal_year=2021
  order by gvkey, datadate;
quit;

data guidance3b;
	set guidance3a3;
	if Cal_year=2020 then Guidance_Ind_2019=Guidance_Ind_2019a;
	if Cal_year=2021 then Guidance_Ind_2019=Guidance_Ind_2019b;
run;

proc sort data=guidance3b nodupkey;
	by gvkey datadate;
run;

/* -------------- Create seperate indicator variables for 2020 -------------- */

data guidance3bseparate2020;
	set guidance3b;
	if Cal_year=2020;
	NoGuide=0;
	StopGuide=0;
	ContinuedGuide=0;
	StartGuide=0;

	**All Years**;
	if Cal_year=2020 and Guidance_Ind=0 and Guidance_Ind_2019=0 then NoGuide=1;
	if Cal_year=2020 and Guidance_Ind=0 and Guidance_Ind_2019=1 then StopGuide=1;
	if Cal_year=2020 and Guidance_Ind=1 and Guidance_Ind_2019=1 then ContinuedGuide=1;
	if Cal_year=2020 and Guidance_Ind=1 and Guidance_Ind_2019=0 then StartGuide=1;
run;

/* -------------- Create seperate indicator variables for 2021 -------------- */

data guidance3bseparate2021;
	set guidance3b;
	if Cal_year=2021;
	NoGuide=0;
	StopGuide=0;
	ContinuedGuide=0;
	StartGuide=0;
	**All Years**;
	if Cal_year=2021 and Guidance_Ind=0 and Guidance_Ind_2019=0 then NoGuide=1;
	if Cal_year=2021 and Guidance_Ind=0 and Guidance_Ind_2019=1 then StopGuide=1;
	if Cal_year=2021 and Guidance_Ind=1 and Guidance_Ind_2019=1 then ContinuedGuide=1;
	if Cal_year=2021 and Guidance_Ind=1 and Guidance_Ind_2019=0 then StartGuide=1;
run;

proc sql;
	create	table guidance3c as
	select	a.*, b.NoGuide as NoGuide2019vs2020, b.StopGuide as StopGuide2019vs2020, b.ContinuedGuide as ContinuedGuide2019vs2020,
	b.StartGuide as StartGuide2019vs2020
	from	guidance3b as a left join guidance3bseparate2020 as b
	on		a.gvkey = b.gvkey
	and 	month(a.datadate)=month(b.datadate)
	and 	(a.Cal_year=2019 or a.Cal_year=2020);
quit;

proc sql;
	create	table guidance3d as
	select	a.*, b.NoGuide as NoGuide2019vs2021, b.StopGuide as StopGuide2019vs2021, b.ContinuedGuide as ContinuedGuide2019vs2021,
	b.StartGuide as StartGuide2019vs2021
	from	guidance3c as a left join guidance3bseparate2021 as b
	on		a.gvkey = b.gvkey
	and 	month(a.datadate)=month(b.datadate)
	and 	(a.Cal_year=2019 or a.Cal_year=2021);
quit;

proc sort data=guidance3d;
	by gvkey datadate;
run;

/* ---------------- Merge Guidance Indicator to main dataset ---------------- */

proc sql;
	create	table datawGuidance as
	select	a.*, b.NoGuide2019vs2020, b.StopGuide2019vs2020, b.ContinuedGuide2019vs2020, b.StartGuide2019vs2020, b.Guidance_Ind,
 	b.NoGuide2019vs2021, b.StopGuide2019vs2021, b.ContinuedGuide2019vs2021, b.StartGuide2019vs2021
	from	datawAudit2 as a left join guidance3d as b
	on		a.gvkey = b.gvkey
	and 	a.datadate=b.datadate;
quit;

/* -------------------------------------------------------------------------- */
/*                       Calculate restatement variables                      */
/* -------------------------------------------------------------------------- */

data Restatement;
	set datawGuidance;
run;

/* -------------- Retrieve Restatement Data from Data Analytics ------------- */

proc sql;
  create table Restatement2
  as select a.*, b.file_date as file_date_res, b.res_notif_key, b.res_accounting,b.res_fraud, b.res_cler_err,
 b.res_other, b.res_adverse, b.res_sec_invest, b.RES_END_DATE, b.DATE_OF_8K_402, b.RES_BEGIN_DATE
  from Restatement a left join work.auditnonreli b
  on not missing(b.company_fkey) and (b.company_fkey=a.cik_historical)
  	and b.res_begin_date <= a.datadate
	and b.res_end_date > a.lagdatadate
  and b.FILE_DATE ne .;
quit;

/* --------------------------- Generate variables --------------------------- */

%macro generate_restatement_indicators(year_list=, var_list=);
    %do var_index = 1 %to %sysfunc(countw(&var_list., %str( )));
            %let var =%scan(&var_list., &var_index., %str( ));

        %do year_index = 1 %to %sysfunc(countw(&year_list., %str( )));
            %let year =%scan(&year_list., &year_index., %str( ));

            if 1 le cal_month le 3   and Cal_year=&year. and file_date_res<Deadline&year_index. and &var.=1 then &var._App=1;
            if 4 le cal_month le 6   and Cal_year=&year. and file_date_res<Deadline&year_index. and &var.=1 then &var._App=1;
            if 7 le cal_month le 9   and Cal_year=&year. and file_date_res<Deadline&year_index. and &var.=1 then &var._App=1;
            if 10 le cal_month le 12 and Cal_year=&year. and file_date_res<Deadline&year_index. and &var.=1 then &var._App=1;

        %end;

        if &var._App ne 1 then &var._App=0;

    %end;
%mend;

data restatements2a;
	set restatement2;

	* Total Restatements;
	if res_notif_key ne . then TotalRestatement=1; else TotalRestatement=0;

	* Indicator for Significant Restatements;
	if DATE_OF_8K_402 ne . and TotalRestatement=1 then Restatement=1;
	else Restatement=0;

	* Create Little R Restatements;
	if DATE_OF_8K_402 =. and TotalRestatement=1 then LittleRestatement=1;
	else LittleRestatement=0;

	* Based on File Date;
	Deadline1 = '01mar2021'd;
	Deadline2 = '01mar2022'd;
	format Deadline1 Deadline2 date9.;

    * Generate indicators;
    %generate_restatement_indicators(year_list=2019 2020, var_list=TotalRestatement Restatement LittleRestatement);

run;

proc sql;
  create table restatements2a_mergeback
    as select distinct a.gvkey, a.datadate, sum(a.Restatement_App) as sum_Restatement_App,
	sum(a.LittleRestatement_App) as sum_LittleRestatement_App, sum(a.TotalRestatement_App) as sum_TotalRestatement_App, 
	sum(a.Restatement) as sum_Restatement,
	sum(a.LittleRestatement) as sum_LittleRestatement, sum(a.TotalRestatement) as sum_TotalRestatement
    from  restatements2a as a
    group by gvkey, datadate
  	order by gvkey, datadate;
quit;

/* --------------------- Merge back to original database -------------------- */

proc sort data=Restatement nodupkey;
	by gvkey datadate;
run;

proc sql;
  create table datawRestatement
  as select a.*, b.sum_Restatement_App, b.sum_LittleRestatement_App, b.sum_TotalRestatement_App,
  b.sum_Restatement, b.sum_LittleRestatement, b.sum_TotalRestatement
  from Restatement a left join restatements2a_mergeback b
  on a.gvkey=b.gvkey
  and a.datadate= b.datadate;
run;

proc sort data=datawRestatement nodupkey;
	by gvkey datadate;
run;

data datawRestatement2;
	set datawRestatement;

	* Restatements allowed for Tests;
	if sum_LittleRestatement_App>0 then LittleRestatement_App=1; else LittleRestatement_App=0;
	if sum_Restatement_App>0 then Restatement_App=1; else Restatement_App=0;
	if sum_TotalRestatement_App>0 then TotalRestatement_App=1; else TotalRestatement_App=0;

	* All Restatements;
	if sum_LittleRestatement>0 then LittleRestatement=1; else LittleRestatement=0;
	if sum_Restatement>0 then Restatement=1; else Restatement=0;
	if sum_TotalRestatement>0 then TotalRestatement=1; else TotalRestatement=0;
run;

proc sort data=datawRestatement2 nodupkey;
	by gvkey datadate;
run;

/* -------------------------------------------------------------------------- */
/*                            Create SPAC indicator                           */
/* -------------------------------------------------------------------------- */
* Dataset based on Blankespoor et al. 2022;


/* -------------------- Add SPAC Sample to Main Database -------------------- */

data SPACIndicator;
	set datawRestatement2;
	cik_historical_numeric=cik_historical*1;
run;

proc sql;
  create table SPACIndicator2 
  as select a.*, b.s1_date, b.ipo_amt, b.cik as cik_SPAC
  from SPACIndicator a left join spac_main b
  on a.cik_historical_numeric=b.cik
  order by gvkey, datadate;
quit;

data check (keep=gvkey datadate cik_historical cik_historical_numeric ipo_amt s1_date cik_SPAC);
	set SPACIndicator2;
	if ipo_amt ne .;
run;

data SPACIndicator3;
	set SPACIndicator2;
	if ipo_amt  ne . then SPACInd=1; else SPACInd=0;
run;

proc means data=SPACIndicator3 mean;
	var SPACInd;
	class calendar_qtr;
run;

**JUMP;

/* -------------------------------------------------------------------------- */
/*              Reshape dataset to faciliate regressions in Stata             */
/* -------------------------------------------------------------------------- */
* In the steps below we take long data and move it wide.;

/* ------------------------------ Set up macro ------------------------------ */

%macro move_year_data_to_columns(
	source_dataset=, 
	out_dataset=, 
	col_list=, 
	base_year=, 
	to_add_year=,
	custom_sql=,
	verbose=0
);

    * Build variable list for SQL query;
    %let col_string =;
    %do col_index = 1 %to %sysfunc(countw(&col_list., %str( )));
        %let col = %scan(&col_list., &col_index.,%str( ));
		%if &col_index = 1 %then %do;
        	%let col_string = b.&col. as &col._&to_add_year._to_&base_year.;
		%end; %else %do;
			%let col_string = &col_string.,b.&col. as &col._&to_add_year._to_&base_year.;
		%end;
    %end;

    %if &verbose. > 0 %then %put WARNING: Adding columns using the following conversions: &col_string.;

    * Difference in years;
    %let year_diff = %sysevalf(&base_year.-&to_add_year., integer);

	%if &verbose. > 0 %then %put WARNING: Year difference is: &year_diff.;

    * Run SQL query;
    proc sql undo_policy=none;
        create table &out_dataset.
        as select a.*, &col_string.
        from &source_dataset. a left join &source_dataset. b
        on a.gvkey=b.gvkey
		and b.Cal_year=&to_add_year.
        and a.Cal_year=&base_year.
        and year(a.datadate)=year(b.datadate)+&year_diff.
        and month(a.datadate)=month(b.datadate)
        and a.fqtr=b.fqtr
        and a.datadate ne .
        and b.datadate ne .
        and a.sec_delay_final ne . 
        and b.sec_delay_final ne .
        and a. HST_IS_ACCEL_FILER ne .
        and b. HST_IS_ACCEL_FILER ne .
		%if %isBlank(&custom_sql.) = 0 %then %do;
			&custom_sql.
		%end;
        and a.rdq<=a.fdate_final  
        and b.rdq<=b.fdate_final
        and a.SPACInd=0
        and b.SPACInd=0
        order by gvkey, datadate;
	quit;

	* Sort and remote duplicates;
	proc sort data=&out_dataset. nodupkey;
		by gvkey datadate;
	run;
%mend;

/* ------------------------ Set the starting dataset ------------------------ */

data data_with_columns;
	set SPACIndicator3;
run;

/* ------------------ Define columns that need to be moved ------------------ */

%let columns_to_move = EAdelay EAdelay_dat sec_delay_final LateFiler COVID_Extension2 TimelyFiler;

/* ------------------------------ Bring forward ----------------------------- */

* Bring 2020 delays to year 2021;
%move_year_data_to_columns(
    source_dataset = data_with_columns,
    out_dataset = data_with_columns,
    col_list = &columns_to_move.,
    base_year = 2021,
    to_add_year = 2020,
    verbose = 1
);

* Bring 2019 delays to year 2021;
%move_year_data_to_columns(
    source_dataset = data_with_columns,
    out_dataset = data_with_columns,
    col_list = &columns_to_move.,
    base_year = 2021,
    to_add_year = 2019,
    verbose = 1
);

* Bring 2019 delays to year 2020;
%move_year_data_to_columns(
    source_dataset = data_with_columns,
    out_dataset = data_with_columns,
    col_list = &columns_to_move.,
    base_year = 2020,
    to_add_year = 2019,
    verbose = 1
);


* Bring 2018 delays to year 2020;
%move_year_data_to_columns(
    source_dataset = data_with_columns,
    out_dataset = data_with_columns,
    col_list = &columns_to_move.,
    base_year = 2020,
    to_add_year = 2018,
    verbose = 1
);

* Bring 2018 delays to year 2019;
%move_year_data_to_columns(
    source_dataset = data_with_columns,
    out_dataset = data_with_columns,
    col_list = &columns_to_move.,
    base_year = 2019,
    to_add_year = 2018,
    verbose = 1
);

* Bring 2017 delays to year 2020;
%move_year_data_to_columns(
    source_dataset = data_with_columns,
    out_dataset = data_with_columns,
    col_list = &columns_to_move.,
    base_year = 2020,
    to_add_year = 2017,
    verbose = 1
);

* Bring 2017 delays to year 2018;
%move_year_data_to_columns(
    source_dataset = data_with_columns,
    out_dataset = data_with_columns,
    col_list = &columns_to_move.,
    base_year = 2018,
    to_add_year = 2017,
    verbose = 1
);


/* ----------------------------- Bring backward ----------------------------- */

* Bring 2020 delays to year 2019;
%move_year_data_to_columns(
    source_dataset = data_with_columns,
    out_dataset = data_with_columns,
    col_list = &columns_to_move.,
    base_year = 2019,
    to_add_year = 2020,
    verbose = 1
);

* Bring 2021 delays to year 2020;
%move_year_data_to_columns(
    source_dataset = data_with_columns,
    out_dataset = data_with_columns,
    col_list = &columns_to_move.,
    base_year = 2020,
    to_add_year = 2021,
    verbose = 1
);

* Bring 2021 delays to year 2019;
%move_year_data_to_columns(
    source_dataset = data_with_columns,
    out_dataset = data_with_columns,
    col_list = &columns_to_move.,
    base_year = 2019,
    to_add_year = 2021,
    verbose = 1,
	custom_sql = and a.EAdelay_2020_to_2019 ne .
);

/* ------------------------- Sort dataset and format ------------------------ */

proc sort data=data_with_columns nodupkey;
	by gvkey datadate;
run;

data data_with_columns;
	set data_with_columns;
	format  fdate_final date9.;
	gvkey_num=gvkey*1;
run;

/* -------------------------------------------------------------------------- */
/*                Merge in the calendar rotation bias estimates               */
/* -------------------------------------------------------------------------- */

/* ----------------- Convert date to be compatible with rdq ----------------- */

data calendar_bias_per_day;
	set calendar_bias_per_day;
	format date YYMMDDN8.;
run;

/* ---------------------------- Join based on RDQ --------------------------- */

proc sql;
  create table data_2020fa
  as select a.*, b.calendar_bias, b.date as cb_date, b.day_of_week as rdq_weekday, b.week_rank as rdq_week_rank
  from data_with_columns a left join calendar_bias_per_day b
  on a.rdq=b.date
  order by gvkey, datadate;
quit;

proc sort data=data_2020fa nodupkey;
	by gvkey datadate;
run;

/* ---- Quick inspect to make sure calendar bias distribution makes sense --- */

proc sql;
	select calendar_bias, count(*) as count
	from data_2020fa
	group by calendar_bias;
quit;

/* -------------------------- Add pattern indicator ------------------------- */

data data_2020fa2;
	set data_2020fa;

	** We have an issue with months that sometimes have 4 occurances of a weekday and sometimes 5 (e.g., sometimes a month as 4 Fridays, sometimes 5);
	** If we don't account for this we understimate the frequency of pattern firms. So instead, we set week_rank 5 to 4 to account for this. ;

	rdq_week_rank_adj = rdq_week_rank;
	if rdq_week_rank = 5 then rdq_week_rank_adj = 4;

	**Create lags;
	by gvkey calendar_qtr;
	prev_rdq_weekday = lag(rdq_weekday);
	prev_rdq_weekrank = lag(rdq_week_rank);
	prev_rdq_weekrank_adj = lag(rdq_week_rank_adj);
	if first.gvkey then prev_rdq_weekday= '';
	if first.gvkey then prev_rdq_weekrank=.;
	if first.gvkey then prev_rdq_weekrank_adj=.;

	**Create comparisons;
	if not(missing(prev_rdq_weekday)) then same_weekday_as_last = rdq_weekday = prev_rdq_weekday;
	if not(missing(prev_rdq_weekrank)) then same_weekrank_as_last = rdq_week_rank = prev_rdq_weekrank;
	if not(missing(prev_rdq_weekrank_adj)) then same_weekrank_as_last_adj = rdq_week_rank_adj = prev_rdq_weekrank_adj;

	** Pattern indicators;
	if not(missing(prev_rdq_weekrank_adj)) then same_week_and_day = same_weekrank_as_last_adj & same_weekday_as_last;
	if same_week_and_day=1 then Pattern_firm_final=1; else Pattern_firm_final=0;
	if same_week_and_day=. then Pattern_firm_final=.;

run;

/* -------------------------------------------------------------------------- */
/*                Create Change in Sales and Expenses variables               */
/* -------------------------------------------------------------------------- */

data data_2020fa2b;
	set data_2020fa2;
	if (Cal_year=2019 or Cal_year=2020) and lag4SALEQ ne 0 then Ch_SALEQ=(SALEQ-lag4SALEQ)/lag4SALEQ;
	if Cal_year=2021 and lag8SALEQ ne 0 then Ch_SALEQ=(SALEQ-lag8SALEQ)/lag8SALEQ;
	if Ch_SALEQ=. then Ch_SALEQ=0;

	if (Cal_year=2019 or Cal_year=2020) and lag4XOPRQ ne 0 then Ch_XOPRQ=(XOPRQ-lag4XOPRQ)/lag4XOPRQ;
	if Cal_year=2021 and lag8XOPRQ ne 0 then Ch_XOPRQ=(XOPRQ-lag8XOPRQ)/lag8XOPRQ;
	if Ch_XOPRQ=. then Ch_XOPRQ=0;

	**Absolute Changes**;
	if (Cal_year=2019 or Cal_year=2020) and lag4SALEQ ne 0 then absCh_SALEQ=(abs(SALEQ-lag4SALEQ))/lag4SALEQ;
	if Cal_year=2021 and lag8SALEQ ne 0 then absCh_SALEQ=(abs(SALEQ-lag8SALEQ))/lag8SALEQ;
	if absCh_SALEQ=. then absCh_SALEQ=0;

	if (Cal_year=2019 or Cal_year=2020) and lag4XOPRQ ne 0 then absCh_XOPRQ=(abs(XOPRQ-lag4XOPRQ))/lag4XOPRQ;
	if Cal_year=2021 and lag8XOPRQ ne 0 then absCh_XOPRQ=(abs(XOPRQ-lag8XOPRQ))/lag8XOPRQ;
	if absCh_XOPRQ=. then absCh_XOPRQ=0;

	**Indicator Variables**;
	if Ch_SALEQ<=(-.10) then BigDecreaseSales=1; else BigDecreaseSales=0;
	if Ch_XOPRQ>=.10 then BigIncreaseExp=1; else BigIncreaseExp=0;
	if absCh_SALEQ>=.10 then BigChangeSales=1; else BigChangeSales=0;
	if absCh_XOPRQ>=.10 then BigChangeExp=1; else BigChangeExp=0;
run;

/* -------------------------------------------------------------------------- */
/*                       Create additional SEC variables                      */
/* -------------------------------------------------------------------------- */

data data_2020fa2c;
	set data_2020fa2b;

	**Last Dayfiler;
	if fdate_final=sec_deadline_final2 then LastDayFiler=1; else LastDayFiler=0;
	if fdate_final=. then LastDayFiler=.;
	if sec_deadline_final2=. then LastDayFiler=.;

	**DiffF_dateDeadline;
	DiffFdatetoDeadline=sec_deadline_final2-fdate_final;

	**DiffF_dateDeadline;
	DiffRDQtoFDATE=fdate_final-rdq;
run;

proc means data=data_2020fa2c n mean p25 p75;
	var DiffFdatetoDeadline DiffRDQtoFDATE Concurrent;
	class calendar_qtr;
run;

/* -------------------------------------------------------------------------- */
/*              Add Textual Characteristics Variables from Python             */
/* -------------------------------------------------------------------------- */

proc sql;
	create	table data_2020fa2d as
	select	a.*, b.full_number_of_sentences, b.mda_number_of_sentences,b.rf_number_of_risk_factors, 
	b. full_fog_index, b.full_number_of_covid_words, b.rf_number_of_covid_words
	from data_2020fa2c as a left join text_statistics as b
	on		a.Fname = b.fname;
quit;

proc means data=data_2020fa2d ;
	var full_number_of_sentences;
	class calendar_qtr;
run;

data data_2020fa4;
	set data_2020fa2d;
run;

proc sort data=data_2020fa4 nodupkey;
	by gvkey datadate;
run;

/* -------------------------------------------------------------------------- */
/*           Define New Concurrent and ConcurrentFilerChg Variables           */
/* -------------------------------------------------------------------------- */

%macro create_lags_by_year(source_dataset=, out_dataset=, col_list=, to_add_year=, verbose=0);

    * Build variable list for SQL query;
    %let col_string =;
    %do col_index = 1 %to %sysfunc(countw(&col_list., %str( )));
        %let col = %scan(&col_list., &col_index.,%str( ));

		%let col_name = &col._in_&to_add_year.;
		%if &col_index = 1 %then %do;
        	%let col_string = b.&col. as &col_name.;
		%end; %else %do;
			%let col_string = &col_string.,b.&col. as &col_name.;
		%end;
    %end;

    %if &verbose. > 0 %then %put WARNING: Adding columns using the following conversions: &col_string.;

    * Run SQL query;
    proc sql undo_policy=none;
        create table &out_dataset.
        as select a.*, &col_string.
        from &source_dataset. a left join &source_dataset. b
	    on a.gvkey=b.gvkey
		and b.Cal_year = &to_add_year.
		and a.Cal_year ne &to_add_year.
		and a.fqtr = b.fqtr
		and a.Cal_year ne .
		and b.Cal_year ne .
		and a.fqtr ne .
		and b.fqtr ne .
		and a.sec_delay_final ne . 
		and b.sec_delay_final ne .
		and a.HST_IS_ACCEL_FILER ne .
		and b.HST_IS_ACCEL_FILER ne . 
		and a.rdq<=a.fdate_final  
		and b.rdq<=b.fdate_final
		and a.SPACInd=0
		and b.SPACInd=0
	    order by gvkey, datadate;
	quit;

	* Sort and remote duplicates;
	proc sort data=&out_dataset. nodupkey;
		by gvkey datadate;
	run;
%mend;

/* --------------------------- Add Concurrent Lag --------------------------- */

data data_with_columns_concurrent;
	set data_2020fa4;
run;

* Add 2020;
%create_lags_by_year(
    source_dataset = data_with_columns_concurrent,
    out_dataset = data_with_columns_concurrent,
    col_list = Concurrent,
    to_add_year = 2020,
    verbose = 1
);

* Add 2019;
%create_lags_by_year(
    source_dataset = data_with_columns_concurrent,
    out_dataset = data_with_columns_concurrent,
    col_list = Concurrent,
    to_add_year = 2019,
    verbose = 1
);

* Add 2018;
%create_lags_by_year(
    source_dataset = data_with_columns_concurrent,
    out_dataset = data_with_columns_concurrent,
    col_list = Concurrent,
    to_add_year = 2018,
    verbose = 1
);

* Calculate change status indicator;

data data_with_columns_concur_1;
	set data_with_columns_concurrent;

	*2021;
	if Cal_year=2021 and Concurrent=1 and Concurrent_in_2019=1 then ExistingConcurrent2021=1; else ExistingConcurrent2021=0;
	if Cal_year=2021 and Concurrent=1 and Concurrent_in_2019=0 then NewConcurrent2021=1; else NewConcurrent2021=0;
	if Cal_year=2021 and Concurrent=0 and Concurrent_in_2019=1 then StopConcurrent2021=1; else StopConcurrent2021=0;
	if Cal_year=2021 and Concurrent=0 and Concurrent_in_2019=0 then NoConcurrent2021=1; else NoConcurrent2021=0;

	* 2020;
	if Cal_year=2020 and Concurrent=1 and Concurrent_in_2019=1 then ExistingConcurrent2020=1; else ExistingConcurrent2020=0;
	if Cal_year=2020 and Concurrent=1 and Concurrent_in_2019=0 then NewConcurrent2020=1; else NewConcurrent2020=0;
	if Cal_year=2020 and Concurrent=0 and Concurrent_in_2019=1 then StopConcurrent2020=1; else StopConcurrent2020=0;
	if Cal_year=2020 and Concurrent=0 and Concurrent_in_2019=0 then NoConcurrent2020=1; else NoConcurrent2020=0;

	* 2019;
	if Cal_year=2019 and Concurrent=1 and Concurrent_in_2018=1 then ExistingConcurrent2019=1; else ExistingConcurrent2019=0;
	if Cal_year=2019 and Concurrent_in_2018 =. then ExistingConcurrent2019=.;

	if Cal_year=2019 and Concurrent=1 and Concurrent_in_2018=0 then NewConcurrent2019=1; else NewConcurrent2019=0;
	if Cal_year=2019 and Concurrent_in_2018 =. then NewConcurrent2019=.;

	if Cal_year=2019 and Concurrent=0 and Concurrent_in_2018=1 then StopConcurrent2019=1; else StopConcurrent2019=0;
	if Cal_year=2019 and Concurrent_in_2018 =. then StopConcurrent2019=.;

	if Cal_year=2019 and Concurrent=0 and Concurrent_in_2018=0 then NoConcurrent2019=1; else NoConcurrent2019=0;
	if Cal_year=2019 and Concurrent_in_2018 =. then NoConcurrent2019=.;
	
	* Create change variables;

	ConcurrentFilerChg=0;

	* 2019;
	if Cal_year=2019 and NewConcurrent2019 = 1 			then ConcurrentFilerChg =  1;
	if Cal_year=2019 and ExistingConcurrent2019 = 1 	then ConcurrentFilerChg =  0;
	if Cal_year=2019 and StopConcurrent2019 = 1 		then ConcurrentFilerChg = -1;
	if Cal_year=2019 and NewConcurrent2019 = . 			then ConcurrentFilerChg =  .;
	

	* 2020;
	if Cal_year=2020 and NewConcurrent2020 = 1 			then ConcurrentFilerChg =  1;
	if Cal_year=2020 and ExistingConcurrent2020 = 1 	then ConcurrentFilerChg =  0;
	if Cal_year=2020 and StopConcurrent2020 = 1 		then ConcurrentFilerChg = -1;

	* 2021;
	if Cal_year=2021 and NewConcurrent2021 = 1 			then ConcurrentFilerChg =  1;
	if Cal_year=2021 and ExistingConcurrent2021 = 1 	then ConcurrentFilerChg =  0;
	if Cal_year=2021 and StopConcurrent2021 = 1 		then ConcurrentFilerChg = -1;
run;


* Inspect;

proc sql INOBS=100;
	create table _inspect
	as select gvkey, Cal_year, fqtr, fdate_final, rdq, Concurrent, Concurrent_in_2018, Concurrent_in_2019, Concurrent_in_2020, ConcurrentFilerChg
	from data_with_columns_concur_1
	where Cal_year > 2017;
quit;


/* --------------------- Make Final Concurrent Variable --------------------- */

data data_2020fa110;
	set data_with_columns_concur_1;
	NewConcurrentFinal=0;
	if Cal_year=2020 and NewConcurrent2020=1 then NewConcurrentFinal=1;
	if Cal_year=2021 and NewConcurrent2021=1 then NewConcurrentFinal=1;
	if Cal_year=2019 and NewConcurrent2019=1 then NewConcurrentFinal=1;
	if Cal_year=2019 and NewConcurrent2019=. then NewConcurrentFinal=.;
	StopConcurrentFinal=0;
	if Cal_year=2020 and StopConcurrent2020=1 then StopConcurrentFinal=1;
	if Cal_year=2021 and StopConcurrent2021=1 then StopConcurrentFinal=1;
	if Cal_year=2019 and StopConcurrent2019=1 then StopConcurrentFinal=1;
	if Cal_year=2019 and StopConcurrent2019=. then StopConcurrentFinal=.;
run;

proc means data=data_2020fa110 mean;
	var NewConcurrentFinal;
	class calendar_qtr;
run;

/* -------------------------------------------------------------------------- */
/*                    Add Institutional Ownership variables                   */
/* -------------------------------------------------------------------------- */

proc sql;
  create table data_2020fa13
  as select a.*, b.IOR
   from data_2020fa110  a left join work.IO_TimeSeries b
	on a.permno= b.permno
	and a.datadate = b.rdate
  order by gvkey, datadate;
quit;

data data_2020fa14;
	set data_2020fa13;
	if IOR=. then IOR=0;
	IOR_log= log (1 + IOR);
run;

/* -------------------------------------------------------------------------- */
/*                            Define Reporting Lag                            */
/* -------------------------------------------------------------------------- */

data data_2020fa15;
	set data_2020fa14;
	rep_lag= rdq-datadate;
	if rep_lag > 0 then log_rep_lag= log(rep_lag);

	**Squared**;
	rep_lag_squared=rep_lag*rep_lag;

	**Cubed**;
	rep_lag_cubed=rep_lag*rep_lag*rep_lag;
run;

/* --------------------- Day of the Week, Month and Year -------------------- */

data data_2020fa17;
	set data_2020fa15;
	**Day of the Week where 1 = Sunday, 2 = Monday, ?, 7 = Saturday**;
	WeekDayExtra=weekday(rdq);
	if WeekdayExtra=1 then SundayInd=1; else SundayInd=0;
	if WeekdayExtra=2 then MondayInd=1; else MondayInd=0;
	if WeekdayExtra=3 then TuesdayInd=1; else TuesdayInd=0;
	if WeekdayExtra=4 then WednesdayInd=1; else WednesdayInd=0;
	if WeekdayExtra=5 then ThursdayInd=1; else ThursdayInd=0;
	if WeekdayExtra=6 then FridayInd=1; else FridayInd=0;
	if WeekdayExtra=7 then SaturdayInd=1; else SaturdayInd=0;

	**Indicator for Year**;
	if Cal_year=2019 then Cal_year2019=1; else Cal_year2019=0;
	if Cal_year=2020 then Cal_year2020=1; else Cal_year2020=0;
	if Cal_year=2021 then Cal_year2021=1; else Cal_year2021=0;

	**Indicator for Month**;
	Month=Month(rdq);
	if Month=1 then January=1; else January=0;
	if Month=2 then February=1; else February=0;
	if Month=3 then March=1; else March=0;
	if Month=4 then April=1; else April=0;
	if Month=5 then May=1; else May=0;
	if Month=6 then June=1; else June=0;
	if Month=7 then July=1; else July=0;
	if Month=8 then August=1; else August=0;
	if Month=9 then September=1; else September=0;
	if Month=10 then October=1; else October=0;
	if Month=11 then November=1; else November=0;
	if Month=12 then December=1; else December=0;
run;

data work.beforesampleselection;
	set data_2020fa17;
run;

/* -------------------------------------------------------------------------- */
/*                              Sample Selection                              */
/* -------------------------------------------------------------------------- */

* Quick store for debugging;

PROC EXPORT DATA=work.beforesampleselection
	OUTFILE="%SYSFUNC(DEQUOTE(&OUT_FOLDER.))\tmp_before_sample_selection.dta"
	DBMS=dta REPLACE;
RUN;

* Restrict sample based on dates;

data data_2020fa5;
	set work.beforesampleselection;

	* Restrict to calendar years 2018 to 2022;

	if Cal_year>2018 & Cal_year<2022;
	if Cal_year=2021 & Cal_month>6 then delete;
run;

%put WARNING: Number of observations report: before: %num_obs(beforesampleselection) || after: %num_obs(data_2020fa5);

/* --------------------- Delete firms without sec_delay --------------------- */

data data_2020f4;
	set data_2020fa5;
	if  sec_delay_final=. then delete;
run;

%put WARNING: Number of observations report: before: %num_obs(data_2020fa5) || after: %num_obs(data_2020f4);

/* -------------- Delete firms without accelerated filer status ------------- */

data data_2020f5;
	set data_2020f4;
	if  HST_IS_ACCEL_FILER=. then delete;
run;

%put WARNING: Number of observations report: before: %num_obs(data_2020f4) || after: %num_obs(data_2020f5);

/* ------------------- Delete observations with RDQ>FDATE ------------------- */

data data_2020f5a;
	set data_2020f5;
	if rdq>fdate_final then delete;
run;

%put WARNING: Number of observations report: before: %num_obs(data_2020f5) || after: %num_obs(data_2020f5a);

/* ------------------------------ Remove spacs ------------------------------ */

data data_2020f5a1;
	set data_2020f5a;
	if SPACInd=1 then delete;
run;

%put WARNING: Number of observations report: before: %num_obs(data_2020f5a) || after: %num_obs(data_2020f5a1);

/* --- Remove observations with missing variables for the delay variables --- */

proc sort data=data_2020f5a1 nodupkey;
	by gvkey datadate;
run;

data data_2020f5a2;
	set data_2020f5a1;
	if Cal_year=2020 and EAdelay_2019_to_2020 =. or EADelay =. then delete;
	if Cal_year=2019 and EAdelay_2020_to_2019 =. or EADelay =. then delete;
	if Cal_year=2021 and (EAdelay_2020_to_2021 =. or EAdelay_2019_to_2021 =. or EADelay =.) then delete;
run;

proc sort data=data_2020f5a2 nodupkey;
	by gvkey datadate;
run;

%put WARNING: Number of observations report: before: %num_obs(data_2020f5a1) || after: %num_obs(data_2020f5a2);

proc means data=data_2020f5a2 n mean;
	var NewConcurrent2020;
	class calendar_qtr;
run;

/* -------------------------------------------------------------------------- */
/*                             Winsorize variables                            */
/* -------------------------------------------------------------------------- */

%WT(data=data_2020f5a2, out=data_2020f6, vars=BM DA ME ROA disc_acc 
GDWLIPQ WDPQ SPIQ WD_s absSPIQ_s absOtherSPI_s RCPQ_s absCh_SALEQ absCh_XOPRQ
nonlinear UE_m2_2_s Predict beta persistence MB Log_ME bhar_d_m1_to_1 mturnover_m12_to_dat mturnoveravg_m12_to_dat
IOR IOR_log Lognumest numest, 
byvar= calendar_qtr , type=W, pctl=1 99);

/* -------------------------------------------------------------------------- */
/*                             Create quintile ranks                          */
/* -------------------------------------------------------------------------- */

PROC SORT DATA=data_2020f6;
	BY Calendar_qtr;
RUN;

proc rank data=data_2020f6 out=data_2020f7 groups=5;
	var ME choi_ofsize1 choi_ofsize2 ln_audit_fees total_audit_fees num_clients
	nonlinear UE_m2_2_s Predict beta persistence MB Log_ME choi_large_office_by_fees choi_large_office_by_clients
	beck_large_office beck_ln_office_size ege_ln_office_size auditor_tenure BM
	full_number_of_sentences mda_number_of_sentences;
	by calendar_qtr ;
	ranks ME_r choi_ofsize1_r choi_ofsize2_r ln_audit_fees_r total_audit_fees_r num_clients_r
	nonlinear_r UE_m2_2_s_r Predict_r beta_r persistence_r MB_r Log_ME_r choi_large_office_by_fees_r choi_large_office_by_clients_r
	beck_large_office_r beck_ln_office_size_r ege_ln_office_size_r auditor_tenure_r BM_r
	full_nmb_of_sentences_r mda_number_of_sentences_r;			
run;

data data_2020f8;
	set data_2020f7;
	ME_r_s= ME_r/4;
	choi_ofsize1_r_s=choi_ofsize1_r/4;
	choi_ofsize2_r_s=choi_ofsize2_r/4;
	ln_audit_fees_r_s= ln_audit_fees_r/4;
	total_audit_fees_r_s= total_audit_fees_r/4;
	num_clients_r_s=num_clients_r/4;
	Nonlinear_r_s= Nonlinear_r/4;
	UE_m2_2_s_r_s= UE_m2_2_s_r/4;
	Predict_r_s=Predict_r /4;
	beta_r_s= beta_r/4;
	persistence_r_s=persistence_r /4;
	MB_r_s= MB_r/4;
	Log_ME_r_s=Log_ME_r/4;
	choi_large_office_by_fees_r_s= choi_large_office_by_fees_r/4;
	choi_large_office_by_clients_r_s=choi_large_office_by_clients_r/4;
	beck_large_office_r_s=beck_large_office_r/4; 
	beck_ln_office_size_r_s=beck_ln_office_size_r/4; 
	ege_ln_office_size_r_s=ege_ln_office_size_r/4; 
	auditor_tenure_r_s=auditor_tenure_r/4;
	BM_r_s=BM_r/4;
	full_nmb_of_sentences_r_s=full_nmb_of_sentences_r/4;
	mda_number_of_sentences_r_s=mda_number_of_sentences_r/4;
run;


/* -------------------------------------------------------------------------- */
/*                           Create balanced sample                           */
/* -------------------------------------------------------------------------- */

data SubSampleERC;
	set data_2020f8;
	if missing(UE_m2_2_s*Predict*beta*persistence*bm*Log_ME*LossEPS*rep_lag*Lognumest*IOR*mturnoveravg_m12_to_dat) then delete;
run;

PROC SORT DATA=SubSampleERC;
	BY Calendar_qtr;
RUN;

proc rank data=SubSampleERC out=SubSampleERC2 groups=10;
	var UE_m2_2_s ;
	by calendar_qtr ;
	ranks UE_m2_2_s_rb; 			
run;

data SubSampleERC3;
	set SubSampleERC2;
	UE_m2_2_s_rb_s= UE_m2_2_s_rb/9;
run;

proc sql;
  create table data_2020f10
  as select a.*, b.UE_m2_2_s_rb_s, b.UE_m2_2_s_rb, b.UE_m2_2_s as UE_m2_2_sb, b.Predict as Predictb ,
	b.beta as betab, b.persistence as persistenceb, b.bm as bmb, b.Log_ME as Log_MEb, b.LossEPS as LossEPSb, 
	b.rep_lag as rep_lagb, b.Lognumest as Lognumestb, b.IOR as IORb, b.mturnoveravg_m12_to_dat as mturnoveravg_m12_to_datb
   from data_2020f8  a left join SubSampleERC3 b
	on a.gvkey= b.gvkey
	and a.datadate = b.datadate
  order by gvkey, datadate;
quit;

proc sort data=data_2020f10 nodupkey;
	by gvkey datadate;
run;

proc means data=data_2020f10 n mean;
	var UE_m2_2_sb Predictb betab persistenceb bmb Log_MEb LossEPSb rep_lagb Lognumestb IORb mturnoveravg_m12_to_datb;
run;

/* -------------------------------------------------------------------------- */
/*                      Add preliminary vs. final dataset                     */
/* -------------------------------------------------------------------------- */

/* --------------------------- Preliminary dataset -------------------------- */

proc sort data=wrds_csq_pit;
	by gvkey datadate;
run;

data Preliminary;
	set work.wrds_csq_pit;
	if UPDQ=2;
run;

proc sort data=Preliminary out=Preliminary2;
	by gvkey datadate ;
run;

proc sort data=Preliminary out=Preliminary2 nodupkey;
	by gvkey datadate ;
run;

/* ------------------------------ Final dataset ----------------------------- */

data Final;
	set work.wrds_csq_pit;
	if UPDQ=3;
run;

proc sort data=Final out=Final2;
	by gvkey datadate ;
run;

proc sort data=Final out=Final2 nodupkey;
	by gvkey datadate ;
run;

/* ------------------ Merge preliminary and final datasets ------------------ */

proc sql;
  create table data_2020f11
  as select a.*, b.PDATEQ, b.EPSPIY as EPSPIY_prelim, b.EPSFIY as EPSFIY_prelim,
  b.EPSPIQ as EPSPIQ_prelim, b.EPSFIQ as EPSFIQ_prelim
   from data_2020f10 a left join Preliminary2 b
	on a.gvkey= b.gvkey
	and a.datadate = b.datadate
  order by gvkey, datadate;
quit;

proc sort data=data_2020f11 nodupkey;
	by gvkey datadate;
run;

proc sql;
  create table data_2020f12
  as select a.*, b.FDATEQ, b.EPSPIY as EPSPIY_final, b.EPSFIY as EPSFIY_final,
  b.EPSPIQ as EPSPIQ_final, b.EPSFIQ as EPSFIQ_final
   from data_2020f11 a left join Final2 b
	on a.gvkey= b.gvkey
	and a.datadate = b.datadate
  order by gvkey, datadate;
quit;

proc sort data=data_2020f12 nodupkey;
	by gvkey datadate;
run;

data data_2020f12a;
	set data_2020f12;
	if EPSPIY_final ne . and EPSPIY_prelim ne . and EPSPIY_final ne EPSPIY_prelim then PremFinalDiff=1; else PremFinalDiff=0;
	if EPSFIY_final ne . and EPSFIY_prelim ne . and EPSFIY_final ne EPSFIY_prelim then PremFinalDiff=1;
	if EPSPIQ_final ne . and EPSPIQ_prelim ne . and EPSPIQ_final ne EPSPIQ_prelim then PremFinalDiff=1;
	if EPSFIQ_final ne . and EPSFIQ_prelim ne . and EPSFIQ_final ne EPSFIQ_prelim then PremFinalDiff=1;
run;

proc means data=data_2020f12a n mean;
	var PremFinalDiff EADelay fdate_final;
run;

/* ------------------ Robustness for EA Revision ------------------ */

data data_2020f13;
	set data_2020f12a;
run;

proc means data=data_2020f13 n mean;
	var PremFinalDiff EADelay;
run;

/* -------------------------------------------------------------------------- */
/*                       Add auditor variables for tests                      */
/* -------------------------------------------------------------------------- */

PROC SORT DATA=data_2020f13;
	BY Calendar_qtr;
RUN;

proc rank data=data_2020f13 out=data_2020f14 groups=2;
	var choi_ofsize2 auditor_tenure;
	by calendar_qtr ;
	ranks choi_ofsize2_median auditor_tenure_median;			
run;

proc means data=data_2020f14 n mean;
	var choi_ofsize2;
	class choi_ofsize2_median;
run;

proc means data=data_2020f14 n mean;
	var auditor_tenure;
	class auditor_tenure_median;
run;

proc means data=data_2020f14 n mean;
	var is_big_4;
	class is_big_4;
run;

/* -------------------------------------------------------------------------- */
/*                             Create Tercile ranks                           */
/* -------------------------------------------------------------------------- */

PROC SORT DATA=data_2020f14;
BY Calendar_qtr;
RUN;

proc rank data=data_2020f14 out=data_2020f15 groups=3;
	var ME choi_ofsize1 choi_ofsize2 ln_audit_fees total_audit_fees num_clients
	nonlinear UE_m2_2_s Predict beta persistence MB Log_ME choi_large_office_by_fees choi_large_office_by_clients
	beck_large_office beck_ln_office_size ege_ln_office_size auditor_tenure BM
	full_number_of_sentences mda_number_of_sentences;
	by calendar_qtr ;
	ranks ME_r3 choi_ofsize1_r3 choi_ofsize2_r3 ln_audit_fees_r3 total_audit_fees_r3 num_clients_r3
	nonlinear_r3 UE_m2_2_s_r3 Predict_r3 beta_r3 persistence_r3 MB_r3 Log_ME_r3 choi_large_office_by_fees_r3 choi_large_office_by_clients_r3
	beck_large_office_r3 beck_ln_office_size_r3 ege_ln_office_size_r3 auditor_tenure_r3 BM_r3
	full_number_of_sentences_r3 mda_number_of_sentences_r3;			
run;

data data_2020f16;
	set data_2020f15;
	ME_r3_s= ME_r3/2;
	choi_ofsize1_r3_s=choi_ofsize1_r3/2;
	choi_ofsize2_r3_s=choi_ofsize2_r3/2;
	ln_audit_fees_r3_s= ln_audit_fees_r3/2;
	total_audit_fees_r3_s= total_audit_fees_r3/2;
	num_clients_r3_s=num_clients_r3/2;
	Nonlinear_r3_s= Nonlinear_r3/2;
	UE_m2_2_s_r3_s= UE_m2_2_s_r3/2;
	Predict_r3_s=Predict_r3 /2;
	beta_r3_s= beta_r3/2;
	persistence_r3_s=persistence_r3 /2;
	MB_r3_s= MB_r3/2;
	Log_ME_r3_s=Log_ME_r3/2;
	choi_large_office_by_fees_r3_s= choi_large_office_by_fees_r3/2;
	choi_large_office_by_cli_r3_s=choi_large_office_by_clients_r3/2;
	beck_large_office_r3_s=beck_large_office_r3/2; 
	beck_ln_office_size_r3_s=beck_ln_office_size_r3/2; 
	ege_ln_office_size_r3_s=ege_ln_office_size_r3/2; 
	auditor_tenure_r3_s=auditor_tenure_r3/2;
	BM_r_s=BM_r3/2;
	full_number_of_sentences_r3_s=full_number_of_sentences_r3/2; 
	mda_number_of_sentences_r3_s=mda_number_of_sentences_r3/2;
run;

/* -------------------------------------------------------------------------- */
/*            Seasonal Lag for New Concurrent Final                           */
/* -------------------------------------------------------------------------- */

proc sql;
	create table data_2020f17 
	as select a.*, b. NewConcurrentFinal as lag4NewConcurrentFinal
	from data_2020f16 a left join data_2020f16  b
	on a.gvkey=b.gvkey
	and (a.Cal_year=b.Cal_year+1 and a.fqtr=b.fqtr)
	and a.Cal_year ne .
	and b.Cal_year ne .
	and a.fqtr ne .
	and b.fqtr ne .
	order by gvkey, datadate;
quit;

proc means data=data_2020f17;
	var new_auditor_office;
run;

/* -------------------------------------------------------------------------- */
/*            MDA Number of Sentences for 2019 as a Locked Rank               */
/* -------------------------------------------------------------------------- */

data MDA2019 (keep=gvkey datadate Cal_year fqtr mda_number_of_sentences calendar_qtr mda_no_sent2019);
	set data_2020f17;
	if Cal_year=2019;
	mda_no_sent2019=mda_number_of_sentences;
run;

PROC SORT DATA=MDA2019;
	BY Calendar_qtr;
RUN;

proc rank data=MDA2019 out=MDA2019b groups=5;
	var mda_no_sent2019;
	by calendar_qtr ;
	ranks mda_no_sent2019_r;			
run;

data MDA2019c;
	set MDA2019b;
	mda_no_sent2019_r_s=mda_no_sent2019_r/4;
run;

**merge back to main dataset*;

proc sql;
	create table data_2020f20 
	as select a.*, b.mda_no_sent2019, b.mda_no_sent2019_r, b.mda_no_sent2019_r_s
	from data_2020f17 a left join MDA2019c  b
	on a.gvkey=b.gvkey
	and a.fqtr=b.fqtr
	and a.fqtr ne .
	and b.fqtr ne .
	order by gvkey, datadate;
quit;

proc means data=data_2020f20 n mean;
	var mda_no_sent2019;
	class mda_no_sent2019_r;
run;

proc sort data=data_2020f20 nodupkey;
	by gvkey datadate;
run;

**#of firms**;
proc sort data=data_2020f20 out=numberoffirmscheck nodupkey;
	by gvkey;
run;

/* -------------------------------------------------------------------------- */
/*                       Store primary dataset for Stata                      */
/* -------------------------------------------------------------------------- */

data OUT.covid_data_from_sas; 
	set  data_2020f20;
run;

PROC EXPORT DATA=OUT.covid_data_from_sas
	OUTFILE="%SYSFUNC(DEQUOTE(&OUT_FOLDER.))\covid_data_from_sas.dta"
	DBMS=dta REPLACE;
RUN;

%put WARNING: Stored the primary dataset to the following location: "%SYSFUNC(DEQUOTE(&OUT_FOLDER.))\covid_data_from_sas.dta";

proc means data=OUT.covid_data_from_sas;
	var PremFinalDiff;
	class calendar_qtr;
run;

/* -------------------------------------------------------------------------- */
/*                 Store secondary dataset for appendix tests                 */
/* -------------------------------------------------------------------------- */
* Note: the difference is that the secondary dataset includes 2018;

data data_2020fa5extra;
	set work.beforesampleselection;
	**Restrict to calendar years 2018 to 2022*;
	if Cal_year>2017 & Cal_year<2022;
	if Cal_year=2021 & Cal_month>6 then delete;

run;

%put WARNING: Number of observations report: before: %num_obs(beforesampleselection) || after: %num_obs(data_2020fa5extra);

/* --------------------- Delete firms without sec_delay --------------------- */

data data_2020f4extra;
	set data_2020fa5extra;
	if  sec_delay_final=. then delete;
run;

%put WARNING: Number of observations report: before: %num_obs(data_2020fa5extra) || after: %num_obs(data_2020f4extra);

/* ----------------- Delete firms if filer status is missing ---------------- */

data data_2020f5extra;
	set data_2020f4extra;
	if  HST_IS_ACCEL_FILER=. then delete;
run;

%put WARNING: Number of observations report: before: %num_obs(data_2020f4extra) || after: %num_obs(data_2020f5extra);

/* ------------------- Delete observations with RDQ>FDATE ------------------- */

data data_2020f5aextra;
	set data_2020f5extra;
	if rdq>fdate_final then delete;
run;

%put WARNING: Number of observations report: before: %num_obs(data_2020f5extra) || after: %num_obs(data_2020f5aextra);

/* ------------------------ Delete SPACs observations ----------------------- */

data data_2020f5a1extra;
	set data_2020f5aextra;
	if SPACInd=1 then delete;
run;

%put WARNING: Number of observations report: before: %num_obs(data_2020f5aextra) || after: %num_obs(data_2020f5a1extra);

/* --------------------- Delete firms without ea_delay ---------------------- */

data data_2020f5a2extra;
	set data_2020f5a1extra;
	if Cal_year=2020 and EAdelay_2019_to_2020 =. or EADelay =. then delete;
	if Cal_year=2019 and EAdelay_2020_to_2019 =. or EADelay =. then delete;
	if Cal_year=2021 and (EAdelay_2020_to_2021 =. or EAdelay_2019_to_2021 =. or EADelay =.) then delete;;
run;

%put WARNING: Number of observations report: before: %num_obs(data_2020f5a1extra) || after: %num_obs(data_2020f5a2extra);

proc means data=data_2020f5a2extra n mean;
	var NewConcurrent2020;
	class calendar_qtr;
run;

/* --------------------------- Winsorize variables -------------------------- */

%WT(data=data_2020f5a2extra, out=data_2020f6extra, vars=BM DA ME ROA disc_acc 
GDWLIPQ WDPQ SPIQ WD_s absSPIQ_s absOtherSPI_s RCPQ_s absCh_SALEQ absCh_XOPRQ
nonlinear UE_m2_2_s Predict beta persistence MB Log_ME bhar_d_m1_to_1 mturnover_m12_to_dat mturnoveravg_m12_to_dat
IOR IOR_log Lognumest numest, 
byvar= calendar_qtr , type=W, pctl=1 99);

/* ----------------------------- Store to Stata ----------------------------- */

data OUT.covid_data_from_sas_extra; 
	set  data_2020f6extra;
run;

PROC EXPORT DATA=OUT.covid_data_from_sas_extra
	OUTFILE="%SYSFUNC(DEQUOTE(&OUT_FOLDER.))\covid_data_from_sas_extra.dta"
	DBMS=dta REPLACE;
RUN;

%put WARNING: Stored the secondary dataset to the following location: "%SYSFUNC(DEQUOTE(&OUT_FOLDER.))\covid_data_from_sas_extra.dta";




