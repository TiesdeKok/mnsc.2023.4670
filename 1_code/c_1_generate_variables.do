/* -------------------------------------------------------------------------- */
/*                                  Preamble                                  */
/* -------------------------------------------------------------------------- */

clear all
global NAME "c_1_generate_variables" 
global STATA_VERSION "17.0"

/* -------------------- Set working directory dynamically ------------------- */

/* IMPORTANT: Add / Change your project working directory here */

cap cd "E:\Dropbox\Work\Research\COVID and EA Timing\covid-empirical"
cap cd "W:\covid_timeliness_project\covid-empirical" 

global PROJECT_DIR "`pwd'"

pwd

/* ------------------------------ Start logging ------------------------------*/

local log_path "3_output\logs\\$NAME.smcl"
log using `log_path', replace

/* ------------------- Create pipeline folder for .do file ------------------ */

capture mkdir "2_pipeline"
global pipeline "2_pipeline/$NAME"

capture mkdir "$pipeline"
if _rc == 0 {
	foreach folder in "out" "store" "tmp" {
		capture mkdir "$pipeline/`folder'"
	}
} 

/* -------------------------- Install dependencies -------------------------- */

cap ssc inst egenmore

/* -------------------------------------------------------------------------- */
/*                       Load and process data from SAS                       */
/* -------------------------------------------------------------------------- */

/* -------------------------------- Load data ------------------------------- */

use "2_pipeline/b_2_process_data/covid_data_from_sas.dta", clear // Don't change this, only change the absolute path at the top.
 
/* ------------------------ Convert column data types ----------------------- */

destring gvkey, replace

/* ------------------ Check rows do not have missing values ----------------- */

//count
drop if sec_delay_final==.
//count
drop if HST_IS_ACCEL_FILER==.

/* -------------------------------------------------------------------------- */
/*                          Create general variables                          */
/* -------------------------------------------------------------------------- */

/* ---------------------------- Utility variables --------------------------- */

gen one=1

gen Untimely_Filer=0
replace Untimely_Filer=1 if TimelyFiler==0

gen Post=0
replace Post=1 if Cal_year>2019

gen cyear = year(datadate)
gen cqtr = quarter(datadate)
gen cyq = cyear * 10 + cqtr
gen postqs = cyq
replace postqs = 0 if cyear == 2019

/* --------------------------- SEC Delay variables -------------------------- */

gen sec_date = fdate_final             
gen sec_date_year = year(sec_date)
gen sec_date_week = week(sec_date)
gen sec_date_year_week = sec_date_year * 100 + sec_date_week
gen sec_date_weekday = dow(sec_date)
gen sec_date_week_weekday = sec_date_week * 10 + sec_date_weekday
 
/* ------------------------- Closing Date variables ------------------------- */

gen closing_year = year(datadate)       
gen closing_qtr = quarter(datadate)
gen closing_year_qtr = closing_year * 10 + closing_qtr

bys gvkey: egen tmp = max(closing_year_qtr) if closing_year == 2019
bys gvkey: egen closing_year_qtr_max = max(tmp)
drop tmp
gen fpe_dow = dow(apdedateq)

/* Calculate number of days between closing date and standard SEC filing date */

gen sec_filing_limit = sec_deadline_final - apdedateq  

* For the timely sample we require a balance sample where the firm is timely in both the pre-and-post period *
* To guarantee this we create a "in_balanced" indicator which equals one if the firm was timely in both periods *

local years 2019 2020 2021
local search_window : list sizeof local(years)
local search_window=`search_window'-1
display `search_window'

sort gvkey closing_qtr closing_year
foreach year in `years' {
	gen timely_in_`year' = 0
	forval i=0(1)`search_window' {
		display "`year' - `i'"
		// Search in previous records
		by gvkey closing_qtr (closing_year): replace timely_in_`year' = 1 if `year'==closing_year[_n-`i'] & TimelyFiler[_n-`i'] == 1 
		// Search in later records
		by gvkey closing_qtr (closing_year): replace timely_in_`year' = 1 if `year'==closing_year[_n+`i'] & TimelyFiler[_n+`i'] == 1 
	}
}
sort gvkey datadate // Restoring sort to regular panel data style just in case


/* ----------- Create indicators for the primary regression groups ---------- */

gen group_2019_vs_2020 = (closing_year == 2019) | (closing_year == 2020)
gen group_2019_vs_2021 = (closing_year == 2019) | (closing_year == 2021)
gen group_2020_vs_2021 = (closing_year == 2020) | (closing_year == 2021)

foreach post_year in 2020 2021 {
	forval quarter=1(1)4 {
		// Regular indicators for all filers
		gen group_2019_vs_`post_year'_q`quarter' = (group_2019_vs_`post_year') & (closing_qtr == `quarter')
		
		// Indicators for filers who are timely accross both periods
		gen group_2019_vs_`post_year'_q`quarter'_timely = (group_2019_vs_`post_year')  /// 
														& (closing_qtr == `quarter') ///
														& (timely_in_2019 == 1) ///
														& (timely_in_`post_year' == 1)
	}
}

/* ------------------- Create summary stats table variable ------------------ */

gen sum_stats_groups = closing_year_qtr if closing_year >= 2020
replace sum_stats_groups = 2019 if closing_year == 2019


/* -------- Create summary stats table variable for FRQ tests --------------- */

gen sum_stats_groups_FRQ = closing_year_qtr if closing_year == 2020
replace sum_stats_groups_FRQ = 2019 if closing_year == 2019

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/*                          Table variables                                   */
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/* ------------------ Earnings Announcement (RDQ) variables ----------------- */

gen ea_year = year(rdq)
gen ea_week = week(rdq)
gen ea_year_week = ea_year * 100 + ea_week
gen ea_weekday = dow(rdq)

/* ------------------------ Calculate change in delay ----------------------- */
 
gen ea_delay_chg = EADelay - EAdelay_2019_to_2020 if closing_year==2020         
replace ea_delay_chg = EADelay - EAdelay_2019_to_2021 if closing_year == 2021  
replace ea_delay_chg = EADelay - EAdelay_2018_to_2019 if closing_year == 2019

/* ----------Create Big changes in EA delays variable (EALargeChg)----------- */

gen big_change = 0 if EADelay != .
replace big_change = 1 if ea_delay_chg > 7 & ea_delay_chg != .
replace big_change = -1 if ea_delay_chg < -7 & ea_delay_chg != .
replace big_change = . if ea_delay_chg==.

/* ------------- Create Big changes split variable for T-tests -------------- */

gen EALargeLag_1_Descriptive=0
replace EALargeLag_1_Descriptive=1 if big_change==1
replace EALargeLag_1_Descriptive=. if big_change==.

/* ------------------ Create SEC and Big change variables ------------------- */


gen sec_delay_chg = sec_delay_final - sec_delay_final_2019_to_2020 if closing_year==2020         
replace sec_delay_chg = sec_delay_final - sec_delay_final_2019_to_2021 if closing_year == 2021  

gen filing_chg = sec_delay_final - sec_delay_final_2019_to_2020 if closing_year==2020         
replace filing_chg = sec_delay_final - sec_delay_final_2019_to_2021 if closing_year == 2021  
replace filing_chg = sec_delay_final - sec_delay_final_2018_to_2019 if closing_year == 2019

gen big_change_sec = 0 if sec_delay_final != .
replace big_change_sec = 1 if filing_chg > 7 & filing_chg != .
replace big_change_sec = -1 if filing_chg < -7 & filing_chg != .
replace big_change_sec = . if filing_chg==.


/* ------------- Calculate difference between EA and Filing date ------------ */

gen diffEAandFiling=fdate_final-rdq

/* -------------------------------------------------------------------------- */
/*                        Management Guidance variables                       */
/* -------------------------------------------------------------------------- */

/* ---- Indicator Post-StoppedGuidance and Post-All Others 2020 vs. 2019 ---- */

gen Post_Stop2019vs2020=0
replace Post_Stop2019vs2020=1 if StopGuide2019vs2020==1 & Post==1
gen Post_AllOthers2019vs2020=0
replace Post_AllOthers2019vs2020=1 if StopGuide2019vs2020==0 & Post==1

/* ---- Indicator Post-StoppedGuidance and Post-All Others 2021 vs. 2019 ---- */

gen Post_Stop2019vs2021=0
replace Post_Stop2019vs2021=1 if StopGuide2019vs2021==1 & Post==1
gen Post_AllOthers2019vs2021=0
replace Post_AllOthers2019vs2021=1 if StopGuide2019vs2021==0 & Post==1

/* -------------------------------------------------------------------------- */
/*                          California variables                              */
/* -------------------------------------------------------------------------- */

/* ----------------------- Indicator for California ------------------------- */

gen Cali=0
replace Cali=1 if State2=="CA"

/* ----- Indicator Variable for Post Cali vs. Post All Others --------------- */

gen PostCali=0
replace PostCali=1 if Cali==1 & Post==1

gen PostAllExceptCali=0
replace PostAllExceptCali=1 if Cali==0 & Post==1

/* -------------------------------------------------------------------------- */
/*              Create Form Type (10Q vs. 10Q) indicators                     */
/* -------------------------------------------------------------------------- */

/* ----------------------- Indicator for FormType---------------------------- */

gen Ind10K=0
replace Ind10K=1 if form_final=="10-K"

/* ---------- Indicator Variable for Post 10K vs. Post All Others ----------- */

gen Post10K=0
replace Post10K=1 if Ind10K==1 & Post==1

gen Post10Q=0
replace Post10Q=1 if Ind10K==0 & Post==1


* --------------------------------------------------------------------------- */
/*                         Concurrent filer variables                         */
/* -------------------------------------------------------------------------- */

//tabstat NewConcurrentFinal, by(calendar_qtr)

gen NewConcurrentFinal2020or2021=0
replace NewConcurrentFinal2020or2021=1 if (Cal_year==2020 & NewConcurrentFinal==1) | (Cal_year==2021 & NewConcurrentFinal==1) 

//tabstat NewConcurrentFinal2020or2021 NewConcurrentFinal, by(calendar_qtr)

/* --------- Statistics for Mantain Concurrent------------------------------- */
 
gen MantainConcurrent=0
replace MantainConcurrent=1 if Cal_year==2021 & lag4NewConcurrentFinal==1 & Concurrent==1

//tabstat lag4NewConcurrentFinal MantainConcurrent if group_2019_vs_2021_q1_timely==1 | group_2019_vs_2021_q2_timely==1, by(calendar_qtr)


/* -------------------------------------------------------------------------- */
/*                           Variables for Restatements                       */
/* -------------------------------------------------------------------------- */

gen Post_big_change=Post*big_change
gen Post_Untimely_Filer=Post*Untimely_Filer

/* -------------------------------------------------------------------------- */
/*                            Variables for t-tests                           */
/* -------------------------------------------------------------------------- */

egen choi_ofsize2_quintile = xtile(choi_ofsize2), n(5) by(closing_year_qtr)

gen mve = cshoq * prccq 
egen mve_decile_ttests = xtile(mve), n(10) by(closing_year_qtr)


/* -------------------------------------------------------------------------- */
/*                            Statistics                                      */
/* -------------------------------------------------------------------------- */

//tabstat StopGuide2019vs2020, statistics (mean count)
//tabstat StopGuide2019vs2021, statistics (mean count)

/* ------------------ Statistics for Paper SEC Filing limit ----------------- */

//tabstat sec_filing_limit if group_2019_vs_2020_q1 ==1, by(calendar_qtr) 
//tabstat sec_filing_limit if group_2019_vs_2020_q2 ==1, by(calendar_qtr) 
//tabstat sec_filing_limit if group_2019_vs_2020_q3 ==1, by(calendar_qtr) 
//tabstat sec_filing_limit if group_2019_vs_2020_q4 ==1, by(calendar_qtr) 
//tabstat sec_filing_limit if group_2019_vs_2021_q1 ==1, by(calendar_qtr) 
//tabstat sec_filing_limit if group_2019_vs_2021_q2 ==1, by(calendar_qtr) 


//tabstat sec_filing_limit if group_2019_vs_2020_q1_timely ==1, by(calendar_qtr) 
//tabstat sec_filing_limit if group_2019_vs_2020_q2_timely ==1, by(calendar_qtr) 
//tabstat sec_filing_limit if group_2019_vs_2020_q3_timely ==1, by(calendar_qtr) 
//tabstat sec_filing_limit if group_2019_vs_2020_q4_timely ==1, by(calendar_qtr) 
//tabstat sec_filing_limit if group_2019_vs_2021_q1_timely ==1, by(calendar_qtr) 
//tabstat sec_filing_limit if group_2019_vs_2021_q2_timely ==1 , by(calendar_qtr) 

/* ----------------------- Waiting to file by deadline ---------------------- */

gen DiffDeadlineFilingDate=sec_deadline_final2-fdate_final

gen FilebyDeadline=0
replace FilebyDeadline=1 if DiffDeadlineFilingDate==0 | DiffDeadlineFilingDate==1

//tabstat FilebyDeadline if Cal_year==2019

/* ------------------------------ Big_EA_change ----------------------------- */

gen big_change_nonzero=0
replace big_change_nonzero=1 if big_change==1
replace big_change_nonzero=1 if big_change==-1
replace big_change_nonzero=. if big_change==.

//tabstat big_change_nonzero if Untimely_Filer ==0

gen big_change_nonzeropos=0
replace big_change_nonzeropos=1 if big_change==1
replace big_change_nonzeropos=. if big_change==.
//tabstat big_change_nonzeropos

gen big_change_nonzeroneg=0
replace big_change_nonzeroneg=1 if big_change==-1
replace big_change_nonzeroneg=. if big_change==.
//tabstat big_change_nonzeroneg

/* ----------------------------- COVID Extension ---------------------------- */

gen ExtensionandLate=0
replace ExtensionandLate=1 if COVID_Extension2==1 & LateFiler==1

//tabstat COVID_Extension2 LateFiler ExtensionandLate, by(calendar_qtr)

/* -------------------------------------------------------------------------- */
/*                              Store the dataset                             */
/* -------------------------------------------------------------------------- */

save "$pipeline/out/regression_data.dta", replace

/* ------------------------------  Close Log  ------------------------------- */

log close
	
********************************************************************************
********************************************************************************