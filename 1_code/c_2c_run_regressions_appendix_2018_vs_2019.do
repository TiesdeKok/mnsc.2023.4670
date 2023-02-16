/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/*         EXTRA Dataset for Appendix Test that include 2018                  */
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

clear all
global NAME "c_2c_run_regressions_appendix_2018_vs_2019" 
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

/* ---------------------------- File name globals --------------------------- */

global table_file_ "$pipeline/out/table_"

/* -------------------------- Install dependencies -------------------------- */

cap ssc inst outreg2
cap ssc inst distinct
cap ssc inst reghdfe

/* -------------------------------------------------------------------------- */
/*                         Load Extra dataset from SAS                        */
/* -------------------------------------------------------------------------- */

/* -------------------------------- Load data ------------------------------- */

use "2_pipeline/b_2_process_data/covid_data_from_sas_extra.dta", clear 

/* ------------------------ Convert column data types ----------------------- */

destring gvkey, replace

/* ----------------------------- Define Globals ----------------------------- */

** Cluster by SEC filing date depending on Analysis
global fe_and_cluster_uni_sec "a(gvkey) cluster(fdate_final)" // fe and clustering for univariate regressions 
global fe_and_cluster_multi_sec "a(gvkey sec_filing_limit) cluster(fdate_final)" // fe and clustering for multivariate regressions

** Cluster by EA date depending on Analysis
global fe_and_cluster_uni_ea "a(gvkey) cluster(rdq)" // fe and clustering for univariate regressions 
global fe_and_cluster_multi_ea "a(gvkey sec_filing_limit) cluster(rdq)" // fe and clustering for multivariate regressions

** Outreg options
global outreg_options "excel tstat bdec(3) tdec(2) adjr2 aster(tstat)"

/* ---------------------------- Define Variables ---------------------------- */

**Create Post indicator variable
gen Post2019=0
replace Post2019=1 if Cal_year>2018

gen Untimely_Filer=0
replace Untimely_Filer=1 if TimelyFiler==0

//tabstat Untimely_Filer EADelay, by(calendar_qtr) statistics (count mean)

**Big Change

gen closing_year = year(datadate)       
gen closing_qtr = quarter(datadate)
gen closing_year_qtr = closing_year * 10 + closing_qtr

gen ea_delay_chg = EADelay - EAdelay_2019_to_2020 if closing_year==2020         
replace ea_delay_chg = EADelay - EAdelay_2019_to_2021 if closing_year == 2021  
replace ea_delay_chg = EADelay - EAdelay_2018_to_2019 if closing_year == 2019
replace ea_delay_chg = EADelay - EAdelay_2017_to_2018 if closing_year == 2018

gen big_change = 0 if EADelay != .
replace big_change = 1 if ea_delay_chg > 7 & ea_delay_chg != .
replace big_change = -1 if ea_delay_chg < -7 & ea_delay_chg != .
replace big_change = . if ea_delay_chg==.

gen sec_filing_limit = sec_deadline_final - apdedateq 

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/*             Appendix Table OA.5. ConcurrentReporterTests                   */
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */	

/* -------------------------------------------------------------------------- */
/*                Panel A - Statistics- Mean Concurrent Filers                */
/* -------------------------------------------------------------------------- */

* For the timely sample we require a balance sample where the firm is timely in both the pre-and-post period *
* To guarantee this we create a "in_balanced" indicator which equals one if the firm was timely in both periods *

local years 2018 2019 2020 2021
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

gen group_2018_vs_2019 = (closing_year == 2018) | (closing_year == 2019)
gen group_2018_vs_2020 = (closing_year == 2018) | (closing_year == 2020)
gen group_2018_vs_2021 = (closing_year == 2018) | (closing_year == 2021)
gen group_2019_vs_2020 = (closing_year == 2019) | (closing_year == 2020)
gen group_2019_vs_2021 = (closing_year == 2019) | (closing_year == 2021)
gen group_2020_vs_2021 = (closing_year == 2020) | (closing_year == 2021)

foreach post_year in 2019 2020 2021 {
	forval quarter=1(1)4 {
		// Regular indicators for all filers
		gen group_2018_vs_`post_year'_q`quarter' = (group_2018_vs_`post_year') & (closing_qtr == `quarter')
		
		// Indicators for filers who are timely accross both periods
		gen group_2018_vs_`post_year'_q`quarter'_timely = (group_2018_vs_`post_year')  /// 
														& (closing_qtr == `quarter') ///
														& (timely_in_2018 == 1) ///
														& (timely_in_`post_year' == 1)
	}
}

**Panel A of OA.5- Year-over-year change from 2018**

gen Ind10K=0
replace Ind10K=1 if form_final=="10-K"

tabstat Concurrent if (group_2018_vs_2019_q1_timely==1 | group_2018_vs_2019_q2_timely==1 | group_2018_vs_2019_q3_timely==1 | group_2018_vs_2019_q4_timely==1) & (closing_year == 2018), by(calendar_qtr)

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/*                     Appendix Table OA.7 – 2019 vs. 2018                    */
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

**Restrict to 2018 vs. 2019

drop if Cal_year > 2019
drop if Cal_month < 10
//tabstat EADelay, by(calendar_qtr) statistics (count mean)

/* -------------------------------------------------------------------------- */
/*                            Panel A - Late Filers                           */
/* -------------------------------------------------------------------------- */

local table_label "OA.7_a"
local append "replace"

foreach dv in Untimely_Filer {
    reghdfe `dv' Post2019  , $fe_and_cluster_multi_sec
    outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv'") $outreg_options
    local append "append"
}		

/* -------------------------------------------------------------------------- */
/*                             Panel B - SEC Delay                            */
/* -------------------------------------------------------------------------- */

local table_label "OA.7_b"
local append "replace" 
foreach dv in sec_delay_final  {
    reghdfe `dv' Post2019  if Untimely_Filer==0, $fe_and_cluster_multi_sec
    outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv'") $outreg_options
    local append "append"
}
 
/* -------------------------------------------------------------------------- */
/*                          Panel C - EA Large Change                         */
/* -------------------------------------------------------------------------- */

local table_label "OA.7_c"
local append "replace"  
foreach dv in big_change  {
    reghdfe `dv' Post2019  if Untimely_Filer==0, $fe_and_cluster_multi_ea
    outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv'") $outreg_options
    local append "append"
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/*                                  Clean up                                  */
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

sleep 2000

/* ---------------------------- Remove text files --------------------------- */

local txtfiles: dir "$pipeline/out" files "*.txt"
foreach txt in `txtfiles' {
    erase `"$pipeline/out/`txt'"'
}

/* ---------------------------- Remove TMP files ---------------------------- */

local tmpfiles: dir "$pipeline/out" files "*.tmp"
foreach txt in `tmpfiles' {
    erase `"$pipeline/out/`txt'"'
}

/* ------------------------------  Close Log  ------------------------------- */

log close
	
********************************************************************************
********************************************************************************