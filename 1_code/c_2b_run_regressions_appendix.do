/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/*                                  Preamble                                  */
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

clear all
global NAME "c_2b_run_regressions_appendix" 
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

cap ssc inst outreg2
cap ssc inst distinct
cap ssc inst reghdfe

/* -------------------------------------------------------------------------- */
/*                                  Load data                                 */
/* -------------------------------------------------------------------------- */

use "2_pipeline/c_1_generate_variables/out/regression_data.dta", clear

/* -------------------------------------------------------------------------- */
/*                                 Set globals                                */
/* -------------------------------------------------------------------------- */

/* --------------------------- Regression globals --------------------------- */

global controls "GWImp_Ind WDPQ_Ind RCPQ_Ind OtherSPI_Ind BigChangeSales BigChangeExp"
global controls_scaled "GWImp_s WD_s RCPQ_s absOtherSPI_s absCh_SALEQ absCh_XOPRQ"

global controls_ERC "c_Predictb c_betab c_persistenceb c_bmb c_Log_MEb c_LossEPSb c_rep_lagb c_Lognumestb c_IORb c_mturnoveravg_m12_to_datb "
global controls_ERC_interactions "c_UE_c_Predictb c_UE_c_betab c_UE_c_persistenceb c_UE_c_bmb c_UE_c_Log_MEb c_UE_c_LossEPSb c_UE_c_rep_lagb c_UE_c_Lognumestb c_UE_c_IORb c_UE_c_mturnoveravg_m12_to_datb "

global auditorvars "ME_r_s choi_ofsize1_r_s choi_ofsize2_r_s ln_audit_fees_r_s total_audit_fees_r_s num_clients_r_s is_big_4 choi_large_office_by_fees choi_large_office_by_clients beck_large_office beck_ln_office_size_r_s ege_ln_office_size_r_s auditor_tenure_r_s"


global big_change_var big_change

** Cluster by SEC filing date depending on Analysis**;
global fe_and_cluster_uni_sec "a(gvkey) cluster(fdate_final)" // fe and clustering for univariate regressions 
global fe_and_cluster_multi_sec "a(gvkey sec_filing_limit) cluster(fdate_final)" // fe and clustering for multivariate regressions
global nofe_and_cluster_uni_sec "a(one) cluster(fdate_final)" // nofe and clustering to check economic magnitude regressions 
global nofe_and_cluster_multi_sec "a(one) cluster(fdate_final)" // nofe and clustering to check economic magnitude regressions 

** Cluster by EA date depending on Analysis**;
global fe_and_cluster_uni_ea "a(gvkey) cluster(rdq)" // fe and clustering for univariate regressions 
global fe_and_cluster_multi_ea "a(gvkey sec_filing_limit) cluster(rdq)" // fe and clustering for multivariate regressions
global nofe_and_cluster_uni_ea "a(one) cluster(rdq)" // nofe and clustering for multivariate regressions
global nofe_and_cluster_multi_ea "a(one) cluster(rdq)" // nofe and clustering for multivariate regressions

* Table globals - Samples*

global col_groups_all "group_2019_vs_2020_q1 group_2019_vs_2020_q2 group_2019_vs_2020_q3 group_2019_vs_2020_q4 group_2019_vs_2021_q1 group_2019_vs_2021_q2"
global col_groups_timely ""
foreach col_group in $col_groups_all {
	global col_groups_timely "$col_groups_timely `col_group'_timely"  
}

global col_groups_timely2021 "group_2019_vs_2021_q1_timely group_2019_vs_2021_q2_timely"
global col_groups_timely2020 "group_2019_vs_2020_q1_timely group_2019_vs_2020_q2_timely group_2019_vs_2020_q3_timely group_2019_vs_2020_q4_timely"
global col_groups_timely2019 "group_2019_vs_2020_q1_timely group_2019_vs_2020_q2_timely group_2019_vs_2020_q3_timely group_2019_vs_2020_q4_timely"

global col_groups_restatement_all "group_2019_vs_2020_q1 group_2019_vs_2020_q2 group_2019_vs_2020_q3 group_2019_vs_2020_q4"
global col_groups_restatement_timely "group_2019_vs_2020_q1_timely group_2019_vs_2020_q2_timely group_2019_vs_2020_q3_timely group_2019_vs_2020_q4_timely"

global col_groups_all_formtype "group_2019_vs_2020_q1 group_2019_vs_2020_q2 group_2019_vs_2020_q3 group_2019_vs_2020_q4"
global col_groups_timely_formtype "group_2019_vs_2020_q1_timely group_2019_vs_2020_q2_timely group_2019_vs_2020_q3_timely group_2019_vs_2020_q4_timely"

/* ----------------------------- Outreg options ----------------------------- */

global outreg_options "excel tstat bdec(3) tdec(2) adjr2 aster(tstat)"

/* ---------------------------- File name globals --------------------------- */

global table_file_ "$pipeline/out/table_"

// Important - You need to always run the code above before you can run any of the code below. 



/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/*                   OA.1 – FRQ by Cross-sectional Regressions                */
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------- */
/* --------------------------- By LateFilers--------------------------------- */
/* -------------------------------------------------------------------------- */

/* ------------------------------- Firm FE  --------------------------------- */
	
local table_label "OA.1_a"
local append "append"
foreach dv in TotalRestatement_App {
    foreach group in $col_groups_restatement_all  {
        reghdfe `dv' Post Untimely_Filer Post_Untimely_Filer  if `group'==1 , $fe_and_cluster_uni_sec
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options
        local append "append"
    }
}

local table_label "OA.1_a"
local append "append"
foreach dv in PremFinalDiff {
    foreach group in $col_groups_restatement_all  {
        reghdfe `dv' Post Untimely_Filer Post_Untimely_Filer  if `group'==1 & Concurrent==0, $fe_and_cluster_uni_ea
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options
        local append "append"
    }
}

/* ----------------------- Firm & Deadline FE  ------------------------------ */


	
local table_label "OA.1_b"
local append "replace"
foreach dv in TotalRestatement_App {
    foreach group in $col_groups_restatement_all  {
        reghdfe `dv' Post Untimely_Filer Post_Untimely_Filer  if `group'==1 , $fe_and_cluster_multi_sec
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options
        local append "append"
    }
}

local table_label "OA.1_b"
local append "append"
foreach dv in PremFinalDiff {
    foreach group in $col_groups_restatement_all  {
        reghdfe `dv' Post Untimely_Filer Post_Untimely_Filer  if `group'==1 & Concurrent==0, $fe_and_cluster_multi_ea
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options
        local append "append"
    }
}


/* -------------------------------------------------------------------------- */
/* --------------------------- By Filing Lag--------------------------------- */
/* -------------------------------------------------------------------------- */

/* --------------------------------- Firm FE -------------------------------- */

local table_label "OA.1_c"
local append "replace"
foreach dv in TotalRestatement_App {
    foreach group in $col_groups_restatement_timely  {
        use "2_pipeline/c_1_generate_variables/out/regression_data.dta", clear
        keep if `group'==1
        gen Post_sec_delay_final=Post*sec_delay_final
        reghdfe `dv' Post sec_delay_final Post_sec_delay_final ,$fe_and_cluster_uni_sec  
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options
        local append "append"
        use "2_pipeline/c_1_generate_variables/out/regression_data.dta", clear
    }
}

local table_label "OA.1_c"
local append "append"
foreach dv in PremFinalDiff {
    foreach group in $col_groups_restatement_timely  {
        use "2_pipeline/c_1_generate_variables/out/regression_data.dta", clear
        keep if `group'==1
        gen Post_sec_delay_final=Post*sec_delay_final
        reghdfe `dv' Post sec_delay_final Post_sec_delay_final if Concurrent==0,$fe_and_cluster_uni_ea  
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options
        local append "append"
        use "2_pipeline/c_1_generate_variables/out/regression_data.dta", clear
    }
}


/* --------------------------- Firm & Deadline FE --------------------------- */

local table_label "OA.1_d"
local append "replace"
foreach dv in TotalRestatement_App {
    foreach group in $col_groups_restatement_timely  {
        use "2_pipeline/c_1_generate_variables/out/regression_data.dta", clear
        keep if `group'==1
        gen Post_sec_delay_final=Post*sec_delay_final
        reghdfe `dv' Post sec_delay_final Post_sec_delay_final ,$fe_and_cluster_multi_sec  
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options
        local append "append"
        use "2_pipeline/c_1_generate_variables/out/regression_data.dta", clear
    }
}

local table_label "OA.1_d"
local append "append"
foreach dv in PremFinalDiff {
    foreach group in $col_groups_restatement_timely  {
        use "2_pipeline/c_1_generate_variables/out/regression_data.dta", clear
        keep if `group'==1
        gen Post_sec_delay_final=Post*sec_delay_final
        reghdfe `dv' Post sec_delay_final Post_sec_delay_final if Concurrent==0,$fe_and_cluster_multi_ea  
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options
        local append "append"
        use "2_pipeline/c_1_generate_variables/out/regression_data.dta", clear
    }
}


/* -------------------------------------------------------------------------- */
/* --------------------------- By Large Lags--------------------------------- */
/* -------------------------------------------------------------------------- */

/* --------------------------------- Firm FE -------------------------------- */

local table_label "OA.1_e"
local append "replace"
foreach dv in TotalRestatement_App {
    foreach group in $col_groups_restatement_timely  {
        reghdfe `dv' Post big_change Post_big_change  if `group'==1 , $fe_and_cluster_uni_sec
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options
        local append "append"
    }
}	


local table_label "OA.1_e"
local append "append"
foreach dv in PremFinalDiff {
    foreach group in $col_groups_restatement_timely  {
        reghdfe `dv' Post big_change Post_big_change  if `group'==1 & Concurrent==0 , $fe_and_cluster_uni_ea
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options
        local append "append"
    }
}


/* --------------------------- Firm & Deadline FE --------------------------- */

local table_label "OA.1_f"
local append "replace"
foreach dv in TotalRestatement_App {
    foreach group in $col_groups_restatement_timely  {
        reghdfe `dv' Post big_change Post_big_change  if `group'==1 , $fe_and_cluster_multi_sec
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options
        local append "append"
    }
}	


local table_label "OA.1_f"
local append "append"
foreach dv in PremFinalDiff {
    foreach group in $col_groups_restatement_timely  {
        reghdfe `dv' Post big_change Post_big_change  if `group'==1 & Concurrent==0, $fe_and_cluster_multi_ea
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options
        local append "append"
    }
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/*          Appendix Table OA.2 - ERC Test Variable Definitions               */
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

**See Table in Appendix for definitions.



/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/*                      Appendix Table OA.3 - 10K vs. 10Q                     */
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------- */
/*                     Panel A - Statistics- 10Ks vs 10Qs                     */
/* -------------------------------------------------------------------------- */

tabstat EADelay , by(calendar_qtr) statistics (count)

tabstat EADelay if Ind10K==1, by(calendar_qtr) statistics (count)

tabstat EADelay if Ind10K==0, by(calendar_qtr) statistics (count)

/* -------------------------------------------------------------------------- */
/*                      Coordination Costs Between 10K/Qs                     */
/* -------------------------------------------------------------------------- */

/* ---------------- Panel B - Late Filers & EALargeLag ---------------------- */

local table_label "OA.3_b"
local append "replace"
foreach dv in Untimely_Filer {
    foreach group in $col_groups_all_formtype {
        reghdfe `dv'  Post10Q Post10K   if `group'==1, $fe_and_cluster_multi_sec
        lincom Post10K - Post10Q
        local coef=r(estimate)
        local se=r(se)
        local t `=`coef'/`se''
        local p=r(p)
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options addstat("estimate", `coef', "standard error", `se', "t statistic", `t', "p-val", `p' )
        local append "append"
    }
}

local table_label "OA.3_b"
local append "append"
foreach dv in "big_change" {
    foreach group in $col_groups_timely_formtype  {
        reghdfe `dv' Post10Q Post10K   if `group'==1, $fe_and_cluster_multi_ea
        lincom Post10K - Post10Q
        local coef=r(estimate)
        local se=r(se)
        local t `=`coef'/`se'' 
        local p=r(p)
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options addstat("estimate", `coef', "standard error", `se', "t statistic", `t', "p-val", `p' )
        local append "append"
    }
}



/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/*                  Appendix Table OA.4 - Management Guidance                 */
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------- */
/*                            Panel A - Late Filers                           */
/* -------------------------------------------------------------------------- */

local table_label "OA.4_a"
local append "replace"

/* ------------------------------ 2019 vs. 2020 ----------------------------- */

foreach q in 1 2 3 4 {
    reghdfe Untimely_Filer Post_AllOthers2019vs2020 Post_Stop2019vs2020  if group_2019_vs_2020_q`q'==1 , $fe_and_cluster_multi_sec nocon
    lincom Post_Stop2019vs2020-Post_AllOthers2019vs2020 
    local coef=r(estimate)
    local se=r(se)
    local t `=`coef'/`se''
    local p=r(p)
    outreg2 using  "${table_file_}`table_label'.xls", `append' $outreg_options addstat("estimate", `coef', "standard error", `se', "t statistic", `t', "p-val", `p' )
    local append "append"
}

/* ------------------------------ 2019 vs. 2021 ----------------------------- */

foreach q in 1 2 {
    reghdfe Untimely_Filer Post_AllOthers2019vs2021 Post_Stop2019vs2021  if group_2019_vs_2021_q`q'==1 , $fe_and_cluster_multi_sec nocon
    lincom Post_Stop2019vs2021-Post_AllOthers2019vs2021
    local coef=r(estimate)
    local se=r(se)
    local t `=`coef'/`se''
    local p=r(p)
    outreg2 using  "${table_file_}`table_label'.xls", `append' $outreg_options addstat("estimate", `coef', "standard error", `se', "t statistic", `t', "p-val", `p' )
    local append "append"
}

/* -------------------------------------------------------------------------- */
/*                             Panel B - SEC Delay                            */
/* -------------------------------------------------------------------------- */
	
local table_label "OA.4_b"
local append "replace"

/* ------------------------------ 2019 vs. 2020 ----------------------------- */

foreach q in 1 2 3 4 {
    reghdfe sec_delay_final Post_AllOthers2019vs2020 Post_Stop2019vs2020  if group_2019_vs_2020_q`q'_timely==1 , $fe_and_cluster_multi_sec nocon 
    lincom Post_Stop2019vs2020-Post_AllOthers2019vs2020
    local coef=r(estimate)
    local se=r(se)
    local t `=`coef'/`se''
    local p=r(p)
    outreg2 using  "${table_file_}`table_label'.xls", `append' $outreg_options addstat("estimate", `coef', "standard error", `se', "t statistic", `t', "p-val", `p' )
    local append "append"
}

/* ------------------------------ 2019 vs. 2021 ----------------------------- */

foreach q in 1 2 {
    outreg2 using  "${table_file_}`table_label'.xls", `append' $outreg_options addstat("estimate", `coef', "standard error", `se', "t statistic", `t', "p-val", `p' )
    local append "append"
    reghdfe sec_delay_final Post_AllOthers2019vs2021 Post_Stop2019vs2021  if group_2019_vs_2021_q`q'_timely==1 , $fe_and_cluster_multi_sec nocon
    lincom Post_Stop2019vs2021-Post_AllOthers2019vs2021
    local coef=r(estimate)
    local se=r(se)
    local t `=`coef'/`se''
    local p=r(p)
    outreg2 using  "${table_file_}`table_label'.xls", `append' $outreg_options addstat("estimate", `coef', "standard error", `se', "t statistic", `t', "p-val", `p' )
    local append "append"
}

/* -------------------------------------------------------------------------- */
/*                          Panel C - EA Large Change                         */
/* -------------------------------------------------------------------------- */

local table_label "OA.4_c"
local append "replace"

/* ------------------------------ 2019 vs. 2020 ----------------------------- */

foreach q in 1 2 3 4 {
    reghdfe $big_change_var Post_AllOthers2019vs2020 Post_Stop2019vs2020  if group_2019_vs_2020_q`q'_timely==1 , $fe_and_cluster_multi_ea nocon 
    lincom Post_Stop2019vs2020-Post_AllOthers2019vs2020
    local coef=r(estimate)
    local se=r(se)
    local t `=`coef'/`se''
    local p=r(p)
    outreg2 using  "${table_file_}`table_label'.xls", `append' $outreg_options addstat("estimate", `coef', "standard error", `se', "t statistic", `t', "p-val", `p' )
    local append "append"
}

/* ------------------------------ 2019 vs. 2021 ----------------------------- */

local table_label "OA.4_c"
local append "append"

foreach q in 1 2 {
    outreg2 using  "${table_file_}`table_label'.xls", `append' $outreg_options addstat("estimate", `coef', "standard error", `se', "t statistic", `t', "p-val", `p' )
    local append "append"
    reghdfe $big_change_var Post_AllOthers2019vs2021 Post_Stop2019vs2021  if group_2019_vs_2021_q`q'_timely==1 , $fe_and_cluster_multi_ea nocon
    lincom Post_Stop2019vs2021-Post_AllOthers2019vs2021
    local coef=r(estimate)
    local se=r(se)
    local t `=`coef'/`se''
    local p=r(p)
    outreg2 using  "${table_file_}`table_label'.xls", `append' $outreg_options addstat("estimate", `coef', "standard error", `se', "t statistic", `t', "p-val", `p' )
    local append "append"
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/*             Appendix Table OA.5 – Concurrent Report Tests                  */
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/* -------------------------- Panel A - Averages ---------------------------- */

tabstat Concurrent if (group_2019_vs_2020_q1_timely==1 | group_2019_vs_2020_q2_timely==1 | group_2019_vs_2020_q3_timely==1 | group_2019_vs_2020_q4_timely==1 | group_2019_vs_2021_q1_timely==1 | group_2019_vs_2021_q2_timely==1), by(calendar_qtr)

**To get year-over-year changes for 2019 please go to separate Stata file "c_2c_2018_vs_2019_Appendix_ConcurrentReportTests"**


/* -----------------------Panel B - Firm & Deadline FE ---------------------- */

local table_label "OA.5_b"
local append "replace"
foreach dv in Concurrent {
    foreach group in $col_groups_timely  {
        reghdfe `dv' Post  if `group'==1, $fe_and_cluster_multi_sec
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options
        local append "append"
    }
}

/* ------Panel C - Firm & Deadline FE Dropping NewConcurrent2020 or 2021 ---- */

local table_label "OA.5_c"
local append "replace"
foreach dv in "big_change" {
    foreach group in $col_groups_timely  {
        reghdfe `dv' Post  if `group'==1 & NewConcurrentFinal2020or2021==0, $fe_and_cluster_multi_ea
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options
        local append "append"
    }
}


/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/*             Appendix Table OA.6 - California Case Study                    */
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------- */
/*                            Panel A - Late Filers                           */
/* -------------------------------------------------------------------------- */

local table_label "OA.6_a"
local append "replace"
foreach dv in Untimely_Filer   {
    foreach group in $col_groups_all {
        reghdfe `dv' PostAllExceptCali PostCali if `group'==1, $fe_and_cluster_multi_sec
        lincom PostCali-PostAllExceptCali
        local coef=r(estimate)
        local se=r(se)
        local t `=`coef'/`se''
        local p=r(p)
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options addstat("estimate", `coef', "standard error", `se', "t statistic", `t', "p-val", `p' )
        local append "append"
    }
}

/* -------------------------------------------------------------------------- */
/*                             Panel B - SEC delay                            */
/* -------------------------------------------------------------------------- */

local table_label "OA.6_b"
local append "replace"
foreach dv in sec_delay_final   {
    foreach group in $col_groups_timely  {
        reghdfe `dv' PostAllExceptCali PostCali if `group'==1, $fe_and_cluster_multi_sec
        lincom PostCali-PostAllExceptCali 
        local coef=r(estimate)
        local se=r(se)
        local t `=`coef'/`se''
        local p=r(p)
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options addstat("estimate", `coef', "standard error", `se', "t statistic", `t', "p-val", `p' )
        local append "append"
    }
}

/* -------------------------------------------------------------------------- */
/*                          Panel C - EA Large Change                         */
/* -------------------------------------------------------------------------- */

local table_label "OA.6_c"
local append "replace"
foreach dv in "$big_change_var"  {
    foreach group in $col_groups_timely  {
        reghdfe `dv' PostAllExceptCali PostCali if `group'==1 , $fe_and_cluster_multi_ea
        lincom PostCali - PostAllExceptCali
        local coef=r(estimate)
        local se=r(se)
        local t `=`coef'/`se''
        local p=r(p)
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options addstat("estimate", `coef', "standard error", `se', "t statistic", `t', "p-val", `p' )
        local append "append"
    }
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/*             Appendix Table OA.7 - Q4-2019 vs. Q4-2018                      */
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

** Code located in seperate file: `c_2c_2018_vs_2019_Appendix.do`




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