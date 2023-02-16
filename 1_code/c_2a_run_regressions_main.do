/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/*                                  Preamble                                  */
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

clear all
global NAME "c_2a_run_regressions_main" 
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

/* ----------------------- Create frame for all filers ---------------------- */

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

** Cluster by SEC filing date depending on Analysis
global fe_and_cluster_uni_sec "a(gvkey) cluster(fdate_final)" // fe and clustering for univariate regressions 
global fe_and_cluster_multi_sec "a(gvkey sec_filing_limit) cluster(fdate_final)" // fe and clustering for multivariate regressions
global nofe_and_cluster_multi_sec "a(one) cluster(fdate_final)" // nofe and clustering to check economic magnitude regressions 

** Cluster by EA date depending on Analysis
global fe_and_cluster_uni_ea "a(gvkey) cluster(rdq)" // fe and clustering for univariate regressions 
global fe_and_cluster_multi_ea "a(gvkey sec_filing_limit) cluster(rdq)" // fe and clustering for multivariate regressions
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

global col_groups_ERC_all "group_2019_vs_2020_q1 group_2019_vs_2020_q2 group_2019_vs_2020_q3 group_2019_vs_2020_q4"

/* ----------------------------- Outreg options ----------------------------- */

global outreg_options "excel tstat bdec(3) tdec(2) adjr2 aster(tstat)"


/* ---------------------------- File name globals --------------------------- */

global table_file_ "$pipeline/out/table_"

// Important - You need to always run the code above before you can run any of the code below. 

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/*                         Table 1 - Sample statistics                        */
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/* ----------------------- Panel A - Sample statistics ---------------------- */

local dv_vars "Untimely_Filer sec_delay_final EADelay big_change"

tabstat `dv_vars', statistics( count mean  sd ) columns(statistics)

/* ----------------- Panel A- Within-firm STDDEV (Last Column)--------------- */

local dv_vars "Untimely_Filer sec_delay_final EADelay big_change"

global residual_vars ""

foreach dv in `dv_vars' {
    reghdfe `dv' , $fe_and_cluster_uni_ea residuals(residuals_`dv')
    global residual_vars "$residual_vars residuals_`dv'"
}

tabstat $residual_vars, statistics( sd ) columns(statistics)

/* ---------------------- Panel B - Statistics by group --------------------- */

tabstat $controls, statistics( mean ) columns(variables) by(sum_stats_groups) format(%9.4fc)

local table_label "1b"
local append "replace"
foreach dv in $controls {
    foreach group in $col_groups_all {
        reghdfe `dv' Post  if `group'==1, $fe_and_cluster_uni_sec
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options
        local append "append"
    }
}

local append "replace"
foreach dv in $controls {
        reghdfe `dv' i.postqs, noa cluster(fdate_final)
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv'") $outreg_options
        local append "append"
    }
	
/* ------------------- Panel C - Narrative characteristics ------------------ */

** Generated in the following Python file: `c_3_generate_narrative_statistics.ipynb`

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/*                           Table 2 - Summary Statistics                     */
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/* ------------------ Panel A - Average of Late Filers ---------------------- */

** Number unique observations per calendar quarter

egen tag = tag(gvkey closing_year_qtr)
egen ndistinct = total(tag), by(closing_year_qtr)
tabdisp closing_year_qtr, c(ndistinct)

tabstat LateFilerNT LateFilerCOVID LateFiler, statistics(mean) by(sum_stats_groups)

/* ------------------- Panel B - Univariate - Firm FE only ------------------ */
    
tabstat Untimely_Filer, by(closing_year_qtr) statistics (count)

local table_label "2b"
local append "replace"
foreach dv in Untimely_Filer {
    foreach group in $col_groups_all {
        reghdfe `dv' Post  if `group'==1, $fe_and_cluster_uni_sec
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options
        local append "append"
    }
}

/* ---------------- Panel C - Univariate - Firm + deadline FE --------------- */

local table_label "2c"
local append "replace"
foreach dv in Untimely_Filer {
    foreach group in $col_groups_all {
        reghdfe `dv' Post  if `group'==1, $fe_and_cluster_multi_sec
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options
        local append "append"
    }
}

/* ---------------- Panel D - Q1-2020 Late Filer Characteristics------------- */

tabstat mve_decile_ttests Large_Acc Acc_f NonAcc NUMEST is_big_4 choi_ofsize2_quintile new_auditor_office if Cal_year==2020 & Cal_month<4, by(Untimely_Filer) statistics (mean) format(%10.3g)

local table_label "2d"
local append "replace"
foreach dv in mve_decile_ttests Large_Acc Acc_f NonAcc NUMEST is_big_4 choi_ofsize2_quintile new_auditor_office {
        reghdfe `dv' Untimely_Filer  if Cal_year==2020 & Cal_month<4, $nofe_and_cluster_multi_sec
		outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv'") $outreg_options
        local append "append"
      }

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/*                  Table 3 - SEC Filing Lag - Timely filers                  */
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/* ------------------- Panel A - Univariate - Firm FE only ------------------ */

local table_label "3a"
local append "replace"
foreach dv in sec_delay_final {
    foreach group in $col_groups_timely  {
        reghdfe `dv' Post  if `group'==1, $fe_and_cluster_uni_sec
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options
        local append "append"
    }
}

/* ---------------- Panel B - Univariate - Firm + deadline FE --------------- */
	
local table_label "3b"
local append "replace"
foreach dv in sec_delay_final {
    foreach group in $col_groups_timely  {
        reghdfe `dv' Post  if `group'==1, $fe_and_cluster_multi_sec
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options
        local append "append"
    }
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/*             Table 4 - Earnings Announcement Lag - Timely filers            */
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/* ------------------- Panel A - Univariate - Firm FE only ------------------ */

local table_label "4a"
local append "replace"
foreach dv in EADelay  {
    foreach group in $col_groups_timely  {
        reghdfe `dv' Post  if `group'==1, $fe_and_cluster_uni_ea
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options
        local append "append"
    }
}

/* --------------- Panel B - Univariate - Firm + deadline FE ---------------- */

local table_label "4b"
local append "replace"
foreach dv in EADelay  {
    foreach group in $col_groups_timely  {
        reghdfe `dv' Post  if `group'==1, $fe_and_cluster_multi_ea
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options
        local append "append"
    }
}

/* --------------- Panel C- Multivariate - Firm + deadline FE --------------- */

local table_label "4c"
local append "replace"
foreach dv in "big_change" {
    foreach group in $col_groups_timely  {
        reghdfe `dv' Post  if `group'==1, $fe_and_cluster_multi_ea
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options
        local append "append"
    }
}

/* -------------- Panel D - Q1-2020 EALrgChg Filer Characteristics----------- */

tabstat mve_decile_ttests Large_Acc Acc_f NonAcc NUMEST is_big_4 choi_ofsize2_quintile new_auditor_office if Cal_year==2020 & Cal_month<4, by(EALargeLag_1_Descriptive) statistics (mean) format(%10.3g)

local table_label "4d"
local append "replace"
foreach dv in mve_decile_ttests Large_Acc Acc_f NonAcc NUMEST is_big_4 choi_ofsize2_quintile new_auditor_office {
        reghdfe `dv' EALargeLag_1_Descriptive  if Cal_year==2020 & Cal_month<4, $nofe_and_cluster_multi_ea
		outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv'") $outreg_options
        local append "append"
      }

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/*                          Table 5 - Reporting Quality                       */
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/* ------------------- Panel A - Averages------------------------------------ */

tabstat TotalRestatement_App PremFinalDiff  , by(sum_stats_groups_FRQ) statistics (mean)

/* ------------ Panel B - Restatements & Revisions with firm FE-------------- */

local table_label "5b"
local append "replace"
foreach dv in TotalRestatement_App {
    foreach group in $col_groups_restatement_all  {
        reghdfe `dv' Post   if `group'==1 , $fe_and_cluster_uni_sec
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options
        local append "append"
    }
}

local table_label "5b"
local append "append"
foreach dv in PremFinalDiff {
    foreach group in $col_groups_restatement_all  {
        reghdfe `dv' Post   if `group'==1 & Concurrent==0, $fe_and_cluster_uni_ea
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options
        local append "append"
    }
}

/* ------ Panel C - Restatements & Revisions with firm & deadline FE--------- */

local table_label "5c"
local append "replace"
foreach dv in TotalRestatement_App {
    foreach group in $col_groups_restatement_all  {
        reghdfe `dv' Post   if `group'==1 , $fe_and_cluster_multi_sec
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options
        local append "append"
    }
}

local table_label "5c"
local append "append"
foreach dv in PremFinalDiff {
    foreach group in $col_groups_restatement_all  {
        reghdfe `dv' Post   if `group'==1 & Concurrent==0, $fe_and_cluster_multi_ea
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options
        local append "append"
    }
}

/* ------------------------ Panel D - ERC Regressions------------------------ */

local controls_ERC "c_Predictb c_betab c_persistenceb c_bmb c_Log_MEb c_LossEPSb c_rep_lagb c_Lognumestb c_IORb c_mturnoveravg_m12_to_datb "
local controls_ERC_interactions "c_UE_c_Predictb c_UE_c_betab c_UE_c_persistenceb c_UE_c_bmb c_UE_c_Log_MEb c_UE_c_LossEPSb c_UE_c_rep_lagb c_UE_c_Lognumestb c_UE_c_IORb c_UE_c_mturnoveravg_m12_to_datb "
local table_label "5d"
local append "replace"

foreach dv in bhar_d_m1_to_1 {
    foreach group in $col_groups_ERC_all  {
        use "2_pipeline/c_1_generate_variables/out/regression_data.dta", clear
        **UE**
        egen m_UE_m2_2_s_rb_s = mean(UE_m2_2_s_rb_s)
        gen c_UE_m2_2_s_rb_s=UE_m2_2_s_rb_s-m_UE_m2_2_s_rb_s

        **Predict**;
        egen m_Predictb = mean(Predictb)
        gen c_Predictb=Predictb-m_Predictb
        gen c_UE_c_Predictb=c_Predictb*c_UE_m2_2_s_rb_s

        *Beta*;
        egen m_betab = mean(betab)
        gen c_betab=betab-m_betab
        gen c_UE_c_betab=c_betab*c_UE_m2_2_s_rb_s

        *Persistence*;
        egen m_persistenceb = mean(persistenceb)
        gen c_persistenceb=persistenceb-m_persistenceb
        gen c_UE_c_persistenceb=c_persistenceb*c_UE_m2_2_s_rb_s	

        *BM*;
        egen m_bmb = mean(bmb)
        gen c_bmb=bmb-m_bmb
        gen c_UE_c_bmb=c_bmb*c_UE_m2_2_s_rb_s	

        *Log_ME*;
        egen m_Log_MEb = mean(Log_MEb)
        gen c_Log_MEb=Log_MEb-m_Log_MEb
        gen c_UE_c_Log_MEb=c_Log_MEb*c_UE_m2_2_s_rb_s	

        *LossEPSb*;
        egen m_LossEPSb = mean(LossEPSb)
        gen c_LossEPSb=LossEPSb-m_LossEPSb
        gen c_UE_c_LossEPSb=c_LossEPSb*c_UE_m2_2_s_rb_s	

        *Post*;
        egen m_Post = mean(Post)
        gen c_Post=Post-m_Post
        gen c_UE_c_Post=c_Post*c_UE_m2_2_s_rb_s				

        *rep_lag*;
        egen m_rep_lagb = mean(rep_lagb)
        gen c_rep_lagb=rep_lagb-m_rep_lagb
        gen c_UE_c_rep_lagb=c_rep_lagb*c_UE_m2_2_s_rb_s			

        *Lognumestb*;
        egen m_Lognumestb = mean(Lognumestb)
        gen c_Lognumestb=Lognumestb-m_Lognumestb
        gen c_UE_c_Lognumestb=c_Lognumestb*c_UE_m2_2_s_rb_s	

        *IORb*;
        egen m_IORb = mean(IORb)
        gen c_IORb=IORb-m_IORb
        gen c_UE_c_IORb=c_IORb*c_UE_m2_2_s_rb_s	

        *Turnover*;
        egen m_mturnoveravg_m12_to_datb = mean(mturnoveravg_m12_to_datb)
        gen c_mturnoveravg_m12_to_datb=mturnoveravg_m12_to_datb-m_mturnoveravg_m12_to_datb
        gen c_UE_c_mturnoveravg_m12_to_datb=c_mturnoveravg_m12_to_datb*c_UE_m2_2_s_rb_s	

        ** Run regressions **;
        reghdfe `dv' c_UE_m2_2_s_rb_s c_Post c_UE_c_Post `controls_ERC' `controls_ERC_interactions' if `group'==1, $fe_and_cluster_uni_ea
        outreg2 using  "${table_file_}`table_label'.xls", `append' ctitle("`dv' - `group'") $outreg_options
        local append "append"
        
    }
}

use "2_pipeline/c_1_generate_variables/out/regression_data.dta", clear

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