%macro rollingreg
 (
 data= ,
 out_ds= ,
 model_equation= ,
 id= , date=date ,
 start_date= ,
 end_date= ,
 freq=, s=, n=,
 regprint=noprint
 );
%* Start with empty output data sets;
proc datasets nolist;
 delete _all_ds _outest_ds;
run;
* Prepare input data for by-id-date use;
proc sort data=&data;
 by &id &date;
run;
%* Set the 'by-id' variable; 

%let by_id= ; *blank default, no by variable;
%if %length(&id) > 0 %then %let by_id= by &id;
%* Determine date range variables;
%if %lowcase(%substr(&date,1,4))= year %then %let year_date=1;
 %else %let year_date=0;
%let sdate1 = &start_date;
%let sdate2 = &end_date;
%* Make start and end date if missing;
%if &start_date = %str() | &end_date = %str() %then %do;
 proc sql noprint;
 create table _dx1 as
 select min(&date) as min_date, max(&date) as max_date
 from &data where not missing(&date);
 select min_date into : min_date from _dx1;
 select max_date into : max_date from _dx1;
 quit;
%end;
%* SDATE1 and SDATE2 put in sas date number form (1/1/1960=0);
%if &sdate1 = %str() %then %do;
 %let sdate1= &min_date;
%end;
%else %do;
 %if (%index(&sdate1,%str(-)) > 1) | (%index(&sdate1,%str(/)) > 1)
 %then %let sdate1= %sysfunc(inputn(&sdate1,mmddyy10.));
 %else %if ( %length(&sdate1)=7 )
 %then %let sdate1= %sysfunc(inputn(01&sdate1,date9.));
 %else %if ( %length(&sdate1)=8 | %length(&sdate1)=9 )
 %then %let sdate1= %sysfunc(inputn(&sdate1,date9.));
 %else %if ( %length(&sdate1)=4 )
 %then %let sdate1= %sysfunc(inputn(01JAN&sdate1,date9.));
 %if &year_date=1 %then %let sdate1=%sysfunc(year(&sdate1));
%end;

%if &sdate2 = %str() %then %do;
 %let sdate2= &max_date;
%end;
%else %do;
 %if (%index(&sdate2,%str(-)) > 1) | (%index(&sdate2,%str(/)) > 1)
 %then %let sdate2= %sysfunc(inputn(&sdate2,mmddyy10.));
 %else %if ( %length(&sdate2)=7 ) %then %do;
 %let sdate2= %sysfunc(inputn(01&sdate2,date9.));
 %let sdate2= %sysfunc(intnx(month,&sdate2,0,end));
 %end;
 %else %if ( %length(&sdate2)=8 | %length(&sdate2)=9 )
 %then %let sdate2= %sysfunc(inputn(&sdate2,date9.));
 %else %if ( %length(&sdate2)=4 )
 %then %let sdate2= %sysfunc(inputn(31DEC&sdate2,date9.));
 %if &year_date=1 %then %let sdate2=%sysfunc(year(&sdate2));
%end;
%*Determine loop frequency parameters;
%if %eval(&n)= 0 %then %let n= &s;
%* if n blank use 1 period (=&s) assumption; 


%if &year_date=1 %then %let freq=year;
%* year frequency case;
%put Date variable: &date year_date: &year_date;
%put Start and end dates: &start_date &end_date // &sdate1 &sdate2;
%if &year_date=0 %then
 %put %sysfunc(putn(&sdate1,date9.)) %sysfunc(putn(&sdate2,date9.));
%put Freq: &freq s: &s n: &n;
%* Preliminary date setting for each iteration/loop;
%* First end date (idate2) is n periods after the start date;
%if &year_date=1 %then %let idate2= %eval(&sdate1+(&n-1));
 %else %let idate2= %sysfunc(intnx(&freq,&sdate1,(&n-1),end));
%if &year_date=0 %then %let idate1= %sysfunc(intnx(&freq,&idate2,-&n+1,begin));
 %else %let idate1= %eval(&idate2-&n+1);
%put First loop: &idate1 -- &idate2;
%put Loop through: &sdate2;
%if (&idate2 > &sdate2) %then %do;
%* Dates are not acceptable-- show problem, do not run loop;
%put PROBLEM-- end date for loop exceeds range : ( &idate2 > &sdate2 );
%end;
%else %do; *Dates are accepted-- run loops;
%let jj=0;
%do %while(&idate2 <= &sdate2);
%let jj=%eval(&jj+1);
%*Define loop start date (idate1) based on inherited end date (idate2);
%if &year_date=0 %then %do;
 %let idate1= %sysfunc(intnx(&freq,&idate2,-&n+1,begin));
 %let date1c= %sysfunc(putn(&idate1,date9.));
 %let date2c= %sysfunc(putn(&idate2,date9.));
%end;
%if &year_date=1 %then %do;
 %let idate1= %eval(&idate2-&n+1);
 %let date1c= &idate1;
 %let date2c= &idate2;
%end;
%let idate1= %sysfunc(max(&sdate1,&idate1));
%put Loop: &jj -- &date1c &date2c;
%put &jj -- &idate1 &idate2;
proc datasets nolist;
 delete _outest_ds;
run;
%***** analysis code here -- for each loop;
%* noprint to just make output set;
%let noprint= noprint;
%if %upcase(®print) = yes | %upcase(®print) = print %then %let noprint= ;
proc reg data=&data
outest=_outest_ds edf
 &noprint;
 where &date between &idate1 and &idate2;
 model &model_equation;
 &by_id;
run;
%* Add loop date range variables to output set;
data _outest_ds;
 set _outest_ds;
 regobs= _p_ + _edf_; %* number of observations in regression;
 date1= &idate1;
 date2= &idate2;
 %if &year_date=0 %then format date1 date2 date9.;
run;
%* Append results;
proc datasets nolist;
 append base=_all_ds data=_outest_ds;
run;
%* Set next loop end date;
%if &year_date=0 %then %let idate2= %sysfunc(intnx(&freq,&idate2,&s,end));
 %else %if &year_date=1 %then %let idate2= %eval(&idate2+&s);
%end; *% end of loop;
%* Save outout set to desired location;
data &out_ds;
 set _all_ds;
run;
proc sort data=&out_ds;
 by &id date2;
run;
%end; %* end for date check pass section;
%mend; 

