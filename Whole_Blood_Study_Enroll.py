import os
import oracledb
from dotenv import dotenv_values
from datetime import datetime, time, date
import pandas as pd
from mskpymail import send_email

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
config = dotenv_values(__location__ + "/.env")

oracledb.init_oracle_client(lib_dir="C:/Oracle/instantclient_21_9/")
connection = oracledb.connect(
    user=config["DB_USER"], password=config["DB_PASS"], dsn=config["DB_NAME"]
)

# Current Date Time
now = datetime.now().time()
current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
numeric_datetime = datetime.now().strftime("%Y%m%d%H%M%S")
today = date.today()
# Set the default time
default_time = time(4, 0, 0)

# Check if the current time is before 10:05 am
if now < time(10, 36, 0):
    today_start = datetime.combine(datetime.today(), default_time).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
else:
    today_start = datetime.combine(datetime.today(), time(10, 0, 0)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
parms = {"start_date": today_start, "end_date": current_datetime}

print("Start date:", today_start)
print("End date:", current_datetime)


cbc_query = """
SELECT
    per.NAME_FULL_FORMATTED as "patient_name"
  , v500.lab_fmt_accession(uces.accession_nbr) as "cbc_accession_nbr"
  , PM_GET_ALIAS('MRN', 0, per.PERSON_ID, 0, SYSDATE) as "mrn"
  , cclsql_utc_cnvt(uces.in_lab_dt_tm, 1,126) as "cbc_date_time_in_lab"
  , v500.omf_get_cv_display(uces.catalog_cd) as "cbc_order_procedure"
  , v500.omf_get_cv_display(enc.loc_nurse_unit_cd) as "pt_location"
  , v500.omf_get_cv_display(uces.activity_type_cd) as "cbc_activity_type"
  , v500.omf_get_cv_display(r.task_assay_cd) as "assay"
  , case v500.omf_get_cdf_meaning(pr.result_type_cd)
  when '1' then v500.lab_get_long_text_nortf(pr.long_text_id)
  when '2' then pr.result_value_alpha
  when '3' then trim(v500.lab_fmt_result(pr.service_resource_cd,r.task_assay_cd,pr.result_value_numeric,0))
  when '4' then
    case when (pr.long_text_id > 0 AND trim(pr.result_value_alpha) > ' ')
      then pr.result_value_alpha || ' - ' || v500.lab_get_long_text_nortf(pr.long_text_id)
    when (pr.long_text_id > 0)
      then v500.lab_get_long_text_nortf(pr.long_text_id)
    else pr.result_value_alpha
    end
  when '6' then to_char(cclsql_utc_cnvt(pr.result_value_dt_tm,1,126),'yyyy-mm-dd')
  when '7' then pr.ascii_text
  when '8' then trim(v500.lab_fmt_result(pr.service_resource_cd,r.task_assay_cd,pr.result_value_numeric,0))
  when '9' then v500.omf_get_cv_display(pr.result_code_set_cd)
  when '11' then to_char(cclsql_utc_cnvt(pr.result_value_dt_tm,1,126),'yyyy-mm-dd HH24:MI:SS')
end as "cbc_result"
  , case rrf.normal_ind
  when 0 then ''
  when 1 then trim(v500.lab_fmt_result (rrf.service_resource_cd,rrf.task_assay_cd,rrf.normal_low,0))
  when 2 then ''
  when 3 then trim(v500.lab_fmt_result (rrf.service_resource_cd,rrf.task_assay_cd,rrf.normal_low,0))
end as "reference_low"
  , case rrf.normal_ind
  when 0 then ''
  when 1 then ''
  when 2 then trim(v500.lab_fmt_result (rrf.service_resource_cd,rrf.task_assay_cd,rrf.normal_high,0))
  when 3 then trim(v500.lab_fmt_result (rrf.service_resource_cd,rrf.task_assay_cd,rrf.normal_high,0))
end as "reference_high"
FROM ENCOUNTER ENC
  , PERSON PER
  , UM_CHARGE_EVENT_ST UCES
  , REFERENCE_RANGE_FACTOR RRF
  , PERFORM_RESULT PR
  , RESULT R
WHERE (uces.result_id = r.result_id)
AND (uces.encntr_id = enc.encntr_id)
AND (uces.patient_id = per.person_id)
AND (pr.result_id = r.result_id
 and pr.result_status_cd in (select unique code_value from code_value where code_set = 1901 and cdf_meaning in ('VERIFIED','AUTOVERIFIED','CORRECTED')))
AND (pr.reference_range_factor_id = rrf.reference_range_factor_id)
AND (((uces.in_lab_dt_tm BETWEEN cclsql_cnvtdatetimeutc(to_date(:start_date,'YYYY-MM-DD HH24:MI:SS'),1,126,1) AND cclsql_cnvtdatetimeutc(to_date(:end_date,'YYYY-MM-DD HH24:MI:SS'),1,126,1))
    AND (enc.loc_nurse_unit_cd IN (2552871925.00,2552871943.00,2552871949.00,2555187977.00,2554842765.00,2552871955.00,2555865409.00,2552871931.00,2554975363.00,2552877235.00,2555408411.00))
    AND (uces.catalog_cd IN (2552677267.00,2561201779.00))
    AND (r.task_assay_cd IS NULL
      OR r.task_assay_cd IN (2554226503.00,2555995881.00,2554650729.00,2552727241.00,0,2552727181.00)))
  AND (uces.patient_fac_cd IS NULL
    OR uces.patient_fac_cd IN (0,2552831699.00,2552819553.00,2552819557.00,2552819545.00,2552819541.00,2552923205.00,2552819561.00,2552819565.00,2552831651.00,2552923151.00,2554829303.00,2554829285.00,2552831657.00,2555416181.00,2555301415.00,2552923169.00,2552831663.00,2552923187.00,2555366977.00,2552923403.00,2552878003.00,2552923367.00,2552923223.00,2555371203.00,2552831645.00,2552831687.00,2552909717.00,2552831669.00,2552819549.00,2552831705.00,2555402539.00,2554942199.00,2555478755.00,2552831675.00,2552923349.00,2552909699.00,2552923295.00,2552909681.00,2552923259.00,2555196575.00,2552615531.00,2552651407.00,2555672665.00,2552923277.00,2552923241.00,2555279575.00,2552909645.00,2552909663.00,2552922971.00,2552922989.00,2552923025.00,2552923007.00,2552922935.00,2552923043.00,2552923061.00,2552922953.00,2552923079.00,2552923097.00,2552923115.00,2552923133.00,2552923313.00,2555909329.00,2553335905.00,2552831681.00,2552831639.00,2552923385.00,2552831711.00,2552923331.00,2556017625.00,2555475409.00,2552831693.00,2552925817.00,2555269075.00,2560121111.00,2560682671.00,2561205975.00,2562996689.00)))
GROUP BY per.person_id, per.NAME_FULL_FORMATTED
  , uces.accession_nbr
  , per.PERSON_ID
  , cclsql_utc_cnvt(uces.in_lab_dt_tm, 1,126)
  , uces.catalog_cd
  , v500.omf_get_cv_display(uces.catalog_cd)
  , enc.loc_nurse_unit_cd
  , uces.activity_type_cd
  , v500.omf_get_cv_display(uces.activity_type_cd)
  , v500.omf_get_cv_display(r.task_assay_cd)
  , case v500.omf_get_cdf_meaning(pr.result_type_cd)
  when '1' then v500.lab_get_long_text_nortf(pr.long_text_id)
  when '2' then pr.result_value_alpha
  when '3' then trim(v500.lab_fmt_result(pr.service_resource_cd,r.task_assay_cd,pr.result_value_numeric,0))
  when '4' then
    case when (pr.long_text_id > 0 AND trim(pr.result_value_alpha) > ' ')
      then pr.result_value_alpha || ' - ' || v500.lab_get_long_text_nortf(pr.long_text_id)
    when (pr.long_text_id > 0)
      then v500.lab_get_long_text_nortf(pr.long_text_id)
    else pr.result_value_alpha
    end
  when '6' then to_char(cclsql_utc_cnvt(pr.result_value_dt_tm,1,126),'yyyy-mm-dd')
  when '7' then pr.ascii_text
  when '8' then trim(v500.lab_fmt_result(pr.service_resource_cd,r.task_assay_cd,pr.result_value_numeric,0))
  when '9' then v500.omf_get_cv_display(pr.result_code_set_cd)
  when '11' then to_char(cclsql_utc_cnvt(pr.result_value_dt_tm,1,126),'yyyy-mm-dd HH24:MI:SS')
end
  , case rrf.normal_ind
  when 0 then ''
  when 1 then trim(v500.lab_fmt_result (rrf.service_resource_cd,rrf.task_assay_cd,rrf.normal_low,0))
  when 2 then ''
  when 3 then trim(v500.lab_fmt_result (rrf.service_resource_cd,rrf.task_assay_cd,rrf.normal_low,0))
end
  , case rrf.normal_ind
  when 0 then ''
  when 1 then ''
  when 2 then trim(v500.lab_fmt_result (rrf.service_resource_cd,rrf.task_assay_cd,rrf.normal_high,0))
  when 3 then trim(v500.lab_fmt_result (rrf.service_resource_cd,rrf.task_assay_cd,rrf.normal_high,0))
end
ORDER BY per.NAME_FULL_FORMATTED nulls first
  , uces.accession_nbr nulls first
  , PM_GET_ALIAS('MRN', 0, per.PERSON_ID, 0, SYSDATE) nulls first
  , cclsql_utc_cnvt(uces.in_lab_dt_tm, 1,126) nulls first
  , v500.omf_get_cv_display(uces.catalog_cd) nulls first
  , v500.omf_get_cv_display(enc.loc_nurse_unit_cd) nulls first
  , v500.omf_get_cv_display(uces.activity_type_cd) nulls first
  , v500.omf_get_cv_display(r.task_assay_cd) nulls first
  , case v500.omf_get_cdf_meaning(pr.result_type_cd)
  when '1' then v500.lab_get_long_text_nortf(pr.long_text_id)
  when '2' then pr.result_value_alpha
  when '3' then trim(v500.lab_fmt_result(pr.service_resource_cd,r.task_assay_cd,pr.result_value_numeric,0))
  when '4' then
    case when (pr.long_text_id > 0 AND trim(pr.result_value_alpha) > ' ')
      then pr.result_value_alpha || ' - ' || v500.lab_get_long_text_nortf(pr.long_text_id)
    when (pr.long_text_id > 0)
      then v500.lab_get_long_text_nortf(pr.long_text_id)
    else pr.result_value_alpha
    end
  when '6' then to_char(cclsql_utc_cnvt(pr.result_value_dt_tm,1,126),'yyyy-mm-dd')
  when '7' then pr.ascii_text
  when '8' then trim(v500.lab_fmt_result(pr.service_resource_cd,r.task_assay_cd,pr.result_value_numeric,0))
  when '9' then v500.omf_get_cv_display(pr.result_code_set_cd)
  when '11' then to_char(cclsql_utc_cnvt(pr.result_value_dt_tm,1,126),'yyyy-mm-dd HH24:MI:SS')
end nulls first
  , case rrf.normal_ind
  when 0 then ''
  when 1 then trim(v500.lab_fmt_result (rrf.service_resource_cd,rrf.task_assay_cd,rrf.normal_low,0))
  when 2 then ''
  when 3 then trim(v500.lab_fmt_result (rrf.service_resource_cd,rrf.task_assay_cd,rrf.normal_low,0))
end nulls first
  , case rrf.normal_ind
  when 0 then ''
  when 1 then ''
  when 2 then trim(v500.lab_fmt_result (rrf.service_resource_cd,rrf.task_assay_cd,rrf.normal_high,0))
  when 3 then trim(v500.lab_fmt_result (rrf.service_resource_cd,rrf.task_assay_cd,rrf.normal_high,0))
end nulls first
"""
# execute cbc query and output to pandas df
cur = connection.cursor()
cbc_result = cur.execute(cbc_query, parms).fetchall()
columns = [desc[0] for desc in cur.description]
cbc_df = pd.DataFrame(cbc_result, columns=columns)
cur.close()
connection.close()


connection = oracledb.connect(
    user=config["DB_USER"], password=config["DB_PASS"], dsn=config["DB_NAME"]
)

micro_query = """
SELECT
  per.NAME_FULL_FORMATTED as "patient_name"
  , v500.lab_fmt_accession(uces.accession_nbr) as "micro_accession_nbr"
  , PM_GET_ALIAS('MRN', 0, per.PERSON_ID, 0, SYSDATE) as "mrn"
  , cclsql_utc_cnvt(uces.in_lab_dt_tm, 1,126) as "micro_date_time_in_lab"
  , v500.omf_get_cv_display(uces.catalog_cd) as "micro_order_procedure"
  , v500.omf_get_cv_display(enc.loc_nurse_unit_cd) as "pt_location"
  , v500.omf_get_cv_display(uces.activity_type_cd) as "micro_activity_type"
FROM ENCOUNTER ENC
  , PERSON PER
  , UM_CHARGE_EVENT_ST UCES
WHERE (uces.encntr_id = enc.encntr_id)
AND (uces.patient_id = per.person_id)
AND (((uces.in_lab_dt_tm BETWEEN cclsql_cnvtdatetimeutc(to_date(:start_date,'YYYY-MM-DD HH24:MI:SS'),1,126,1) AND cclsql_cnvtdatetimeutc(to_date(:end_date,'YYYY-MM-DD HH24:MI:SS'),1,126,1))
    AND (enc.loc_nurse_unit_cd IN (2552871925.00,2552871943.00,2552871949.00,2555187977.00,2554842765.00,2552871955.00,2555865409.00,2552871931.00,2554975363.00,2552877235.00,2555408411.00))
    AND (uces.catalog_cd IN (2552925553.00,2552925561.00,2555518389.00,2552925569.00,2552677933.00,2554342179.00,2552925585.00,2552925593.00,2552677837.00,2552677843.00,2552925601.00,2552677849.00,2555492631.00,2554272027.00,2552677927.00,2562534053.00,2552677873.00,2562580387.00,2561122623.00,2566736283.00,2552925617.00,2552677867.00,2554358997.00,2552677921.00,2562700883.00,2552677819.00,2552834089.00,2552834097.00,2552834105.00,2552659503.00,2552839593.00,2555442753.00,2555442745.00,2555442761.00,2555442769.00,2555442777.00,2555442785.00,2555442793.00,2555442801.00,2555442809.00,2555442817.00,2552839529.00,2552839537.00,2552839425.00,2552839673.00,2552839433.00,2552839585.00,2552839633.00,2552839641.00,2552839665.00,2552839545.00,31713969.00,2552839409.00,2552839417.00,2552839513.00,2552839505.00,2552839649.00,2552839393.00,2560510145.00)))
  AND (uces.patient_fac_cd IS NULL
    OR uces.patient_fac_cd IN (0,2552831699.00,2552819553.00,2552819557.00,2552819545.00,2552819541.00,2552923205.00,2552819561.00,2552819565.00,2552831651.00,2552923151.00,2554829303.00,2554829285.00,2552831657.00,2555416181.00,2555301415.00,2552923169.00,2552831663.00,2552923187.00,2555366977.00,2552923403.00,2552878003.00,2552923367.00,2552923223.00,2555371203.00,2552831645.00,2552831687.00,2552909717.00,2552831669.00,2552819549.00,2552831705.00,2555402539.00,2554942199.00,2555478755.00,2552831675.00,2552923349.00,2552909699.00,2552923295.00,2552909681.00,2552923259.00,2555196575.00,2552615531.00,2552651407.00,2555672665.00,2552923277.00,2552923241.00,2555279575.00,2552909645.00,2552909663.00,2552922971.00,2552922989.00,2552923025.00,2552923007.00,2552922935.00,2552923043.00,2552923061.00,2552922953.00,2552923079.00,2552923097.00,2552923115.00,2552923133.00,2552923313.00,2555909329.00,2553335905.00,2552831681.00,2552831639.00,2552923385.00,2552831711.00,2552923331.00,2556017625.00,2555475409.00,2552831693.00,2552925817.00,2555269075.00,2560121111.00,2560682671.00,2561205975.00,2562996689.00)))
GROUP BY per.person_id, per.NAME_FULL_FORMATTED
  , uces.accession_nbr
  , per.PERSON_ID
  , cclsql_utc_cnvt(uces.in_lab_dt_tm, 1,126)
  , uces.catalog_cd
  , v500.omf_get_cv_display(uces.catalog_cd)
  , enc.loc_nurse_unit_cd
  , uces.activity_type_cd
  , v500.omf_get_cv_display(uces.activity_type_cd)
ORDER BY per.NAME_FULL_FORMATTED nulls first
  , uces.accession_nbr nulls first
  , PM_GET_ALIAS('MRN', 0, per.PERSON_ID, 0, SYSDATE) nulls first
  , cclsql_utc_cnvt(uces.in_lab_dt_tm, 1,126) nulls first
  , v500.omf_get_cv_display(uces.catalog_cd) nulls first
  , v500.omf_get_cv_display(enc.loc_nurse_unit_cd) nulls first
  , v500.omf_get_cv_display(uces.activity_type_cd) nulls first
"""
# execute micro query and output to pandas df
cur = connection.cursor()
micro_result = cur.execute(micro_query, parms).fetchall()
columns = [desc[0] for desc in cur.description]
micro_df = pd.DataFrame(micro_result, columns=columns)
cur.close()
connection.close()

pt = pd.merge(cbc_df, micro_df, how="inner")
pt
table_html = pt.to_html()
pt.to_csv(
    f"//vpenslab/LabShared/MicroBiology/QMS Data/Whole Blood Study/ucc pt with cbc and micro test {numeric_datetime}.csv",
    index=False,
)
attachment = f"//vpenslab/LabShared/MicroBiology/QMS Data/Whole Blood Study/ucc pt with cbc and micro test {numeric_datetime}.csv"
body = f"<h2>Patient's in UCC with CBC and Micro Test {today_start} to {current_datetime}</h2> <br> {table_html} <br>"

send_email(
    to=[
        "clarkb@mskcc.org",
        "mcmillet@mskcc.org",
        "janik@mskcc.org",
    ],
    subject=f"Patient's in UCC with CBC and Micro Test {today_start} to {current_datetime}",
    body=body,
    attachments=[attachment],
    username=config["AD_USERNAME"],
    password=config["AD_PASSWORD"],
)
