import dlt
from pyspark.sql.types import *
from pyspark.sql.functions import col

@dlt.table
def cases_demographics():
  cases = dlt.read("cases")
  expression_files_info = dlt.read("expression_files_info")
  df = cases.selectExpr(
    'case_id',
    '`demographic.ethnicity` as ethnicity',
    '`demographic.gender` as gender',
    '`demographic.race` as race',
    '`demographic.year_of_birth` as year_of_birth',
    '`demographic.year_of_death` as year_of_death'
  )
  return df.join(expression_files_info.select('case_id','file_id'), on = 'case_id')

  #patient diagnoses ingested from files downloaded from GDC API.

@dlt.table
def cases_diagnoses():
  cases=dlt.read("cases")
  expression_files_info = dlt.read("expression_files_info")
  df = cases.selectExpr(
    'case_id',
    '`diagnoses.0.classification_of_tumor` AS classification_of_tumor',
    '`diagnoses.0.diagnosis_id` AS diagnosis_id',
    '`diagnoses.0.primary_diagnosis` AS primary_diagnosis',
    '`diagnoses.0.tissue_or_organ_of_origin` AS tissue_or_organ_of_origin',
    '`diagnoses.0.treatments.0.therapeutic_agents` AS treatments0_therapeutic_agents',
    '`diagnoses.0.treatments.0.treatment_id` AS treatments0_treatment_id',
    '`diagnoses.0.treatments.1.therapeutic_agents` AS treatments1_therapeutic_agents',
    '`diagnoses.0.treatments.1.treatment_id` AS treatments1_treatment_id',
    '`diagnoses.0.treatments.1.updated_datetime` AS treatments1_updated_datetime',
    '`diagnoses.0.tumor_grade` AS tumor_grade'
  )
  return df.join(expression_files_info.select('case_id','file_id'), on = 'case_id')

  #"patient exposures ingested from files downloaded from GDC API.",

@dlt.table
def cases_exposures():
  cases = dlt.read("cases")
  expression_files_info = dlt.read("expression_files_info")
  df = cases.selectExpr(
    'case_id',
    '`exposures.0.alcohol_history` AS alcohol_history',
    '`exposures.0.alcohol_intensity` AS  alcohol_intensity',
    '`exposures.0.cigarettes_per_day` AS cigarettes_per_day',
    '`exposures.0.years_smoked` AS years_smoked',
  )
  return df.join(expression_files_info.select('case_id','file_id'), on = 'case_id')