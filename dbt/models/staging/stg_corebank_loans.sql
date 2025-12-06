{{ config(materialized='view') }}

with src as (
  select * from {{ source('raw', 'loan_applications_raw') }}
),

clean as (
  select
    -- IDs
    cast(UniqueID as string)                            as loan_id,
    cast(UniqueID as string)                            as customer_id,      -- 1 loan ~ 1 customer (simplification)
    cast(manufacturer_id as string)                     as product_id,
    cast(branch_id as string)                           as dealer_id,

    -- Dates (already DATE in BigQuery, so just pass through)
    DisbursalDate                                       as application_date,
    Date_of_Birth                                       as date_of_birth_raw,

    -- Amounts / ratios
    cast(disbursed_amount as numeric)                   as loan_amount,
    cast(asset_cost as numeric)                         as asset_cost,
    cast(ltv as numeric)                                as ltv_ratio,

    -- Basic customer attributes
    Employment_Type                                     as employment_type,
    cast(Current_pincode_ID as string)                  as current_pincode_id,
    cast(State_ID as string)                            as state_id,

    -- KYC / document flags
    MobileNo_Avl_Flag,
    Aadhar_flag,
    PAN_flag,
    VoterID_flag,
    Driving_flag,
    Passport_flag,

    -- Bureau score
    cast(PERFORM_CNS_SCORE as int64)                    as perform_cns_score,
    PERFORM_CNS_SCORE_DESCRIPTION                       as perform_cns_score_description,

    -- Primary bureau aggregates
    cast(PRI_NO_OF_ACCTS as int64)                      as pri_no_of_accts,
    cast(PRI_ACTIVE_ACCTS as int64)                     as pri_active_accts,
    cast(PRI_OVERDUE_ACCTS as int64)                    as pri_overdue_accts,
    cast(PRI_CURRENT_BALANCE as numeric)                as pri_current_balance,
    cast(PRI_SANCTIONED_AMOUNT as numeric)              as pri_sanctioned_amount,
    cast(PRI_DISBURSED_AMOUNT as numeric)               as pri_disbursed_amount,

    -- Secondary bureau aggregates
    cast(SEC_NO_OF_ACCTS as int64)                      as sec_no_of_accts,
    cast(SEC_ACTIVE_ACCTS as int64)                     as sec_active_accts,
    cast(SEC_OVERDUE_ACCTS as int64)                    as sec_overdue_accts,
    cast(SEC_CURRENT_BALANCE as numeric)                as sec_current_balance,
    cast(SEC_SANCTIONED_AMOUNT as numeric)              as sec_sanctioned_amount,
    cast(SEC_DISBURSED_AMOUNT as numeric)               as sec_disbursed_amount,

    -- Instalments
    cast(PRIMARY_INSTAL_AMT as numeric)                 as primary_instal_amt,
    cast(SEC_INSTAL_AMT as numeric)                     as sec_instal_amt,

    -- History / inquiries
    cast(NEW_ACCTS_IN_LAST_SIX_MONTHS as int64)         as new_accts_last_6m,
    cast(DELINQUENT_ACCTS_IN_LAST_SIX_MONTHS as int64)  as delinquent_accts_last_6m,
    AVERAGE_ACCT_AGE                                    as average_acct_age_raw,
    CREDIT_HISTORY_LENGTH                               as credit_history_length_raw,
    cast(NO_OF_INQUIRIES as int64)                      as no_of_inquiries,

    -- Target
    cast(loan_default as int64)                         as loan_default

  from src
)

select * from clean

