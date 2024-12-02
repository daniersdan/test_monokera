from psycopg2 import sql

querie_insured_data = sql.SQL(
    """
select *
from public.insured;
"""
)

update_insured_data_query = sql.SQL(
    """
    UPDATE public.insured AS f
    SET insured_name = t.insured_name,
	insured_age = t.insured_age,
	insured_gender = t.insured_gender,
	insured_address = t.insured_address,
	insured_state = t.insured_state,
	insured_city = t.insured_city,
	insured_postal_code = t.insured_postal_code,
	insured_country = t.insured_country,
	insurance_company = t.insurance_company,
	updated_at = t.updated_at::timestamp
    FROM cte AS t
    WHERE f.id::text = t.id::text;
    """
)

querie_agents_data = sql.SQL(
    """
select *
from public.agents;
"""
)

update_agents_data_query = sql.SQL(
    """
    UPDATE public.agents AS f
    SET agent_name = t.agent_name,
	agent_email = t.agent_email,
	agent_phone = t.agent_phone,
	updated_at = t.updated_at::timestamp
    FROM cte AS t
    WHERE f.id::text = t.id::text;
    """
)

querie_policy_data = sql.SQL(
    """
select *
from public.policy;
"""
)

update_policy_data_query = sql.SQL(
    """
    UPDATE public.policy AS f
    SET policy_start_date = t.policy_start_date::date,
	policy_end_date = t.policy_end_date::date,
	policy_type = t.policy_type,
	insured_id = t.insured_id::uuid,
	agent_id = t.agent_id::uuid,
	updated_at = t.updated_at::timestamp
    FROM cte AS t
    WHERE f.id::text = t.id::text;
    """
)

querie_payments_data = sql.SQL(
    """
select *
from public.payments;
"""
)

querie_claims_data = sql.SQL(
    """
select *
from public.claims;
"""
)
