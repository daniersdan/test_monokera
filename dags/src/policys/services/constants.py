DB_SCHEMA = "public"
INSURED_TABLE = "insured"
AGENTS_TABLE = "agents"
POLICY_TABLE = "policy"
PAYMENTS_TABLE = "payments"
CLAIMS_TABLE = "claims"
PREMIUM_TABLE = "premium"


INSURED_COLUMNS_CSV = [
    "insured_name",
    "insured_gender",
    "insured_age",
    "insured_address",
    "insured_state",
    "insured_postal_code",
    "insured_city",
    "insured_country",
    "insurance_company",
]

INSURED_COLUMNS_TABLE = [
    "id",
    "insured_name",
    "insured_gender",
    "insured_age",
    "insured_address",
    "insured_state",
    "insured_city",
    "insured_postal_code",
    "insured_country",
    "insurance_company",
    "created_at",
    "updated_at",
]

AGENTS_COLUMNS_CSV = ["agent_name", "agent_email", "agent_phone"]

AGENTS_COLUMNS_TABLE = [
    "id",
    "agent_name",
    "agent_email",
    "agent_phone",
    "created_at",
    "updated_at",
]

POLICYS_COLUMNS_CSV = [
    "agent_name",
    "insured_name",
    "policy_number",
    "policy_start_date",
    "policy_end_date",
    "policy_type",
]

POLICYS_COLUMNS_TABLE = [
    "id",
    "policy_number",
    "policy_start_date",
    "policy_end_date",
    "policy_type",
    "insured_id",
    "agent_id",
    "created_at",
    "updated_at",
]

PAYMENTS_COLUMNS_CSV = [
    "policy_number",
    "payment_status",
    "payment_date",
    "payment_amount",
    "payment_method",
]

PAYMENTS_COLUMNS_TABLE = [
    "id",
    "policy_id",
    "payment_status",
    "payment_date",
    "payment_amount",
    "payment_method",
    "created_at",
    "updated_at",
]

CLAIMS_COLUMNS_CSV = [
    "policy_number",
    "claim_date",
    "claim_amount",
    "claim_description",
    "claim_status",
]

CLAIMS_COLUMNS_TABLE = [
    "id",
    "policy_id",
    "claim_date",
    "claim_amount",
    "claim_description",
    "claim_status",
    "created_at",
    "updated_at",
]


PREMIUM_COLUMNS_CSV = [
    "policy_number",
    "premium_amount",
    "deductible_amount",
    "coverage_limit",
]

PREMIUM_COLUMNS_TABLE = [
    "id",
    "policy_id",
    "premium_amount",
    "deductible_amount",
    "coverage_limit",
    "created_at",
    "updated_at",
]
