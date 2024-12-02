CREATE DATABASE db_policys;

\c db_policys;

CREATE TABLE policy (
                        id uuid primary key,
                        policy_number text not null,
                        policy_start_date date not null,
                        policy_end_date date not null,
                        policy_type text not null,
                        insured_id uuid not null,
                        agent_id uuid not null,
                        created_at timestamp not null,
                        updated_at timestamp not null
);


CREATE TABLE insured (
                         id uuid primary key,
                         insured_name text null,
                         insured_gender text null,
                         insured_age int null,
                         insured_address text null,
                         insured_city text null,
                         insured_state text null,
                         insured_postal_code int null,
                         insured_country text null,
                         insurance_company text null,
                         created_at timestamp not null,
                         updated_at timestamp not null
);


CREATE TABLE premium (
                         id uuid primary key,
                         policy_id uuid not null,
                         premium_amount numeric null,
                         deductible_amount numeric null,
                         coverage_limit numeric null,
                         created_at timestamp not null,
                         updated_at timestamp not null
);


CREATE TABLE payments (
                          id uuid primary key,
                          policy_id uuid not null,
                          payment_status text null,
                          payment_date date not null,
                          payment_amount numeric null,
                          payment_method text null,
                          created_at timestamp not null,
                          updated_at timestamp not null
);


CREATE TABLE claims (
                        id uuid primary key,
                        policy_id uuid not null,
                        claim_date date not null,
                        claim_amount numeric null,
                        claim_description text null,
                        claim_status text null,
                        created_at timestamp not null,
                        updated_at timestamp not null
);


CREATE TABLE agents (
                        id uuid primary key,
                        agent_name text null,
                        agent_email text not null,
                        agent_phone text null,
                        created_at timestamp not null,
                        updated_at timestamp not null
);

CREATE INDEX idx_policy_number ON policy(policy_number);
CREATE INDEX idx_insured_email ON insured(insured_name);
CREATE INDEX idx_payment_date ON payments(payment_date);
CREATE INDEX idx_claim_date ON claims(claim_date);

ALTER TABLE policy
    ADD CONSTRAINT fk_policy_insured
        FOREIGN KEY (insured_id) REFERENCES insured(id),
    ADD CONSTRAINT fk_policy_agent
        FOREIGN KEY (agent_id) REFERENCES agents(id);

ALTER TABLE payments
    ADD CONSTRAINT fk_payments_policy
        FOREIGN KEY (policy_id) REFERENCES policy(id);

ALTER TABLE claims
    ADD CONSTRAINT fk_claims_policy
        FOREIGN KEY (policy_id) REFERENCES policy(id);

ALTER TABLE premium
    ADD CONSTRAINT fk_premium_policy
        FOREIGN KEY (policy_id) REFERENCES policy(id);