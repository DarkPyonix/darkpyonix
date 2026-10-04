-- darkpyonix.dev hub schema (SPEC §10). Tokens and session ids are stored as SHA-256 hex only.

-- FR-H6: one account per GitHub user (the numeric, never-reused GitHub user id).
CREATE TABLE accounts (
    account_id    TEXT PRIMARY KEY,               -- a_<16 hex>
    github_id     INTEGER NOT NULL UNIQUE,
    github_login  TEXT NOT NULL,                  -- display only; logins can change
    created_at    INTEGER NOT NULL
);

-- Browser sessions after GitHub sign-in.
CREATE TABLE sessions (
    session_hash  TEXT PRIMARY KEY,
    account_id    TEXT NOT NULL REFERENCES accounts(account_id),
    created_at    INTEGER NOT NULL,
    expires_at    INTEGER NOT NULL
);

-- In-flight GitHub authorizations: state -> PKCE verifier, single use.
CREATE TABLE oauth_transactions (
    state          TEXT PRIMARY KEY,
    code_verifier  TEXT NOT NULL,
    return_to      TEXT NOT NULL,
    expires_at     INTEGER NOT NULL
);

-- FR-H1: a device asks to join; a signed-in user (or the account's main server) approves.
CREATE TABLE device_links (
    link_id      TEXT PRIMARY KEY,                -- l_<32 hex>
    user_code    TEXT NOT NULL UNIQUE,            -- XXXX-XXXX
    endpoint_id  TEXT NOT NULL,
    name         TEXT NOT NULL,
    role         TEXT NOT NULL,
    challenge    TEXT NOT NULL,
    status       TEXT NOT NULL,                   -- pending | approved | denied | claimed
    account_id   TEXT REFERENCES accounts(account_id),
    created_at   INTEGER NOT NULL,
    expires_at   INTEGER NOT NULL
);

CREATE TABLE devices (
    endpoint_id  TEXT PRIMARY KEY,                -- lowercase hex ed25519 key (iroh EndpointId)
    account_id   TEXT NOT NULL REFERENCES accounts(account_id),
    name         TEXT NOT NULL,
    role         TEXT NOT NULL,                   -- main_server | computer
    token_hash   TEXT NOT NULL UNIQUE,
    created_at   INTEGER NOT NULL,
    last_seen    INTEGER,
    online       INTEGER NOT NULL DEFAULT 0,      -- reported by the relay host
    revoked_at   INTEGER
);
CREATE INDEX devices_by_account ON devices(account_id);

-- FR-H2: the latest signed pkarr packet per device (relay payload, base64url).
CREATE TABLE records (
    endpoint_id   TEXT PRIMARY KEY REFERENCES devices(endpoint_id),
    payload       TEXT NOT NULL,
    timestamp_us  INTEGER NOT NULL
);

-- FR-H4
CREATE TABLE shares (
    share_id     TEXT PRIMARY KEY,
    endpoint_id  TEXT NOT NULL REFERENCES devices(endpoint_id),
    created_at   INTEGER NOT NULL
);

CREATE TABLE relay_passes (
    token_hash  TEXT PRIMARY KEY,
    share_id    TEXT NOT NULL,
    expires_at  INTEGER NOT NULL
);

-- FR-H5
CREATE TABLE names (
    name         TEXT PRIMARY KEY,
    endpoint_id  TEXT NOT NULL REFERENCES devices(endpoint_id),
    account_id   TEXT NOT NULL REFERENCES accounts(account_id),
    created_at   INTEGER NOT NULL
);
