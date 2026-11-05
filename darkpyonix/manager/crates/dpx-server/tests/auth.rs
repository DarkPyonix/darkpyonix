//! SPEC FR-A4 (manager password and master token), FR-A5 (token lifetime) and FR-A6 (kernel
//! access tokens held by the kernel). The fake kernel keeps tokens in the backend, which two
//! managers share, as two managers of one account share a real kernel.

mod common;

use common::*;
use dpx_server::{Mode, ServerConfig};
use serde_json::{json, Value};
use std::time::Duration;

const ALL: [&str; 5] = ["read", "history", "execute", "edit", "manage"];

async fn basic(s: &TestServer, method: &str, path: &str, password: &str) -> Resp {
    let api = s.anon();
    let rb = api.req(method, path).basic_auth("", Some(password));
    api.send(method, path, rb).await
}

async fn initial(s: &TestServer, kid: &str) -> String {
    let r = s.anon().post(&format!("/api/kernels/{kid}/tokens/initial"), json!({})).await;
    assert_eq!(r.status, 201, "{}", r.text);
    r.body["token"].as_str().unwrap().to_string()
}

async fn set_first_password(s: &TestServer, kid: &str, password: &str) {
    let init = initial(s, kid).await;
    let r = s.with_token(&init).call("PUT", "/api/auth/password", Some(json!({"password": password}))).await;
    assert_eq!(r.status, 200, "{}", r.text);
}

#[tokio::test]
async fn test_fr_a4_first_password_login_and_master_reset() {
    let s = TestServer::start(Mode::Dedicated).await;
    let (kid, _) = s.kernel("a.py");
    set_first_password(&s, &kid, "correct horse").await;

    let r = basic(&s, "GET", "/api/auth", "correct horse").await;
    assert_eq!(r.status, 200, "{}", r.text);
    assert_eq!(r.body["token"], s.token);
    basic(&s, "GET", "/api/auth", "wrong").await.error(401, "unauthorized");

    let r = basic(&s, "PUT", "/api/auth/tokens/master", "correct horse").await;
    assert_eq!(r.status, 200, "{}", r.text);
    let fresh = r.body["token"].as_str().unwrap().to_string();
    assert_ne!(fresh, s.token);
    s.with_token(&s.token).get("/api/manager").await.error(401, "unauthorized");
    assert_eq!(s.with_token(&fresh).get("/api/manager").await.status, 200);
    // The master token resets the password; the new one logs in.
    let r = s.with_token(&fresh).call("PUT", "/api/auth/password", Some(json!({"password": "second"}))).await;
    assert_eq!(r.status, 200, "{}", r.text);
    basic(&s, "GET", "/api/auth", "correct horse").await.error(401, "unauthorized");
    assert_eq!(basic(&s, "GET", "/api/auth", "second").await.body["token"], fresh);
}

#[tokio::test]
async fn test_fr_a4_login_before_password_is_400() {
    let s = TestServer::start(Mode::Dedicated).await;
    basic(&s, "GET", "/api/auth", "anything").await.error(400, "password_not_set");
    basic(&s, "PUT", "/api/auth/tokens/master", "anything").await.error(400, "password_not_set");
    let (kid, _) = s.kernel("a.py");
    let r = s.anon().post(&format!("/api/kernels/{kid}/tokens/auth"), json!({"password": "x"})).await;
    r.error(400, "password_not_set");
}

#[tokio::test]
async fn test_fr_a4_initial_token_cannot_replace_an_existing_password() {
    let s = TestServer::start(Mode::Dedicated).await;
    let (kid, _) = s.kernel("a.py");
    set_first_password(&s, &kid, "first").await;
    let init = initial(&s, &kid).await;
    let r = s.with_token(&init).call("PUT", "/api/auth/password", Some(json!({"password": "evil"}))).await;
    r.error(403, "forbidden");
    assert_eq!(basic(&s, "GET", "/api/auth", "first").await.status, 200);
    basic(&s, "GET", "/api/auth", "evil").await.error(401, "unauthorized");
}

#[tokio::test]
async fn test_fr_a4_one_password_per_manager() {
    let s = TestServer::start(Mode::Dedicated).await;
    let (k1, _) = s.kernel("a.py");
    let (k2, _) = s.kernel("b.py");
    set_first_password(&s, &k1, "shared pw").await;
    for kid in [&k1, &k2] {
        let r = s.anon().post(&format!("/api/kernels/{kid}/tokens/auth"), json!({"password": "shared pw"})).await;
        assert_eq!(r.status, 200, "{}", r.text);
        assert_eq!(r.body["kernel_id"], kid.as_str());
        assert_eq!(r.body["capabilities"], json!(ALL));
    }
    let r = s.anon().post(&format!("/api/kernels/{k1}/tokens/auth"), json!({"password": "nope"})).await;
    r.error(401, "unauthorized");
}

#[tokio::test]
async fn test_fr_a4_shared_token_reset_revokes_the_previous_token() {
    let s = TestServer::start(Mode::Dedicated).await;
    let (kid, _) = s.kernel("a.py");
    let r = s.admin().call("PUT", "/api/auth/tokens/shared/read+history", None).await;
    assert_eq!(r.status, 200, "{}", r.text);
    assert_eq!(r.body["capabilities"], json!(["read", "history"]));
    let first = r.body["token"].as_str().unwrap().to_string();
    // It works on every kernel of this manager, with its capabilities only.
    assert_eq!(s.with_token(&first).get(&format!("/api/kernels/{kid}")).await.status, 200);
    s.with_token(&first).post(&format!("/api/kernels/{kid}/restart"), json!({})).await.error(403, "forbidden");

    let second = s.admin().call("PUT", "/api/auth/tokens/shared/read+history", None).await;
    let second = second.body["token"].as_str().unwrap().to_string();
    assert_ne!(first, second);
    s.with_token(&first).get(&format!("/api/kernels/{kid}")).await.error(401, "token_revoked");
    assert_eq!(s.with_token(&second).get(&format!("/api/kernels/{kid}")).await.status, 200);
    // A different type is a different token and leaves this one alone.
    s.admin().call("PUT", "/api/auth/tokens/shared/read", None).await;
    assert_eq!(s.with_token(&second).get(&format!("/api/kernels/{kid}")).await.status, 200);

    s.admin().call("PUT", "/api/auth/tokens/shared/history+read", None).await.error(400, "bad_request");
    s.admin().call("PUT", "/api/auth/tokens/shared/admin", None).await.error(400, "bad_request");
    s.with_token(&second).call("PUT", "/api/auth/tokens/shared/read", None).await.error(403, "forbidden");
}

#[tokio::test]
async fn test_fr_a4_ephemeral_manager_refuses_auth_family() {
    let s = TestServer::start(Mode::Ephemeral).await;
    basic(&s, "GET", "/api/auth", "x").await.error(403, "forbidden");
    basic(&s, "PUT", "/api/auth/tokens/master", "x").await.error(403, "forbidden");
    s.admin().call("PUT", "/api/auth/password", Some(json!({"password": "x"}))).await.error(403, "forbidden");
    s.admin().call("PUT", "/api/auth/tokens/shared/read", None).await.error(403, "forbidden");
}

#[tokio::test]
async fn test_fr_a4_password_is_stored_as_a_salted_slow_hash() {
    let s = TestServer::start(Mode::Dedicated).await;
    let (kid, _) = s.kernel("a.py");
    set_first_password(&s, &kid, "plain secret").await;
    let conn = rusqlite::Connection::open(s.home.join("manager.db")).unwrap();
    let stored: String =
        conn.query_row("SELECT value FROM manager_auth WHERE key = 'password'", rusqlite::params![], |r| r.get(0)).unwrap();
    assert!(!stored.contains("plain secret"));
    let parts: Vec<&str> = stored.split('$').collect();
    assert_eq!(parts[0], "pbkdf2-sha256");
    assert!(parts[1].parse::<u32>().unwrap() >= 100_000);
    assert_eq!(parts[2].len(), 32, "a 16-byte salt");
    // The same password hashes differently the next time.
    s.admin().call("PUT", "/api/auth/password", Some(json!({"password": "plain secret"}))).await;
    let again: String =
        conn.query_row("SELECT value FROM manager_auth WHERE key = 'password'", rusqlite::params![], |r| r.get(0)).unwrap();
    assert_ne!(stored, again);
    // Master and shared tokens are hashes too.
    s.admin().call("PUT", "/api/auth/tokens/shared/read", None).await;
    let dump: Vec<String> = conn
        .prepare("SELECT token_sha256 FROM manager_shared")
        .unwrap()
        .query_map(rusqlite::params![], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(dump.len(), 1);
    assert_eq!(dump[0].len(), 64);
}

#[tokio::test]
async fn test_fr_a5_password_and_master_reset_survive_a_manager_restart() {
    let mut s = TestServer::start(Mode::Dedicated).await;
    let (kid, _) = s.kernel("a.py");
    set_first_password(&s, &kid, "stays").await;
    let fresh = basic(&s, "PUT", "/api/auth/tokens/master", "stays").await.body["token"].as_str().unwrap().to_string();
    let shared = s.with_token(&fresh).call("PUT", "/api/auth/tokens/shared/read", None).await.body["token"]
        .as_str()
        .unwrap()
        .to_string();
    s.stop().await;

    let cfg = ServerConfig::dedicated(&s.home, "127.0.0.1", 0);
    let s2 = TestServer::start_cfg(cfg, s.backend.clone(), s.scratch.clone()).await;
    assert_eq!(s2.with_token(&fresh).get("/api/manager").await.status, 200);
    s2.with_token(&s2.token).get("/api/manager").await.error(401, "unauthorized");
    assert_eq!(s2.with_token(&shared).get(&format!("/api/kernels/{kid}")).await.status, 200);
    // The master token's value is not stored, so a login after the restart issues a new one.
    let r = basic(&s2, "GET", "/api/auth", "stays").await;
    assert_eq!(r.status, 200, "{}", r.text);
    let again = r.body["token"].as_str().unwrap().to_string();
    assert_eq!(s2.with_token(&again).get("/api/manager").await.status, 200);
}

#[tokio::test]
async fn test_fr_a6_initial_token_has_no_limit_and_works_through_any_manager() {
    let s = TestServer::start(Mode::Dedicated).await;
    let (kid, _) = s.kernel("a.py");
    let a = initial(&s, &kid).await;
    let b = initial(&s, &kid).await; // no issue-once rule, no 409
    assert_ne!(a, b);
    let r = s.anon().post(&format!("/api/kernels/{kid}/tokens/initial"), json!({"kernel_id": kid})).await;
    assert_eq!(r.status, 201);
    assert_eq!(r.body["capabilities"], json!(ALL));
    assert_eq!(r.body["password_required"], true);
    s.anon().post(&format!("/api/kernels/{kid}/tokens/initial"), json!({"kernel_id": "k_00000000000000000001"})).await.error(400, "bad_request");
    s.anon().post("/api/kernels/not-an-id/tokens/initial", json!({})).await.error(400, "bad_request");
    s.anon().post("/api/kernels/k_00000000000000000000/tokens/initial", json!({})).await.error(404, "not_found");

    // The token is held by the kernel, so a second manager of the same account accepts it.
    let other = TestServer::start_cfg(
        ServerConfig::ephemeral(s.scratch.join("other-home")),
        s.backend.clone(),
        scratch("other"),
    )
    .await;
    assert_eq!(other.with_token(&a).get(&format!("/api/kernels/{kid}")).await.status, 200);
    // ... but only for its own kernel.
    let (kid2, _) = s.kernel("b.py");
    other.with_token(&a).get(&format!("/api/kernels/{kid2}")).await.error(401, "unauthorized");
    // It names a kernel in every path it works on, so it cannot start kernels or read by path.
    s.with_token(&a).post("/api/kernels", json!({"path": "/x.py"})).await.error(401, "unauthorized");
}

#[tokio::test]
async fn test_fr_a6_verify_reports_validity_and_revocation() {
    let s = TestServer::start(Mode::Dedicated).await;
    let (kid, _) = s.kernel("a.py");
    let tok = initial(&s, &kid).await;
    let r = s.with_token(&tok).get(&format!("/api/kernels/{kid}/tokens/{tok}/verify")).await;
    assert_eq!(r.status, 200, "{}", r.text);
    assert_eq!(r.body["valid"], true);
    assert_eq!(r.body["kernel_id"], kid.as_str());
    assert_eq!(r.body["capabilities"], json!(ALL));
    assert_eq!(r.body["blacklisted"], false);
    assert_eq!(r.body["password_set"], false);
    set_first_password(&s, &kid, "pw").await;
    let r = s.with_token(&tok).get(&format!("/api/kernels/{kid}/tokens/{tok}/verify")).await;
    assert_eq!(r.body["password_set"], true);

    // A token the kernel does not know is 401; a revoked one is 401 token_revoked.
    s.admin().get(&format!("/api/kernels/{kid}/tokens/nope/verify")).await.error(401, "unauthorized");
    let issued: Value = s.backend.request_for_test(&kid, "tokens.issue", json!({"kind": "share", "capabilities": ["read"]})).await;
    let share_tok = issued["token"].as_str().unwrap().to_string();
    let share_id = issued["share_id"].as_str().unwrap().to_string();
    assert_eq!(s.with_token(&share_tok).get(&format!("/api/kernels/{kid}")).await.status, 200);
    s.backend.request_for_test(&kid, "tokens.revoke", json!({"share_id": share_id})).await;
    s.with_token(&share_tok).get(&format!("/api/kernels/{kid}")).await.error(401, "token_revoked");
    s.admin().get(&format!("/api/kernels/{kid}/tokens/{share_tok}/verify")).await.error(401, "token_revoked");
}

/// A share made with `POST /shares` lives in the manager database. FR-A6 (INTENT D17) says it is the
/// same thing as a kernel-issued share token, so `verify` must accept it (#113).
#[tokio::test]
async fn test_fr_a6_verify_accepts_manager_db_shares() {
    let s = TestServer::start(Mode::Dedicated).await;
    let (kid, _) = s.kernel("a.py");
    let (other, _) = s.kernel("b.py");
    let a = s.admin();
    let c = a.post(&format!("/api/kernels/{kid}/shares"), json!({"capabilities": ["read", "history"]})).await;
    assert_eq!(c.status, 201, "{}", c.text);
    let tok = c.body["token"].as_str().unwrap().to_string();
    let sid = c.body["share_id"].as_str().unwrap().to_string();
    let verify = format!("/api/kernels/{kid}/tokens/{tok}/verify");

    // The share's own token and the master token both read it back, with the share's capabilities.
    for api in [s.with_token(&tok), s.admin()] {
        let r = api.get(&verify).await;
        assert_eq!(r.status, 200, "{}", r.text);
        assert_eq!(r.body["valid"], true);
        assert_eq!(r.body["kernel_id"], kid.as_str());
        assert_eq!(r.body["capabilities"], json!(["read", "history"]));
        assert_eq!(r.body["blacklisted"], false);
    }
    // A token of another kernel is refused, with the same answer as an unknown token.
    s.admin().get(&format!("/api/kernels/{other}/tokens/{tok}/verify")).await.error(401, "unauthorized");
    s.with_token(&tok).get(&format!("/api/kernels/{other}/tokens/{tok}/verify")).await.error(404, "not_found");
    s.admin().get(&format!("/api/kernels/{kid}/tokens/nope/verify")).await.error(401, "unauthorized");

    // Revoked: the share is gone from the list, and both verify and use say token_revoked.
    assert_eq!(a.delete(&format!("/api/kernels/{kid}/shares/{sid}")).await.status, 204);
    a.get(&verify).await.error(401, "token_revoked");
    s.with_token(&tok).get(&format!("/api/kernels/{kid}")).await.error(401, "token_revoked");
    let listed = a.get(&format!("/api/kernels/{kid}/shares")).await.body["shares"].clone();
    assert!(listed.as_array().unwrap().iter().all(|x| x["share_id"] != sid.as_str()), "{listed}");
    // A revoked token of another kernel does not tell that it ever existed.
    a.get(&format!("/api/kernels/{other}/tokens/{tok}/verify")).await.error(401, "unauthorized");
}

// ------------------------------------------------------------------ attempt limit (FR-A4, #87)

/// Two free attempts, then a 300 ms lock that doubles with every further failure.
async fn limited() -> TestServer {
    TestServer::start_with(Mode::Dedicated, |c| {
        c.password_free_attempts = 2;
        c.password_delay_base = Duration::from_millis(300);
    })
    .await
}

#[tokio::test]
async fn test_fr_a4_repeated_wrong_passwords_are_delayed_with_429() {
    let s = limited().await;
    let (kid, _) = s.kernel("a.py");
    set_first_password(&s, &kid, "right").await;
    basic(&s, "GET", "/api/auth", "wrong 1").await.error(401, "unauthorized");
    basic(&s, "GET", "/api/auth", "wrong 2").await.error(401, "unauthorized");
    let r = basic(&s, "GET", "/api/auth", "wrong 3").await;
    let e = r.error(429, "too_many_attempts");
    assert!(e["data"]["retry_after"].as_u64().unwrap() >= 1, "{}", r.text);
    let header = r.headers.get("retry-after").expect("Retry-After header");
    assert_eq!(header.to_str().unwrap(), e["data"]["retry_after"].as_u64().unwrap().to_string());
}

#[tokio::test]
async fn test_fr_a4_locked_manager_refuses_the_correct_password_until_it_expires() {
    let s = limited().await;
    let (kid, _) = s.kernel("a.py");
    set_first_password(&s, &kid, "right").await;
    basic(&s, "GET", "/api/auth", "wrong 1").await.error(401, "unauthorized");
    basic(&s, "GET", "/api/auth", "wrong 2").await.error(401, "unauthorized");
    // Locked: even the right password is refused, without a PBKDF2 round.
    basic(&s, "GET", "/api/auth", "right").await.error(429, "too_many_attempts");
    tokio::time::sleep(Duration::from_millis(400)).await;
    let r = basic(&s, "GET", "/api/auth", "right").await;
    assert_eq!(r.status, 200, "{}", r.text);
    // A success clears the counter: two more wrong tries are plain 401 again.
    basic(&s, "GET", "/api/auth", "wrong 1").await.error(401, "unauthorized");
    basic(&s, "GET", "/api/auth", "wrong 2").await.error(401, "unauthorized");
}

#[tokio::test]
async fn test_fr_a4_login_paths_share_one_attempt_counter() {
    let s = limited().await;
    let (kid, _) = s.kernel("a.py");
    set_first_password(&s, &kid, "right").await;
    basic(&s, "GET", "/api/auth", "wrong").await.error(401, "unauthorized");
    basic(&s, "PUT", "/api/auth/tokens/master", "wrong").await.error(401, "unauthorized");
    // The kernel login sees the lock the other two paths built.
    let path = format!("/api/kernels/{kid}/tokens/auth");
    s.anon().post(&path, json!({"password": "right"})).await.error(429, "too_many_attempts");
    tokio::time::sleep(Duration::from_millis(400)).await;
    let r = s.anon().post(&path, json!({"password": "right"})).await;
    assert_eq!(r.status, 200, "{}", r.text);
}
