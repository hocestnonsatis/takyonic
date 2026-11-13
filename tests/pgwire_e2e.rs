//! Single-node PostgreSQL wire protocol test.
//!
//! Starts the real `takyonic-server` binary and talks to it with `tokio-postgres`
//! (SCRAM-SHA-256, extended query for DML, simple query for transaction control).

use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio_postgres::NoTls;

struct Server {
    child: Child,
    pg_port: u16,
    log: Arc<Mutex<Vec<String>>>,
}

impl Drop for Server {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(Some(_))) {
            return;
        }
        let _ = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("local addr").port()
}

fn spawn_server(data_dir: &std::path::Path, pg_port: u16, raft_port: u16) -> Server {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut child = Command::new(env!("CARGO_BIN_EXE_takyonic-server"))
        .args([
            "--node-id",
            "1",
            "--bind-host",
            "127.0.0.1",
            "--pg-port",
            &pg_port.to_string(),
            "--raft-port",
            &raft_port.to_string(),
            "--data-dir",
            &data_dir.display().to_string(),
            "--no-demo-bootstrap",
            "--no-mpp",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn takyonic-server");

    let stdout = child.stdout.take().expect("stdout");
    let stderr = child.stderr.take().expect("stderr");
    let log_out = Arc::clone(&log);
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        while reader.read_line(&mut line).unwrap_or(0) > 0 {
            log_out.lock().expect("log").push(line.clone());
            line.clear();
        }
    });
    let log_err = Arc::clone(&log);
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stderr);
        let mut line = String::new();
        while reader.read_line(&mut line).unwrap_or(0) > 0 {
            log_err.lock().expect("log").push(line.clone());
            line.clear();
        }
    });

    Server {
        child,
        pg_port,
        log,
    }
}

fn db_detail(err: &tokio_postgres::Error) -> String {
    match err.as_db_error() {
        Some(db) => format!("{}: {}", db.code().code(), db.message()),
        None => err.to_string(),
    }
}

fn dump_log(server: &Server) -> String {
    server
        .log
        .lock()
        .expect("log")
        .iter()
        .cloned()
        .collect::<Vec<_>>()
        .join("")
}

fn stop_server(server: &mut Server) {
    let _ = Command::new("kill")
        .args(["-TERM", &server.child.id().to_string()])
        .status();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match server.child.try_wait() {
            Ok(Some(status)) => {
                assert!(
                    status.success(),
                    "server exited {status}\n{}",
                    dump_log(server)
                );
                return;
            }
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(50));
            }
            Ok(None) => panic!("server did not exit after SIGTERM\n{}", dump_log(server)),
            Err(e) => panic!("wait failed: {e}\n{}", dump_log(server)),
        }
    }
}

async fn connect(
    port: u16,
) -> (
    tokio_postgres::Client,
    tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
) {
    let (client, connection) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("postgres")
        .password("password")
        .dbname("postgres")
        .connect_timeout(Duration::from_secs(5))
        .connect(NoTls)
        .await
        .expect("connect");
    let handle = tokio::spawn(async move { connection.await });
    (client, handle)
}

async fn connect_retry(server: &mut Server) -> tokio_postgres::Client {
    let deadline = Instant::now() + Duration::from_secs(45);
    let mut last_err = String::new();
    while Instant::now() < deadline {
        if let Ok(Some(status)) = server.child.try_wait() {
            panic!(
                "server exited early ({status}) before accepting pgwire\n{}",
                dump_log(server)
            );
        }
        match tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(server.pg_port)
            .user("postgres")
            .password("password")
            .dbname("postgres")
            .connect_timeout(Duration::from_secs(2))
            .connect(NoTls)
            .await
        {
            Ok((client, connection)) => {
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                return client;
            }
            Err(e) => {
                last_err = e.to_string();
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    panic!("timed out connecting: {last_err}\n{}", dump_log(server));
}

#[tokio::test]
async fn single_node_pgwire_crud_transactions_and_restart() {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let root: PathBuf = std::env::temp_dir().join(format!("takyonic-pgwire-e2e-{nanos}"));
    std::fs::create_dir_all(&root).expect("data dir");
    let pg_port = free_port();
    let raft_port = free_port();

    let mut server = spawn_server(&root, pg_port, raft_port);
    let client = connect_retry(&mut server).await;

    let rejected = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(pg_port)
        .user("postgres")
        .password("wrong-password")
        .dbname("postgres")
        .connect_timeout(Duration::from_secs(5))
        .connect(NoTls)
        .await;
    assert!(
        rejected.is_err(),
        "wrong password was accepted\n{}",
        dump_log(&server)
    );

    let bad_db = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(pg_port)
        .user("postgres")
        .password("password")
        .dbname("appdb")
        .connect_timeout(Duration::from_secs(5))
        .connect(NoTls)
        .await;
    assert!(
        bad_db.is_err(),
        "non-postgres database was accepted\n{}",
        dump_log(&server)
    );

    client
        .batch_execute("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT, qty INT)")
        .await
        .expect("create table");

    let inserted = client
        .execute(
            "INSERT INTO items (id, name, qty) VALUES ($1, $2, $3), ($4, $5, $6), ($7, $8, $9)",
            &[
                &1i64, &"bolt", &10i32, &2i64, &"nut", &3i32, &3i64, &"washer", &7i32,
            ],
        )
        .await
        .expect("insert");
    assert_eq!(inserted, 3, "INSERT row count");

    let rows = client
        .query(
            "SELECT id, name, qty FROM items WHERE qty >= $1 ORDER BY name ASC LIMIT 2",
            &[&5i32],
        )
        .await
        .expect("select where/order/limit");
    assert_eq!(rows.len(), 2);
    let name0: String = rows[0].get(1);
    let name1: String = rows[1].get(1);
    assert_eq!(name0, "bolt");
    assert_eq!(name1, "washer");
    let qty0: i32 = rows[0].get(2);
    assert_eq!(qty0, 10);

    let star = client
        .query("SELECT * FROM items WHERE id = 2", &[])
        .await
        .expect("select star");
    assert_eq!(star.len(), 1);
    let star_id: i64 = star[0].get(0);
    let star_name: String = star[0].get(1);
    let star_qty: i32 = star[0].get(2);
    assert_eq!(star_id, 2);
    assert_eq!(star_name, "nut");
    assert_eq!(star_qty, 3);

    let empty = client
        .query("SELECT id, name FROM items WHERE id = $1", &[&99i64])
        .await
        .expect("empty select");
    assert!(empty.is_empty());

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .execute(
            "INSERT INTO items (id, name, qty) VALUES ($1, $2, $3)",
            &[&50i64, &"hidden", &1i32],
        )
        .await
        .expect("uncommitted insert");

    let (reader, _reader_conn) = connect(pg_port).await;
    let hidden = reader
        .query("SELECT id FROM items WHERE id = 50", &[])
        .await
        .expect("concurrent read");
    assert!(
        hidden.is_empty(),
        "uncommitted insert was visible to another session"
    );

    client.batch_execute("ROLLBACK").await.expect("rollback");
    let after_rb = client
        .query("SELECT id FROM items WHERE id = 50", &[])
        .await
        .expect("after rollback");
    assert!(after_rb.is_empty());

    client
        .batch_execute(
            "BEGIN; INSERT INTO items (id, name, qty) VALUES (51, 'script', 4); ROLLBACK",
        )
        .await
        .expect("script rollback");
    let script_gone = client
        .query("SELECT id FROM items WHERE id = 51", &[])
        .await
        .expect("script row");
    assert!(script_gone.is_empty());

    client.batch_execute("BEGIN").await.expect("begin 2");
    let bad = client
        .batch_execute("SELECT * FROM no_such_table")
        .await
        .expect_err("missing table");
    let bad_msg = db_detail(&bad);
    assert!(
        bad_msg.contains("no_such_table") || bad_msg.contains("does not exist"),
        "unexpected error: {bad_msg}"
    );
    let ignored = client
        .execute(
            "INSERT INTO items (id, name, qty) VALUES ($1, $2, $3)",
            &[&8i64, &"ignored", &1i32],
        )
        .await
        .expect_err("statement in aborted transaction");
    let ignored_msg = db_detail(&ignored);
    assert!(
        ignored_msg.contains("aborted") || ignored_msg.contains("25P02"),
        "expected aborted-txn error, got {ignored_msg}"
    );
    client
        .batch_execute("ROLLBACK")
        .await
        .expect("rollback aborted txn");
    let ignored_row = client
        .query("SELECT id FROM items WHERE id = 8", &[])
        .await
        .expect("ignored row");
    assert!(ignored_row.is_empty());

    let updated = client
        .execute("UPDATE items SET qty = $1 WHERE id = $2", &[&42i32, &1i64])
        .await
        .expect("update");
    assert_eq!(updated, 1);
    let deleted = client
        .execute("DELETE FROM items WHERE name = $1", &[&"nut"])
        .await
        .expect("delete");
    assert_eq!(deleted, 1);

    client.batch_execute("BEGIN").await.expect("begin 3");
    client
        .execute(
            "INSERT INTO items (id, name, qty) VALUES ($1, $2, $3)",
            &[&9i64, &"kept", &5i32],
        )
        .await
        .expect("kept insert");
    client.batch_execute("COMMIT").await.expect("commit");

    let final_rows = client
        .query("SELECT id, name, qty FROM items ORDER BY id", &[])
        .await
        .expect("final select");
    let got: Vec<(i64, String, i32)> = final_rows
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    assert_eq!(
        got,
        vec![
            (1, "bolt".to_string(), 42),
            (3, "washer".to_string(), 7),
            (9, "kept".to_string(), 5),
        ]
    );

    drop(client);
    drop(reader);
    stop_server(&mut server);

    let mut restarted = spawn_server(&root, pg_port, raft_port);
    let client = connect_retry(&mut restarted).await;
    let persisted = client
        .query("SELECT id, name, qty FROM items ORDER BY id", &[])
        .await
        .expect("select after restart");
    let got: Vec<(i64, String, i32)> = persisted
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    assert_eq!(
        got,
        vec![
            (1, "bolt".to_string(), 42),
            (3, "washer".to_string(), 7),
            (9, "kept".to_string(), 5),
        ],
        "durable rows after restart\n{}",
        dump_log(&restarted)
    );

    let _ = std::fs::remove_dir_all(&root);
}
