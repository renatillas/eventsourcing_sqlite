import eventsourcing
import eventsourcing_sqlite
import example_bank_account
import gleam/erlang/process
import gleam/option.{type Option, None, Some}
import gleam/otp/static_supervisor
import gleam/time/timestamp
import gleeunit
import gleeunit/should
import sqlight

const recv_timeout = 5000

const db_connection = "file:testdb?mode=memory&cache=shared"

pub fn main() {
  gleeunit.main()
}

fn sqlite_store() {
  eventsourcing_sqlite.new(
    sqlight_connection_string: db_connection,
    event_encoder: example_bank_account.event_encoder,
    event_decoder: example_bank_account.event_decoder(),
    event_type: example_bank_account.bank_account_event_type,
    event_version: "1.0",
    aggregate_type: example_bank_account.bank_account_type,
    entity_encoder: example_bank_account.entity_encoder,
    entity_decoder: example_bank_account.entity_decoder(),
  )
}

fn delete_from_db(table, connection) {
  sqlight.exec("DELETE FROM " <> table <> ";", connection)
}

pub fn sqlite_store_test() {
  let sqlite_store = sqlite_store()

  let assert Ok(db) = sqlight.open(db_connection)

  let name = process.new_name("test_eventsourcing")
  let query_name = process.new_name("test_query")
  let query = fn(_, _) { Nil }
  let assert Ok(freq) = eventsourcing.frequency(1)

  eventsourcing_sqlite.create_event_table(sqlite_store.eventstore)
  |> should.be_ok
  eventsourcing_sqlite.create_snapshot_table(sqlite_store.eventstore)
  |> should.be_ok

  let assert Ok(spec) =
    eventsourcing.supervised(
      name: name,
      eventstore: sqlite_store,
      handle: example_bank_account.handle,
      apply: example_bank_account.apply,
      empty_state: example_bank_account.BankAccount(opened: False, balance: 0.0),
      queries: [#(query_name, query)],
      snapshot_config: Some(eventsourcing.SnapshotConfig(freq)),
    )

  let assert Ok(_) =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(spec)
    |> static_supervisor.start()

  let actor = process.named_subject(name)

  happy_path(actor)
  delete_from_db("event", db)
  |> should.be_ok()
  delete_from_db("snapshot", db)
  |> should.be_ok()

  load_events(actor)
  delete_from_db("event", db)
  |> should.be_ok()
  delete_from_db("snapshot", db)
  |> should.be_ok()

  snapshots_happy_path(actor)
  delete_from_db("event", db)
  |> should.be_ok()
  delete_from_db("snapshot", db)
  |> should.be_ok()

  snapshot_edge_cases(actor)
  delete_from_db("event", db)
  |> should.be_ok()
  delete_from_db("snapshot", db)
  |> should.be_ok()

  snapshot_error_cases(actor, sqlite_store.eventstore)
  delete_from_db("event", db)
  |> should.be_ok()
  delete_from_db("snapshot", db)
  |> should.be_ok()
}

fn happy_path(actor) {
  eventsourcing.execute_with_response(
    actor,
    "92085b42-032c-4d7a-84de-a86d67123858",
    example_bank_account.OpenAccount("92085b42-032c-4d7a-84de-a86d67123858"),
    [],
  )
  |> process.receive(recv_timeout)
  |> should.be_ok
  |> should.be_ok
  |> should.equal(Nil)

  eventsourcing.execute_with_response(
    actor,
    "92085b42-032c-4d7a-84de-a86d67123858",
    example_bank_account.DepositMoney(10.0),
    [],
  )
  |> process.receive(recv_timeout)
  |> should.be_ok
  |> should.be_ok
  |> should.equal(Nil)

  eventsourcing.execute_with_response(
    actor,
    "92085b42-032c-4d7a-84de-a86d67123858",
    example_bank_account.WithDrawMoney(5.99),
    [],
  )
  |> process.receive(recv_timeout)
  |> should.be_ok
  |> should.be_ok
  |> should.equal(Nil)

  eventsourcing.load_aggregate(actor, "92085b42-032c-4d7a-84de-a86d67123858")
  |> process.receive(recv_timeout)
  |> should.be_ok
  |> should.be_ok
}

fn load_events(actor) {
  eventsourcing.execute_with_response(
    actor,
    "92085b42-032c-4d7a-84de-a86d67123858",
    example_bank_account.OpenAccount("92085b42-032c-4d7a-84de-a86d67123858"),
    [#("meta", "data")],
  )
  |> process.receive(recv_timeout)
  |> should.be_ok
  |> should.be_ok
  |> should.equal(Nil)

  eventsourcing.execute_with_response(
    actor,
    "92085b42-032c-4d7a-84de-a86d67123858",
    example_bank_account.DepositMoney(10.0),
    [],
  )
  |> process.receive(recv_timeout)
  |> should.be_ok
  |> should.be_ok
  |> should.equal(Nil)

  eventsourcing.execute_with_response(
    actor,
    "92085b42-032c-4d7a-84de-a86d67123858",
    example_bank_account.WithDrawMoney(5.99),
    [],
  )
  |> process.receive(recv_timeout)
  |> should.be_ok
  |> should.be_ok
  |> should.equal(Nil)

  eventsourcing.load_events(actor, "92085b42-032c-4d7a-84de-a86d67123858")
  |> process.receive(recv_timeout)
  |> should.be_ok
  |> should.be_ok
}

fn snapshots_happy_path(actor) {
  eventsourcing.execute_with_response(
    actor,
    "92085b42-032c-4d7a-84de-a86d67123858",
    example_bank_account.OpenAccount("92085b42-032c-4d7a-84de-a86d67123858"),
    [],
  )
  |> process.receive(recv_timeout)
  |> should.be_ok
  |> should.be_ok
  |> should.equal(Nil)

  eventsourcing.execute_with_response(
    actor,
    "92085b42-032c-4d7a-84de-a86d67123858",
    example_bank_account.DepositMoney(10.0),
    [],
  )
  |> process.receive(recv_timeout)
  |> should.be_ok
  |> should.be_ok
  |> should.equal(Nil)

  eventsourcing.execute_with_response(
    actor,
    "92085b42-032c-4d7a-84de-a86d67123858",
    example_bank_account.WithDrawMoney(5.99),
    [],
  )
  |> process.receive(recv_timeout)
  |> should.be_ok
  |> should.be_ok
  |> should.equal(Nil)

  eventsourcing.latest_snapshot(
    actor,
    aggregate_id: "92085b42-032c-4d7a-84de-a86d67123858",
  )
  |> process.receive(recv_timeout)
  |> should.be_ok
  |> should.be_ok
  |> fn(
    snapshot: Option(eventsourcing.Snapshot(example_bank_account.BankAccount)),
  ) {
    let assert Some(eventsourcing.Snapshot(_, entity, sequence, _)) = snapshot
    entity.balance |> should.equal(4.01)
    sequence |> should.equal(3)
  }
}

fn snapshot_edge_cases(actor) {
  // Test Case 1: Non-existent aggregate
  eventsourcing.latest_snapshot(actor, aggregate_id: "non-existent-id")
  |> process.receive(recv_timeout)
  |> should.be_ok
  |> should.equal(Ok(None))

  // Test Case 2: Create and update snapshot

  // Open account
  eventsourcing.execute_with_response(
    actor,
    "snapshot-test-id",
    example_bank_account.OpenAccount("snapshot-test-id"),
    [],
  )
  |> process.receive(recv_timeout)
  |> should.be_ok
  |> should.be_ok

  // First snapshot should exist
  eventsourcing.latest_snapshot(actor, aggregate_id: "snapshot-test-id")
  |> process.receive(recv_timeout)
  |> should.be_ok
  |> should.be_ok
  |> fn(snapshot) {
    let assert Some(eventsourcing.Snapshot(_, entity, sequence, _)) = snapshot
    let assert example_bank_account.BankAccount(opened: True, balance: 0.0) =
      entity
    sequence |> should.equal(1)
  }

  // Test Case 3: Multiple updates in sequence
  eventsourcing.execute_with_response(
    actor,
    "snapshot-test-id",
    example_bank_account.DepositMoney(100.0),
    [],
  )
  |> process.receive(recv_timeout)
  |> should.be_ok
  |> should.be_ok

  eventsourcing.execute_with_response(
    actor,
    "snapshot-test-id",
    example_bank_account.WithDrawMoney(30.0),
    [],
  )
  |> process.receive(recv_timeout)
  |> should.be_ok
  |> should.be_ok

  // Verify final snapshot state
  eventsourcing.latest_snapshot(actor, aggregate_id: "snapshot-test-id")
  |> process.receive(recv_timeout)
  |> should.be_ok
  |> should.be_ok
  |> fn(snapshot) {
    let assert Some(eventsourcing.Snapshot(_, entity, sequence, _)) = snapshot
    let assert example_bank_account.BankAccount(opened: True, balance: 70.0) =
      entity
    sequence |> should.equal(3)
  }

  // Test Case 4: Verify snapshot with empty metadata
  eventsourcing.execute_with_response(
    actor,
    "snapshot-test-id",
    example_bank_account.DepositMoney(30.0),
    [],
  )
  |> process.receive(recv_timeout)
  |> should.be_ok
  |> should.be_ok

  // Test Case 5: Verify snapshot with metadata
  eventsourcing.execute_with_response(
    actor,
    "snapshot-test-id",
    example_bank_account.WithDrawMoney(20.0),
    [#("operation", "withdrawal"), #("reason", "test")],
  )
  |> process.receive(recv_timeout)
  |> should.be_ok
  |> should.be_ok

  // Final state verification
  eventsourcing.latest_snapshot(actor, aggregate_id: "snapshot-test-id")
  |> process.receive(recv_timeout)
  |> should.be_ok
  |> should.be_ok
  |> fn(snapshot) {
    let assert Some(eventsourcing.Snapshot(_, entity, sequence, ts)) = snapshot
    let assert example_bank_account.BankAccount(opened: True, balance: 80.0) =
      entity
    sequence |> should.equal(5)
    ts |> should.not_equal(timestamp.unix_epoch)
  }
}

fn snapshot_error_cases(
  actor,
  event_store: eventsourcing_sqlite.SqliteStore(_, _, _, _),
) {
  let assert Ok(db) = sqlight.open(db_connection)
  sqlight.exec("DROP TABLE snapshot", db)
  |> should.be_ok()

  // Test Case 1: Attempt operations before table creation
  eventsourcing.latest_snapshot(actor, aggregate_id: "error-test-id")
  |> process.receive(recv_timeout)
  |> should.be_ok
  |> should.be_error

  // Create tables and test error cases
  eventsourcing_sqlite.create_event_table(event_store)
  |> should.be_ok
  eventsourcing_sqlite.create_snapshot_table(event_store)
  |> should.be_ok

  let account_id = "error-test-id"

  // Test Case 2: Operations on unopened account
  eventsourcing.execute_with_response(
    actor,
    account_id,
    example_bank_account.WithDrawMoney(100.0),
    [],
  )
  |> process.receive(recv_timeout)
  |> should.be_ok
  |> should.be_error

  // Test Case 3: Invalid operation sequence
  eventsourcing.execute_with_response(
    actor,
    account_id,
    example_bank_account.OpenAccount(account_id),
    [],
  )
  |> process.receive(recv_timeout)
  |> should.be_ok
  |> should.be_ok

  // Attempt to withdraw more than balance
  eventsourcing.execute_with_response(
    actor,
    account_id,
    example_bank_account.WithDrawMoney(100.0),
    [],
  )
  |> process.receive(recv_timeout)
  |> should.be_ok
  |> should.be_error

  // Verify snapshot still reflects valid state
  eventsourcing.latest_snapshot(actor, aggregate_id: account_id)
  |> process.receive(recv_timeout)
  |> should.be_ok
  |> should.be_ok
  |> fn(snapshot) {
    let assert Some(eventsourcing.Snapshot(_, entity, sequence, _)) = snapshot
    let assert example_bank_account.BankAccount(opened: True, balance: 0.0) =
      entity
    sequence |> should.equal(1)
  }
}
