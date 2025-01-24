import eventsourcing
import gleam/dynamic/decode
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{None}
import gleam/pair
import gleam/result
import gleam/string
import sqlight

// CONSTANTS ----

const batch_insert_events_query = "
  INSERT INTO event 
  (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata)
  VALUES 
"

const select_events_query = "
  SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata
  FROM event
  WHERE aggregate_type = $1 
    AND aggregate_id = $2
    AND sequence > $3
  ORDER BY sequence
  "

const create_event_table_query = "
  CREATE TABLE IF NOT EXISTS event
  (
    aggregate_type text                         NOT NULL,
    aggregate_id   text                         NOT NULL,
    sequence       bigint CHECK (sequence >= 0) NOT NULL,
    event_type     text                         NOT NULL,
    event_version  text                         NOT NULL,
    payload        text                         NOT NULL,
    metadata       text,
    PRIMARY KEY (aggregate_type, aggregate_id, sequence)
  );
  "

const create_snapshot_table_query = "
  CREATE TABLE IF NOT EXISTS snapshot
  (
    aggregate_type text                         NOT NULL,
    aggregate_id   text                         NOT NULL,
    sequence       bigint CHECK (sequence >= 0) NOT NULL,
    entity         text                         NOT NULL,
    timestamp      int                          NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id)
  );
  "

const save_snapshot_query = "
  INSERT INTO snapshot (aggregate_type, aggregate_id, sequence, entity, timestamp)
  VALUES ($1, $2, $3, $4, $5)
  ON CONFLICT (aggregate_type, aggregate_id)
  DO UPDATE SET
    sequence = EXCLUDED.sequence,
    entity = EXCLUDED.entity
  "

const select_snapshot_query = "
  SELECT aggregate_type, aggregate_id, sequence, entity, timestamp
  FROM snapshot
  WHERE aggregate_type = $1 
    AND aggregate_id = $2
"

// TYPES ----

@internal
pub type SqliteStore(entity, command, event, error) {
  SqliteStore(
    connection_string: String,
    event_encoder: fn(event) -> String,
    event_decoder: decode.Decoder(event),
    event_type: String,
    event_version: String,
    aggregate_type: String,
    entity_encoder: fn(entity) -> String,
    entity_decoder: decode.Decoder(entity),
  )
}

pub type Metadata =
  List(#(String, String))

// CONSTRUCTORS ----

pub fn new(
  sqlight_connection_string connection_string: String,
  event_encoder event_encoder: fn(event) -> String,
  event_decoder event_decoder: decode.Decoder(event),
  event_type event_type: String,
  event_version event_version: String,
  aggregate_type aggregate_type: String,
  entity_encoder entity_encoder: fn(entity) -> String,
  entity_decoder entity_decoder: decode.Decoder(entity),
) -> eventsourcing.EventStore(
  SqliteStore(entity, command, event, error),
  entity,
  command,
  event,
  error,
  sqlight.Connection,
) {
  let eventstore =
    SqliteStore(
      connection_string:,
      event_encoder:,
      event_decoder:,
      event_type:,
      event_version:,
      aggregate_type:,
      entity_encoder:,
      entity_decoder:,
    )

  eventsourcing.EventStore(
    eventstore:,
    load_events: fn(sqlite_store, tx, aggregate_id, start_from) {
      load_events(sqlite_store, tx, aggregate_id, start_from)
    },
    commit_events: fn(tx, aggregate_id, events, metadata) {
      commit_events(eventstore, tx, aggregate_id, events, metadata)
    },
    load_snapshot: fn(tx, aggregate_id) {
      load_snapshot(eventstore, tx, aggregate_id)
    },
    save_snapshot: fn(tx, snapshot) { save_snapshot(eventstore, tx, snapshot) },
    execute_transaction: execute_in_transaction(connection_string),
    load_aggregate_transaction: execute_in_transaction(connection_string),
    get_latest_snapshot_transaction: execute_in_transaction(connection_string),
    load_events_transaction: execute_in_transaction(connection_string),
  )
}

pub fn create_event_table(
  sqlite_store: SqliteStore(entity, command, event, error),
) {
  use db <- result.try(
    sqlight.open(sqlite_store.connection_string)
    |> result.map_error(fn(error) {
      eventsourcing.EventStoreError(
        "Failed to open connection: " <> string.inspect(error),
      )
    }),
  )
  sqlight.exec(create_event_table_query, on: db)
  |> result.map_error(fn(error) {
    eventsourcing.EventStoreError(
      "Failed to create event table: " <> string.inspect(error),
    )
  })
}

pub fn create_snapshot_table(
  sqlite_store: SqliteStore(entity, command, event, error),
) -> Result(Nil, eventsourcing.EventSourcingError(error)) {
  use db <- result.try(
    sqlight.open(sqlite_store.connection_string)
    |> result.map_error(fn(error) {
      eventsourcing.EventStoreError(
        "Failed to open connection: " <> string.inspect(error),
      )
    }),
  )
  sqlight.exec(create_snapshot_table_query, on: db)
  |> result.map(fn(_) { Nil })
  |> result.map_error(fn(error) {
    eventsourcing.EventStoreError(
      "Failed to create snapshot table: " <> string.inspect(error),
    )
  })
}

pub fn load_events(
  sqlite_store: SqliteStore(entity, command, event, error),
  tx,
  aggregate_id: eventsourcing.AggregateId,
  start_from: Int,
) {
  use _ <- result.try(
    sqlight.exec("BEGIN;", on: tx)
    |> result.map_error(fn(error) {
      eventsourcing.EventStoreError(string.inspect(error))
    }),
  )

  let row_decoder = {
    use aggregate_id <- decode.field(1, decode.string)
    use sequence <- decode.field(2, decode.int)
    use payload <- decode.field(5, {
      use payload_string <- decode.then(decode.string)
      let assert Ok(payload) =
        json.parse(payload_string, sqlite_store.event_decoder)
      decode.success(payload)
    })

    use metadata <- decode.field(6, metadata_decoder())
    use event_type <- decode.field(4, decode.string)
    use event_version <- decode.field(0, decode.string)
    use aggregate_type <- decode.field(3, decode.string)
    decode.success(eventsourcing.SerializedEventEnvelop(
      aggregate_id:,
      sequence:,
      payload:,
      metadata:,
      event_type:,
      event_version:,
      aggregate_type:,
    ))
  }
  sqlight.query(
    select_events_query,
    on: tx,
    with: [
      sqlight.text(sqlite_store.aggregate_type),
      sqlight.text(aggregate_id),
      sqlight.int(start_from),
    ],
    expecting: row_decoder,
  )
  |> result.map_error(fn(error) {
    eventsourcing.EventStoreError(string.inspect(error))
  })
}

fn commit_events(
  sqlite_store: SqliteStore(entity, command, event, error),
  tx,
  aggregate: eventsourcing.Aggregate(entity, command, event, error),
  events: List(event),
  metadata: List(#(String, String)),
) -> Result(
  #(List(eventsourcing.EventEnvelop(event)), Int),
  eventsourcing.EventSourcingError(error),
) {
  let eventsourcing.Aggregate(aggregate_id, _, sequence) = aggregate

  let wrapped_events =
    wrap_events(sqlite_store, aggregate_id, events, sequence, metadata)
  let assert Ok(last_event) = list.last(wrapped_events)

  let events =
    persist_events(sqlite_store, tx, wrapped_events)
    |> result.map(fn(_) { #(wrapped_events, last_event.sequence) })

  use _ <- result.try(
    sqlight.exec("COMMIT;", on: tx)
    |> result.map_error(fn(error) {
      eventsourcing.EventStoreError(string.inspect(error))
    }),
  )

  events
  |> result.map_error(fn(error) {
    sqlight.exec("ROLLBACK;", on: tx)
    eventsourcing.EventStoreError(string.inspect(error))
  })
}

fn wrap_events(
  postgres_store: SqliteStore(entity, command, event, error),
  aggregate_id: eventsourcing.AggregateId,
  events: List(event),
  sequence: Int,
  metadata: Metadata,
) -> List(eventsourcing.EventEnvelop(event)) {
  list.map_fold(
    over: events,
    from: sequence,
    with: fn(sequence: Int, event: event) {
      let next_sequence = sequence + 1
      #(
        next_sequence,
        eventsourcing.SerializedEventEnvelop(
          aggregate_id:,
          sequence: sequence + 1,
          payload: event,
          event_type: postgres_store.event_type,
          event_version: postgres_store.event_version,
          aggregate_type: postgres_store.aggregate_type,
          metadata: metadata,
        ),
      )
    },
  )
  |> pair.second
}

fn persist_events(
  sqlite_store: SqliteStore(entity, command, event, error),
  tx,
  wrapped_events: List(eventsourcing.EventEnvelop(event)),
) {
  // Generate the placeholders for batch insert
  let #(placeholders, params, _) =
    list.index_fold(wrapped_events, #("", [], 0), fn(acc, event, index) {
      let #(placeholders, params, _) = acc
      let offset = index * 7
      // 7 parameters per event
      let row_placeholders =
        "($"
        <> string.join(
          list.range(offset + 1, offset + 7)
            |> list.map(int.to_string),
          ", $",
        )
        <> ")"

      let sep = case placeholders {
        "" -> ""
        _ -> ", "
      }

      let assert eventsourcing.SerializedEventEnvelop(
        aggregate_id,
        sequence,
        payload,
        metadata,
        event_type,
        event_version,
        aggregate_type,
      ) = event

      let new_params = [
        sqlight.text(aggregate_type),
        sqlight.text(aggregate_id),
        sqlight.int(sequence),
        sqlight.text(event_type),
        sqlight.text(event_version),
        sqlight.text(payload |> sqlite_store.event_encoder),
        sqlight.text(metadata |> metadata_encoder),
      ]

      #(
        placeholders <> sep <> row_placeholders,
        list.append(params, new_params),
        index + 1,
      )
    })

  // If no events to insert, return early
  case wrapped_events {
    [] -> Ok(Nil)
    _ -> {
      let query = batch_insert_events_query <> placeholders
      sqlight.query(query, on: tx, with: params, expecting: decode.dynamic)
      |> result.map(fn(_) { Nil })
      |> result.map_error(fn(error) {
        eventsourcing.EventStoreError(
          "Failed to insert events: " <> string.inspect(error),
        )
      })
    }
  }
}

fn metadata_encoder(metadata: Metadata) -> String {
  json.array(metadata, fn(row) {
    json.preprocessed_array([json.string(row.0), json.string(row.1)])
  })
  |> json.to_string
}

fn metadata_decoder() {
  use stringmetadata <- decode.then(decode.string)
  let assert Ok(listmetadata) =
    json.parse(stringmetadata, decode.list(decode.list(decode.string)))

  list.map(listmetadata, fn(metadata) {
    let assert [key, val] = metadata
    #(key, val)
  })
  |> decode.success
}

fn load_snapshot(
  sqlite_store: SqliteStore(entity, command, event, error),
  tx,
  aggregate_id: eventsourcing.AggregateId,
) {
  let row_decoder = {
    use aggregate_id <- decode.field(1, decode.string)
    use sequence <- decode.field(2, decode.int)
    use entity <- decode.field(3, {
      use entity_string <- decode.then(decode.string)
      let assert Ok(entity) =
        json.parse(entity_string, sqlite_store.entity_decoder)
      decode.success(entity)
    })
    use timestamp <- decode.field(4, decode.int)
    decode.success(eventsourcing.Snapshot(
      aggregate_id:,
      sequence:,
      entity:,
      timestamp:,
    ))
  }

  sqlight.query(
    select_snapshot_query,
    on: tx,
    with: [
      sqlight.text(sqlite_store.aggregate_type),
      sqlight.text(aggregate_id),
    ],
    expecting: row_decoder,
  )
  |> result.map(fn(response) {
    case response {
      [] -> None
      [snapshot, ..] -> option.Some(snapshot)
    }
  })
  |> result.map_error(fn(error) {
    eventsourcing.EventStoreError(string.inspect(error))
  })
}

fn save_snapshot(
  sqlite_store: SqliteStore(entity, command, event, error),
  tx,
  snapshot: eventsourcing.Snapshot(entity),
) {
  let eventsourcing.Snapshot(aggregate_id, entity, sequence, timestamp) =
    snapshot
  sqlight.query(
    save_snapshot_query,
    on: tx,
    with: [
      sqlight.text(sqlite_store.aggregate_type),
      sqlight.text(aggregate_id),
      sqlight.int(sequence),
      sqlight.text(entity |> sqlite_store.entity_encoder),
      sqlight.int(timestamp),
    ],
    expecting: decode.dynamic,
  )
  |> result.map_error(fn(error) {
    eventsourcing.EventStoreError(string.inspect(error))
  })
  |> result.map(fn(_) { Nil })
}

fn execute_in_transaction(connection_string: String) {
  fn(f) {
    let f = fn(db) {
      f(db) |> result.map_error(fn(error) { string.inspect(error) })
    }
    sqlight.with_connection(connection_string, f)
    |> result.map_error(fn(error) {
      eventsourcing.EventStoreError(string.inspect(error))
    })
  }
}
