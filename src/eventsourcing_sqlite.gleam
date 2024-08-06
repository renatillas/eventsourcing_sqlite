import eventsourcing
import gleam/bool
import gleam/dynamic
import gleam/int
import gleam/io
import gleam/json
import gleam/list
import gleam/pair
import gleam/result
import sqlight

// CONSTANTS ----

const insert_event_query = "
  INSERT INTO event 
  (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata)
  VALUES 
  ($1, $2, $3, $4, $5, $6, $7)
  "

const select_events_query = "
  SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata
  FROM event
  WHERE aggregate_type = $1 AND aggregate_id = $2
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

// TYPES ----

pub opaque type SqliteStore(entity, command, event, error) {
  SqliteStore(
    db: sqlight.Connection,
    empty_aggregate: eventsourcing.Aggregate(entity, command, event, error),
    event_encoder: fn(event) -> String,
    event_decoder: fn(String) -> Result(event, List(dynamic.DecodeError)),
    event_type: String,
    event_version: String,
    aggregate_type: String,
  )
}

pub type Metadata =
  List(#(String, String))

// CONSTRUCTORS ----

pub fn new(
  sqlight_connection sqlight_connection: sqlight.Connection,
  empty_entity empty_entity: entity,
  handle_command_function handle: eventsourcing.Handle(
    entity,
    command,
    event,
    error,
  ),
  apply_function apply: eventsourcing.Apply(entity, event),
  event_encoder event_encoder: fn(event) -> String,
  event_decoder event_decoder: fn(String) ->
    Result(event, List(dynamic.DecodeError)),
  event_type event_type: String,
  event_version event_version: String,
  aggregate_type aggregate_type: String,
) -> eventsourcing.EventStore(
  SqliteStore(entity, command, event, error),
  entity,
  command,
  event,
  error,
) {
  let eventstore =
    SqliteStore(
      db: sqlight_connection,
      empty_aggregate: eventsourcing.Aggregate(empty_entity, handle, apply),
      event_encoder:,
      event_decoder:,
      event_type:,
      event_version:,
      aggregate_type:,
    )

  eventsourcing.EventStore(
    eventstore:,
    commit: commit,
    load_aggregate: load_aggregate,
  )
}

pub fn create_event_table(
  sqlite_store: SqliteStore(entity, command, event, error),
) {
  sqlight.query(
    create_event_table_query,
    on: sqlite_store.db,
    with: [],
    expecting: dynamic.dynamic,
  )
}

pub fn load_aggregate_entity(
  sqlite_store: SqliteStore(entity, command, event, error),
  aggregate_id: eventsourcing.AggregateId,
) -> Result(entity, Nil) {
  let assert Ok(commited_events) = load_events(sqlite_store, aggregate_id)
  use <- bool.guard(commited_events |> list.length == 0, Error(Nil))

  let #(aggregate, sequence) =
    list.fold(
      over: commited_events,
      from: #(sqlite_store.empty_aggregate, 0),
      with: fn(aggregate_and_sequence, event_envelop) {
        let #(aggregate, _) = aggregate_and_sequence
        #(
          eventsourcing.Aggregate(
            ..aggregate,
            entity: aggregate.apply(aggregate.entity, event_envelop.payload),
          ),
          event_envelop.sequence,
        )
      },
    )
  eventsourcing.AggregateContext(aggregate_id:, aggregate:, sequence:).aggregate.entity
  |> Ok
}

pub fn load_events(
  sqlite_store: SqliteStore(entity, command, event, error),
  aggregate_id: eventsourcing.AggregateId,
) {
  use resulted <- result.map(sqlight.query(
    select_events_query,
    on: sqlite_store.db,
    with: [
      sqlight.text(sqlite_store.aggregate_type),
      sqlight.text(aggregate_id),
    ],
    expecting: dynamic.decode7(
      eventsourcing.SerializedEventEnvelop,
      dynamic.element(1, dynamic.string),
      dynamic.element(2, dynamic.int),
      dynamic.element(5, fn(dyn) {
        let assert Ok(payload) =
          dynamic.string(dyn) |> result.map(sqlite_store.event_decoder)
        payload
      }),
      dynamic.element(6, metadata_decoder),
      dynamic.element(3, dynamic.string),
      dynamic.element(4, dynamic.string),
      dynamic.element(0, dynamic.string),
    ),
  ))
  resulted
}

fn load_aggregate(
  sqlite_store: SqliteStore(entity, command, event, error),
  aggregate_id: eventsourcing.AggregateId,
) -> eventsourcing.AggregateContext(entity, command, event, error) {
  let assert Ok(commited_events) = load_events(sqlite_store, aggregate_id)

  let #(aggregate, sequence) =
    list.fold(
      over: commited_events,
      from: #(sqlite_store.empty_aggregate, 0),
      with: fn(aggregate_and_sequence, event_envelop) {
        let #(aggregate, _) = aggregate_and_sequence
        #(
          eventsourcing.Aggregate(
            ..aggregate,
            entity: aggregate.apply(aggregate.entity, event_envelop.payload),
          ),
          event_envelop.sequence,
        )
      },
    )
  eventsourcing.AggregateContext(aggregate_id:, aggregate:, sequence:)
}

fn commit(
  sqlite_store: SqliteStore(entity, command, event, error),
  context: eventsourcing.AggregateContext(entity, command, event, error),
  events: List(event),
  metadata: List(#(String, String)),
) {
  let eventsourcing.AggregateContext(aggregate_id, _, sequence) = context
  let wrapped_events =
    wrap_events(sqlite_store, aggregate_id, events, sequence, metadata)
  persist_events(sqlite_store, wrapped_events)
  io.println(
    "storing: "
    <> wrapped_events |> list.length |> int.to_string
    <> " events for Aggregate ID '"
    <> aggregate_id
    <> "'",
  )
  wrapped_events
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
  wrapped_events: List(eventsourcing.EventEnvelop(event)),
) {
  wrapped_events
  |> list.map(fn(event) {
    let assert eventsourcing.SerializedEventEnvelop(
      aggregate_id,
      sequence,
      payload,
      metadata,
      event_type,
      event_version,
      aggregate_type,
    ) = event

    sqlight.query(
      insert_event_query,
      on: sqlite_store.db,
      with: [
        sqlight.text(aggregate_type),
        sqlight.text(aggregate_id),
        sqlight.int(sequence),
        sqlight.text(event_type),
        sqlight.text(event_version),
        sqlight.text(payload |> sqlite_store.event_encoder),
        sqlight.text(metadata |> metadata_encoder),
      ],
      expecting: dynamic.dynamic,
    )
  })
}

fn metadata_encoder(metadata: Metadata) -> String {
  json.array(metadata, fn(row) {
    json.preprocessed_array([json.string(row.0), json.string(row.1)])
  })
  |> json.to_string
}

fn metadata_decoder(
  dyn_metadata: dynamic.Dynamic,
) -> Result(Metadata, List(dynamic.DecodeError)) {
  use str_metadata <- result.try(dynamic.string(dyn_metadata))
  use list_metadata <- result.map(
    json.decode(
      str_metadata,
      dynamic.list(of: dynamic.list(of: dynamic.string)),
    )
    |> result.map_error(fn(_) { panic }),
  )
  list.map(list_metadata, fn(metadata) {
    case metadata {
      [key, value] -> #(key, value)
      _ -> panic
    }
  })
}
