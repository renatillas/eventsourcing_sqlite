<h1 align="center">Eventsourcing SQLite</h1>

<div align="center">
  ✨ <strong>Event Sourcing Library for Gleam with SQLite</strong> ✨
</div>

<div align="center">
  A Gleam library for building event-sourced systems using SQLite as the event store.
</div>

<br />

<div align="center">
  <a href="https://hex.pm/packages/eventsourcing_sqlite">
    <img src="https://img.shields.io/hexpm/v/eventsourcing_sqlite" alt="Available on Hex" />
  </a>
  <a href="https://hexdocs.pm/eventsourcing_sqlite">
    <img src="https://img.shields.io/badge/hex-docs-ffaff3" alt="Documentation" />
  </a>
</div>

---

## Table of contents

- [Introduction](#introduction)
- [Features](#features)
- [Concurrency Safety](#concurrency-safety)
- [Philosophy](#philosophy)
- [Installation](#installation)
- [Support](#support)
- [Contributing](#contributing)
- [License](#license)

## Introduction

`eventsourcing_sqlite` is a Gleam library designed to help developers build event-sourced systems using SQLite. Event sourcing is a pattern where changes to the application's state are stored as an immutable sequence of events. This approach provides several benefits, including complete audit trails, simplified debugging, and the ability to reconstruct past states.

## Features

- **Event Sourcing**: Build systems based on the event sourcing pattern.
- **SQLite Event Store**: Robust event store implementation using SQLite.
- **Command Handling**: Handle commands and produce events with robust error handling.
- **Event Application**: Apply events to update aggregates.
- **Snapshotting**: Optimize aggregate rebuilding with configurable snapshots.
- **Type-safe Error Handling**: Comprehensive error types and Result-based API.

## Concurrency Safety

This driver is designed to be used in a single-threaded environment. It is not safe to use this driver in a multi-threaded environment.

## Philosophy

eventsourcing_sqlite is designed to make building event-sourced systems easy and intuitive.
It encourages a clear separation between command handling and event application,
making your code more maintainable and testable.

## Installation

Add eventsourcing_sqlite to your Gleam projects from the command line:

``` sh
gleam add eventsourcing_sqlite
```

## Support

eventsourcing_sqlite is built by Renatillas.
Contributions are very welcome!
If you've spotted a bug, or would like to suggest a feature,
please open an issue or a pull request.

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository.
2. Create a new branch (git checkout -b my-feature-branch).
3. Make your changes and commit them (git commit -m 'Add new feature').
4. Push to the branch (git push origin my-feature-branch).
5. Open a pull request.

Please ensure your code adheres to the project's coding standards and includes appropriate tests.

## License

This project is licensed under the MIT License. See the LICENSE file for details.
