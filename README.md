# agent-harness

Durable workflow execution and recovery.

## Implementation status

An experimental internal runtime foundation is implemented on the pinned Node 22.23.3 toolchain. It provides strict local runtime configuration, safe typed errors, and file-backed SQLite workflow durability primitives with checkpoints, worker fencing, recovery receipts, immutable bytes and a transactional event outbox. The package does not implement canonical public ports, provider dispatch, financial authority, or a production workflow. AH-01 and AH-03 acceptance remains pending for canonical fixtures, consumer agreement, cross-repository compatibility, server runtime confirmation, and integrated evidence. See [the runtime foundation decision](docs/decisions/runtime-foundation.md).

[Architecture reference](https://github.com/Jevelopers-neatHack-2026/getticketstub.com/blob/main/docs/architecture-kit/v1.0/README.md) · [Implementation plan](https://github.com/Jevelopers-neatHack-2026/getticketstub.com/blob/main/docs/planning/implementation-plan.md)

## Architecture and delivery

- [Repository responsibilities and interfaces](docs/architecture/OWNERSHIP.md)
- [Relevant acceptance criteria](docs/architecture/ACCEPTANCE.md)
- [Architecture and diagrams](https://github.com/Jevelopers-neatHack-2026/getticketstub.com/blob/main/docs/architecture-kit/v1.0/README.md)
- [Central implementation plan](https://github.com/Jevelopers-neatHack-2026/getticketstub.com/blob/main/docs/planning/implementation-plan.md)
- [Contributor guidance](https://github.com/Jevelopers-neatHack-2026/.github/blob/main/CONTRIBUTING.md)

Architecture integration documents the intended boundaries. It does not establish completed product functionality, passing implementation tests, connected sponsor tools, or live financial activity.
