# streak-tracker

A daily-streak tracker built on [Temporal](https://temporal.io/). Each user's
streak lives in a long-running Temporal workflow that holds state, advances at
"midnight," and survives restarts via Continue-As-New. A small HTTP server and
static web UI sit in front of it.

## How it works

- **Workflow** (`workflow/`) — one `StreakWorkflow` instance per user, keyed by
  workflow ID `streak-<userId>`. It keeps the streak state, runs a timer until
  the next midnight (or virtual day), and reacts to signals. State is exposed
  through a query and the workflow periodically Continues-As-New to keep history
  bounded.
- **Activity** (`activity/`) — `LogStreakMilestone` logs streak milestones.
- **API** (`api/`) — an HTTP server that translates requests into Temporal
  signals/queries and serves the UI.
- **UI** (`static/index.html`) — a single-page front end for recording activity
  and viewing the current streak.

### Timezone-aware streaks

Recording carries a timezone. When a user travels east (losing hours), the
workflow grants grace so a "lost" day doesn't break a streak: it winds the clock
back by the hours lost relative to the anchor timezone before comparing dates.

### Demo mode

Set `dayDuration` (seconds per "day") on the first record call to compress days
into seconds for demos. `0` means real calendar days.

## Prerequisites

- Go 1.25+
- A running Temporal server at `localhost:7233` (e.g. via the
  [Temporal CLI](https://docs.temporal.io/cli): `temporal server start-dev`)

## Running

```bash
go run .
```

This starts the Temporal worker on the `streak-tracker` task queue and an HTTP
server on http://localhost:8080. Open that URL in a browser, or use the API
directly.

## API

| Method | Path            | Description                                   |
| ------ | --------------- | --------------------------------------------- |
| POST   | `/api/record`   | Record activity (starts the workflow if new). |
| GET    | `/api/streak`   | Get current streak state for a user.          |
| POST   | `/api/timezone` | Simulate a timezone change for a user.        |

### Examples

```bash
# Record activity (creates the streak on first call)
curl -X POST localhost:8080/api/record \
  -d '{"userId":"alice","timezone":"America/Chicago"}'

# Get the current streak
curl 'localhost:8080/api/streak?userId=alice'

# Simulate traveling to a new timezone
curl -X POST localhost:8080/api/timezone \
  -d '{"userId":"alice","newTimezone":"Europe/London"}'
```

`/api/record` defaults `timezone` to `America/Chicago` when omitted. Pass
`"dayDuration": <seconds>` on the first record call to enable demo mode.
