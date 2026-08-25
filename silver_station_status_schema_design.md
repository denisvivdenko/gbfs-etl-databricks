# Silver Schema Design: `station_status`

Source: `bronze_station_status` (see `fixtures/bronze_station_status_schema.json`,
sampled in `fixtures/bronze_station_status_sample.json`).

## Findings from the bronze sample

- Sample = 5 polls x 2630 stations (13,150 station-status rows).
- Station roster is stable: all 5 polls contain the exact same 2630 `station_id`s,
  no duplicates within a poll.
- `last_reported` is per-station (28 distinct values per poll) and, per the GBFS
  spec, only advances when *something* in that station's status changed. It is
  independent of `last_updated`, which is the feed-level poll timestamp.
- `vehicle_types_available` is a sparse array (0-7 elements per station, avg 1.25)
  drawn from only 7 distinct `vehicle_type_id`s in the sample.
- `is_installed` / `is_renting` / `is_returning` were constant
  (`true, true, true`) for every row in the sample - these are rare-changing
  operational flags (maintenance events), not high-frequency data.
- `num_vehicles_available` and the vehicle-type mix are the fields that actually
  change on (nearly) every poll - these are what drives `last_reported` forward
  most of the time.
- `ttl` and `version` are feed-level, not station-level, and were constant across
  the whole sample (`ttl=0`, `version="3.0"`). They are cache-control / API-version
  metadata, not analytical facts about a station.
- No station attributes (name, lat/lon, capacity) exist in this feed - those live
  in the separate `station_information` GBFS feed.

## Design goals driving the split

1. Explode the nested `vehicle_types_available` array instead of storing it as-is.
2. Avoid keying everything on `last_reported`: that timestamp advances on *any*
   change, which would force rare-changing fields (operational flags) and
   high-frequency fields (vehicle counts) into the same row cadence, duplicating
   the rare-changing fields on every high-frequency update.
3. Use natural keys (`station_id`, `vehicle_type_id`) directly - no surrogate key
   generation. These IDs are already stable/unique, and natural keys join cleanly
   into other GBFS feeds (`station_information`, `vehicle_types.json`) later
   without any rework. `dim_station` / `dim_vehicle_type` tables are deferred until
   those feeds are ingested and there are actual descriptive attributes to store.
4. Drop `ttl` / `version` entirely from silver. They're feed/session-level
   cache-control metadata, not station facts, and don't carry analytical value
   here.

## Schema

Each table is an append-only change log: a new row is inserted **only when its
own tracked value(s) differ from the station's most-recently-known state in that
table** - not simply when a new poll arrives or when `last_reported` changes for
unrelated reasons. This decouples tables by change frequency so rare-changing
attributes aren't duplicated on every high-frequency update.

### `silver_station_operational_status`

Rare-changing (installation / maintenance events).

| column | type | notes |
|---|---|---|
| `station_id` | string | |
| `effective_from` | timestamp | `last_reported` of the poll where this state was first observed |
| `is_installed` | boolean | |
| `is_renting` | boolean | |
| `is_returning` | boolean | |

PK: `(station_id, effective_from)`. Insert only when
`(is_installed, is_renting, is_returning)` differs from the station's last row.

### `silver_station_vehicle_availability`

High-frequency (changes on most polls).

| column | type | notes |
|---|---|---|
| `station_id` | string | |
| `effective_from` | timestamp | `last_reported` of the poll where this value was first observed |
| `num_vehicles_available` | int | |

PK: `(station_id, effective_from)`. Insert only when `num_vehicles_available`
differs from the station's last row.

### `silver_station_vehicle_type_availability`

Same cadence as vehicle availability, own change detection - tracked
independently **per `(station_id, vehicle_type_id)` pair**, not as a whole-mix
batch.

| column | type | notes |
|---|---|---|
| `station_id` | string | |
| `effective_from` | timestamp | `last_reported` of the poll where this pair's count was first observed at this value |
| `vehicle_type_id` | string | |
| `count` | int | |

PK: `(station_id, effective_from, vehicle_type_id)`. Insert a new row for a
`(station_id, vehicle_type_id)` pair only when its `count` differs from that
pair's last known count.

If a `vehicle_type_id` that a station has previously reported drops out of
`vehicle_types_available` on a later poll, treat it as `count = 0` for that
pair and insert a row (rather than leaving it silently unrepresented). A
`vehicle_type_id` a station has *never* reported is never zero-filled - it
only enters this table the first time it appears for that station, with its
first observed (non-zero) count.

This resolves the earlier "invisible transition" gap: an empty
`vehicle_types_available` array is no longer a silent no-op, since every
previously-seen pair for that station gets an explicit `count = 0` row.

## Deferred / explicitly out of scope for now

- `dim_station`, `dim_vehicle_type`: no descriptive attributes exist in this feed
  to justify a dimension table yet. Revisit once `station_information.json` /
  `vehicle_types.json` are ingested - natural keys join straight in without
  rework.
- `ttl`, `version`: dropped from silver. If feed-health monitoring is ever
  needed, a separate `silver_feed_poll_log` (`last_updated`, `ttl`, `version`,
  `station_count`, one row per poll) can be added later without affecting this
  design.

## Known tradeoff

Reconstructing "current full status for station X" requires an as-of join
across three independently-paced tables (latest row <= t in each), instead of a
single row lookup. This is the accepted cost of splitting by change frequency to
avoid duplicating rare-changing fields across a high-frequency table.

## Open question for next iteration

Relying on `last_reported` as the event-time anchor assumes the vendor strictly
follows the GBFS spec (bumping it on every real change). If this vendor's feed
turns out to be lax about that, some changes could be silently missed. Worth a
spot-check against a longer history before treating this as settled.
