# Shopping Sessions: native Rust pipeline

One static Rust binary that reads the CSV inputs in `$DATA_DIR`, applies every rule in `rules.md`, and writes `user_daily/` and `campaign_daily/` as Parquet to `$OUTPUT_DIR`. No engine or JVM starts, so almost all the run time goes to reading and parsing.

## How it works

The phases, each spread over `$NCPU` threads:

1. **Links.** Parse `links/`, then a union-find joins the IDs. Each ID maps to its person's name, the byte-wise smallest ID in its group.
2. **Parse.** All event and click files are memory-mapped and cut into line-aligned 8 MB chunks. A hand-written splitter applies the CSV dialect exactly: whole-field quotes only, exact field counts, `\r` as data. Each event copy is validated as it is parsed, and the late check is folded in. Events are bucketed by `event_id`. Clicks are bucketed by person.
3. **Dedup.** For each `event_id` bucket, keep the copy with the smallest `ingest_ts`. The survivors that are valid are re-bucketed by person.
4. **Persons.** Each person bucket is sorted by (person, ts) and split into sessions on gaps over 30 min. Each session is credited to the latest click within 7 days, with ties going to the smallest `click_id`. For assists, each campaign's clicks are merged into intervals `[t, t + 7d]`, and the sessions starting inside them are counted per day.
5. **Write.** Uncompressed Parquet: one `user_daily` file per thread and one `campaign_daily` file.

Strings are keyed as `u128`. Up to 15 bytes are packed big-endian with the length in the low byte, which is exact and sorts byte-wise. Longer strings get a 120-bit hash and are kept in a side table.

Click duplicates are not removed: copies are identical, so they cannot change a credit or an assist.

## Test

```sh
make test             # unit tests: CSV dialect, timestamps, prices
make fixtures-local   # 36 fixtures against a local build
make build fixtures   # 36 fixtures in the container, scorer flags
make dev run-dev check-dev ANSWER=dev-answer
```
