# Shopping Sessions: native Rust pipeline

One static Rust binary that reads the CSV inputs in `$DATA_DIR`, applies every rule in `rules.md`, and writes `user_daily/` and `campaign_daily/` as Parquet to `$OUTPUT_DIR`. No engine or JVM starts, so almost all the run time goes to reading and parsing.

## How it works

The phases, each spread over `$NCPU` threads:

1. **Parse.** All files are memory-mapped and cut into line-aligned 4 MB chunks (1 MB for clicks). An AVX2 scan finds `,`, `\n` and `"` 64 bytes at a time. The quotes of a whole 64-byte block are checked with bit operations: with the quote parity counted from each line's start (a prefix XOR, corrected per line by one subtraction), a line is well-formed exactly when every opening quote starts a field, every closing quote is followed by a separator, and no separator is inside quotes. Separator positions go to a small ring, so a line costs a popcount and one test. Timestamps are parsed with SWAR; a date's `YYYY-MM-` is matched against the four months around the window, and F3 digits are converted with SSE. Each event copy is validated as it is parsed, with the late check folded in, and goes to a bucket by `event_id`. Meanwhile one thread reads `links/`, and a union-find maps each ID to its person's name, the byte-wise smallest ID in the group. Clicks are parsed last, using that map, and go to a bucket by person.
2. **Dedup.** For each `event_id` bucket, an open-addressing table keeps the copy with the smallest `ingest_ts`. Valid survivors are re-bucketed by person.
3. **Persons.** In each person bucket, a small table gives each person a dense index, and a counting sort groups their events and clicks. Then each person's events are sorted by ts and split into sessions on gaps over 30 min. Each session is credited to the latest click within 7 days, with ties going to the smallest `click_id`. For assists, each campaign's clicks are merged into intervals `[t, t + 7d]`, and the sessions starting inside them are counted per day.
4. **Write.** A small hand-written Parquet writer (`src/output.rs`): uncompressed PLAIN pages, definition levels as one bit-packed run, and a Thrift compact footer. There is one `user_daily` file per thread and one `campaign_daily` file, each built in memory and written in one call.

IDs of 1 to 9 ASCII bytes other than NUL (all the generator writes) pack exactly into a `u64`, 7 bits a byte via PEXT, with byte-wise order kept. Any other ID gets a hashed key, and its bytes are kept aside. Times are `u32` milliseconds from `window_start - 7 days`. With these, an event copy is 32 bytes and a surviving event 16. Bucket buffers are per-thread arenas backed by huge pages. Person lookups go through a blocked Bloom filter of the renamed IDs, then a partitioned hash map that is built a partition at a time, in cache.

Click duplicates are not removed: copies are identical, so they cannot change a credit or an assist.

## Test

```sh
make test             # unit tests: CSV dialect, timestamps, prices
make fixtures-local   # 36 fixtures against a local build
make build fixtures   # 36 fixtures in the container, scorer flags
make dev run-dev check-dev ANSWER=dev-answer
```
