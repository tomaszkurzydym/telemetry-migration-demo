# Golden fixtures (SYNTHETIC DEMO)

`golden.json` holds hand-written input sequences and **independently specified** expected results, derived
by hand from `docs/contract.md` — not captured from either implementation. Both the Rust processor
(`rust-processor/tests/golden.rs`) and the legacy job (`legacy/src/test/scala/demo/legacy/GoldenFixtureSpec.scala`)
are tested against them.

Rules:
- Each case is processed by a fresh processor, records in order (state carries within a case only).
- `expect` per record: `{"emit": {...}}`, `{"quarantine": "<ERROR_CODE>"}` or `{"duplicate": true}`.
  Numbers in `emit` are compared by decimal value. `emit` lists every output key except `location_enc`.
- **No plaintext coordinates are committed.** Inputs reference a location by token (`"location": "@LOC-A"`);
  the runners substitute synthetic coordinates generated at test time. `"location_enc": "@LOC-A"` means
  "present, and decrypts to the coordinates substituted for `@LOC-A`". `@LOC-INVALID` is substituted
  with an out-of-range coordinate pair.
- `legacy_expect_override` lists the fields where the legacy job's characterized behaviour intentionally
  differs (DEFECT-1 only).
- `input_raw` is used verbatim (malformed JSON cases).
