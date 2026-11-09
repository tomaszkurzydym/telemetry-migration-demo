# SYNTHETIC DEMONSTRATION — NOT CUSTOMER CODE

Everything in this repository is synthetic and was written for this demonstration only:

- **Code**: the "legacy" Scala/Spark job, the Rust processor, and all tooling were written from scratch for this demo.
  None of it came from, or is derived from, any customer's codebase.
- **Data**: all telemetry events come from the seeded generator in `generator/`. No real vehicle, fleet, or
  battery data appears anywhere.
- **VINs**: every identifier has the form `SYN-VIN-NNNN`. This is deliberately not a valid ISO 3779 VIN and
  identifies no real vehicle.
- **Locations**: every coordinate is generated at random inside a box of open ocean in the mid-Atlantic
  (lat 10–11°N, lon 29–30°W), or computed from a formula in test fixtures. No coordinate corresponds to a real
  vehicle, person, or address.
- **Keys**: no encryption key is committed. Tests generate random keys at runtime, and local runs read
  `LOCATION_ENC_KEY` from a git-ignored `.env`.

Nothing in this repository claims production readiness, compliance certification, KMS-grade key management,
end-to-end exactly-once delivery beyond the tested Kafka-to-Kafka boundary, global ordering, or production-scale
performance. See `docs/SUMMARY.md` and `docs/EVIDENCE.md` for exactly what was tested and how.
