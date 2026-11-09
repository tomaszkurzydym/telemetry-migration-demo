# Kubernetes manifests — PRODUCTION FOLLOW-UP (not deployed)

These manifests are a **sketch for a later production rollout**. They were only validated with
`kubectl apply --dry-run=client` (see `docs/EVIDENCE.md`); they have **never run on a cluster**.
Image names, Kafka bootstrap address, and resource sizes are placeholders.

Before any real deployment (see `docs/SUMMARY.md` → Production follow-ups):
- Replace the `location-enc-key` Secret with KMS envelope encryption (data keys wrapped by a KMS key,
  delivered via an external-secrets/CSI driver), with key rotation and `key_id`-based multi-key decrypt.
- Per-consumer keys / scoped decrypt rights; topic ACLs so only the processor can write normalized topics
  and only privileged consumers can read the decrypt path.
- TLS/SASL to Kafka, replication factor ≥ 3, `min.insync.replicas` ≥ 2.
