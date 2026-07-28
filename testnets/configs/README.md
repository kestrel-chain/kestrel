# Kestrel testnet configurations

Only public validator profiles and canonical genesis documents belong here. Never commit `validator.key`, infrastructure credentials, chaos-controller credentials, or unredacted operator inventories.

Stage-specific configurations are versioned only after all operators compare the canonical genesis hash and the relevant promotion gate in `docs/phase-6-status.md` is actually satisfied.

`stage2-campaign.example.json` is the schema example for the read-only campaign
monitor and post-run evidence compiler. Copy it to an operator-controlled
location, replace the placeholder genesis/RPC/log values, and keep any
unredacted infrastructure inventory outside this repository. See
`docs/testnet-operations.md` for the full workflow.
