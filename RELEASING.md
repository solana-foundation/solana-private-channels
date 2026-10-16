# Releasing

How a version of Solana Private Channels is tagged, built, published and deployed.
Upgrading a running stack is in [UPGRADING.md](UPGRADING.md).

## Who may release

Only the release owners may push a `v*` tag. The list lives in the repository's tag
ruleset (no creation, update or deletion by anyone else). Each release names one owner
and one reviewer for the go/no-go.

## Versioning

Semver, starting at `v0.1.0`. The workspace version in `Cargo.toml` is bumped to match in
the release PR. Any client-visible RPC change (a new rejection, a new error code, a
changed retry rule) is at least a minor bump.

## Where tags come from

- Normal releases are tagged on `main`.
- Hotfixes: branch `release/vX.Y` from the tag, land the fix there by PR, tag
  `vX.Y.Z+1`, then cherry-pick the fix to `main` by PR. The `Main & Release` ruleset
  already protects `release/*`.
- Tags are signed: `git tag -s`, so `git verify-tag` works.

## Release candidate flow

1. Tag `vX.Y.Z-rc.1` on the `main` commit to release.
2. `publish-image.yml` builds, scans, pushes and signs
   `ghcr.io/solana-foundation/private-channel-app:vX.Y.Z-rc.1`.
3. Deploy that tag to devnet and run the smoke scenarios. Record the results.
4. Any fix lands on `main` and gets `rc.2`, and so on.
5. Cut `vX.Y.Z` from the same commit as the last green rc.

The tag build rebuilds the image from floating `rust:bookworm` and `debian:bookworm-slim`
bases, so the final tag's digest can differ from the rc that was tested. Compare the two
digests and rerun the smoke subset if they differ.

## Flow

```
git tag -s vX.Y.Z <sha> -m "vX.Y.Z" && git push origin vX.Y.Z
  -> publish-image.yml: build, Trivy (CRITICAL, fixed only), push, cosign sign,
     SBOM and provenance attestations
  -> vars/<env>.yml: image_tag: vX.Y.Z, verify_image_signature: true
  -> ansible-playbook deploy.yml -l <env>
     (preflight runs cosign verify and pins the compose image to the verified digest)
  -> sanity phase runs at the end of the deploy
```

Check before announcing:

```
cosign verify ghcr.io/solana-foundation/private-channel-app:vX.Y.Z \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp '^https://github\.com/solana-foundation/solana-private-channels/\.github/workflows/publish-image\.yml@refs/tags/vX\.Y\.Z$'
```

## Go/no-go

Before the tag:

- `main` clean, CI green on the release commit, CHANGELOG.md updated
- last rc smoke test recorded with no open failures
- rollback criteria written down: which observation inside the watch window triggers a
  redeploy of the previous tag

After the deploy: watch dashboards and alerts for the agreed window, then announce with
the CHANGELOG link and the commit the audit reports cover.

## Rollback

Redeploy the previous tag: `ansible-playbook deploy.yml -l <env> -e image_tag=<previous>`.
This is only safe when the release added no schema change. See
[UPGRADING.md](UPGRADING.md#rollback).
