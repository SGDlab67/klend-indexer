# Secrets handling — klend-indexer

**Decision (2026-07-19): macOS Keychain, injected per-command. No plaintext key on disk, ever.**

## Why this matters more here than on a normal project

The Alchemy key is not just a data-access credential — it is a **payment instrument**.
Billing is usage-based (~$75/TB) with **no monthly floor and no automatic ceiling**.
A leaked key does not merely expose data; it lets a stranger spend money in your name,
and the first signal is an invoice. This is the same failure shape as the empty-`owner`
filter hazard in `src/main.rs` (plan §7d) — silent, financial, discovered late.

Treat this key like a card number, not like a password.

## The rule

Plaintext keys never touch: the repo, `~/.bash_history`, a `.env` file, a chat window,
or a screen share. The key lives in the macOS Keychain and is read into a single
process's environment at launch, existing only for that process's lifetime.

## Store the key (once)

```bash
security add-generic-password -a "$USER" -s alchemy-grpc-token -w
```

`-w` with no value **prompts interactively** and confirms. The secret is never typed as a
shell argument, so it cannot land in history or in another user's `ps` output.

## Run the indexer

```bash
./run.sh
```

`run.sh` reads the key from the Keychain and scopes it to that one command:

```bash
GRPC_TOKEN="$(security find-generic-password -a "$USER" -s alchemy-grpc-token -w)" cargo run
```

Note the form `VAR=value command` rather than `export VAR=value`. The variable exists
only in the child process's environment, not in the shell session — so it does not leak
into every subsequent command, or into a shell you later hand to someone else.

`run.sh` contains a **reference**, not a value, so it is safe to commit. That is the whole
idea: config in the repo, secrets in the vault.

## Rotate

Rotate at any suspicion of exposure, and routinely (Alchemy recommends at minimum annually;
90 days is the stricter common practice):

1. Create a new key in the Alchemy dashboard.
2. `security delete-generic-password -a "$USER" -s alchemy-grpc-token`
3. Re-run the `add-generic-password` command above with the new key.
4. Delete the old key in the dashboard — rotation is not complete until the old one is dead.

## Guardrails to set in the Alchemy dashboard

Do these once; they convert an unbounded loss into a bounded one.

- **Max auto-scaling spend limit** (Billing settings) — a hard dollar/CU cap at which service
  stops. This is the actual ceiling; without it there is none.
- **Spend alerts** (Alerts tab) — threshold notifications, so anomalies surface in hours
  rather than at invoice time.
- **Security console** (app page → Security) — restrict key usage where possible.

Combined with the §8d daily usage check during week 1, that's detection *and* containment.

## What was deliberately rejected

- **`.env` file, even gitignored.** One `git add -f`, one editor plugin, one backup tool, one
  AI coding agent reading the directory, and it is out. Gitignore prevents commits; it does
  not prevent reads. The key never needs to be on disk in plaintext, so it should not be.
- **`export` in `.bashrc`.** Puts the key in the environment of *every* process you launch
  for the rest of the machine's life, including anything that dumps its environment on crash.
- **The leading-space history trick.** Only works when `HISTCONTROL` includes `ignorespace`.
  It was **unset on this machine**, so that mitigation was inert — a good example of a
  security measure that appears to work while doing nothing.

## Upgrade path (not now)

If this ever becomes multi-machine or multi-person, move to **1Password CLI** (`op run --env-file`),
which keeps secret *references* in a committed file, injects real values only for the process
duration, and redacts secrets that get printed to stdout. Keychain is the right call while this
is one developer on one Mac; `op` is the right call the moment it isn't.
