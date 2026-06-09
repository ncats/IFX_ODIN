# PubMed Mirror

Standalone PubMed article mirror for Pharos.

This tool:

- downloads PubMed baseline and update archives from NCBI
- verifies archive checksums
- parses article metadata and abstracts
- upserts articles into a MySQL `pubmed` table
- deletes withdrawn PMIDs
- tracks processed archive files in MySQL state

## Commands

Initialize schema only:

```bash
python scripts/pubmed_mirror/main.py init
```

Rebuild from baseline files:

```bash
python scripts/pubmed_mirror/main.py rebuild
```

Test a one-file baseline rebuild:

```bash
python scripts/pubmed_mirror/main.py rebuild --limit-archives 1
```

Apply incremental updates:

```bash
python scripts/pubmed_mirror/main.py update
```

Show mirror status:

```bash
python scripts/pubmed_mirror/main.py status
```

## Configuration

Default MySQL credentials path:

- `src/use_cases/secrets/pharos_write_credentials.yaml`

Credential YAML shape:

```yaml
url: localhost
user: root
password: rootpassword
port: 3306
```

Default schema:

- `ifx_pubmed`

Default local archive directory:

- `input_files/pubmed`

## Recommended First Run

```bash
python scripts/pubmed_mirror/main.py init
python scripts/pubmed_mirror/main.py rebuild
python scripts/pubmed_mirror/main.py update
python scripts/pubmed_mirror/main.py status
```

## Weekly Cron

Use the wrapper script:

```bash
scripts/pubmed_mirror/run_weekly_update.sh
```

The wrapper uses `bash` so it can run on Linux hosts that do not have `zsh`
installed. If cron logs `run_weekly_update.sh: not found` even though the file
exists, check that the script interpreter exists on the host.

Example cron setup:

```
chmod +x /home/kelleherkj/IFX_ODIN/scripts/pubmed_mirror/run_weekly_update.sh
```

```
crontab -e
```

```cron
15 3 * * 0 /home/kelleherkj/IFX_ODIN/scripts/pubmed_mirror/run_weekly_update.sh >> /home/kelleherkj/IFX_ODIN/scripts/pubmed_mirror/cron.log 2>&1
```

```
crontab -l
```

Use `crontab -e` for the `kelleherkj` user job. `sudo crontab -e` edits
root's crontab, which will also create `cron.log` as root unless ownership is
changed separately.
