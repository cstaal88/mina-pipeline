# v2 Pipeline — Setup TODO

## 1. Create NeonDB database
- Go to https://neon.tech and create a free-tier project
- Copy the connection string (looks like `postgresql://user:pass@ep-xxx.region.aws.neon.tech/dbname?sslmode=require`)

## 2. Test locally
```bash
export DATABASE_URL="postgresql://..."
cd v2draft
pip install -r requirements.txt
python pipeline.py          # creates table + fetches articles
python pipeline.py --dry-run  # test without writing to DB
```

## 3. Fill in topics
- Edit `config.py` — replace the example topics with your own keywords
- Test with: `python query.py --topic your-topic --days 5`

## 4. Adjust outlets (optional)
- The default list has 11 US national outlets
- Add/remove outlets in the `RSS_OUTLETS` dict in `config.py`

## 5. Set up GitHub Actions
- If creating a new repo, push this code there
- Add `DATABASE_URL` as a repository secret (Settings → Secrets → Actions)
- Run the workflow manually first (Actions → RSS Pipeline v2 → Run workflow)
- Uncomment the `schedule` trigger in the workflow file once verified

## 6. Set up scheduled runs
**Option A — GitHub Actions schedule** (already in workflow, just uncomment):
```yaml
schedule:
  - cron: "0 */6 * * *"  # every 6 hours
```

**Option B — cron-job.org** (external trigger):
- Create a cron job that hits the GitHub Actions workflow dispatch API
