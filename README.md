# SLURM analysis

Getting the data:

```bash
STARTDATE=$(date --date="90 days ago" +%F)
echo $STARTDATE
sacct -S "${STARTDATE}" --partition pli-c --allusers --json > sacct_pli_core.json
echo "done"
sacct -S "${STARTDATE}" --partition pli-lc --allusers --json > sacct_pli_large_campus.json
echo "done"
sacct -S "${STARTDATE}" --partition pli --allusers --json > sacct_pli_campus.json
echo "done"
tar -vzcf pli_stats.tar.gz *.json
```
