# GEX Platform Operations Guide  
  
## Service Management  
  
### Check Status  
```bash  
sudo systemctl status gex-ingestion  
sudo systemctl status gex-scheduler  
```  
  
### View Logs  
```bash  
# Real-time logs  
sudo journalctl -u gex-ingestion -f  
sudo journalctl -u gex-scheduler -f  
  
# Last 100 lines  
sudo journalctl -u gex-ingestion -n 100 --no-pager  
```  
  
### Restart Services  
```bash  
sudo systemctl restart gex-ingestion  
sudo systemctl restart gex-scheduler  
```  
  
### Stop Services  
```bash  
sudo systemctl stop gex-ingestion  
sudo systemctl stop gex-scheduler  
```  
  
## Database Queries  
  
### Check Recent Data  
```bash  
psql -U gex_user -d gex_db -h localhost << EOF  
SELECT   
    COUNT(*) as records,  
    MAX(timestamp) as latest,  
    COUNT(DISTINCT strike) as strikes  
FROM options_quotes  
WHERE timestamp > NOW() - INTERVAL '5 minutes';  
EOF  
```  
  
### View Latest GEX  
```bash  
psql -U gex_user -d gex_db -h localhost << 'EOF'  
SELECT   
    timestamp AT TIME ZONE 'America/New_York' as et_time,  
    underlying_price as spot,  
    total_gamma_exposure/1e6 as total_gex_M,  
    net_gex/1e6 as net_gex_M,  
    max_gamma_strike,  
    gamma_flip_point,  
    put_call_ratio  
FROM gex_metrics  
ORDER BY timestamp DESC  
LIMIT 10;  
EOF  
```  
  
### Database Size  
```bash  
psql -U gex_user -d gex_db -h localhost << EOF  
SELECT   
    schemaname,  
    tablename,  
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size  
FROM pg_tables  
WHERE schemaname = 'public'  
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;  
EOF  
```  
  
## Deployment  
  
### Update Code  
```bash  
cd /home/ubuntu/gex-options-platform  
git pull origin main  
source venv/bin/activate  
pip install -r requirements.txt  
sudo systemctl restart gex-ingestion  
sudo systemctl restart gex-scheduler  
```  
  
### Manual Backup  
```bash  
pg_dump -U gex_user -h localhost gex_db | gzip > ~/backup_$(date +%Y%m%d).sql.gz  
```  
  
### Restore Backup  
```bash  
gunzip -c backup_20241222.sql.gz | psql -U gex_user -h localhost gex_db  
```  
  
## Troubleshooting  
  
### No Data Coming In  
1. Check if market is open: `python scripts/debug_market_hours.py`  
2. Check TradeStation auth: `python src/ingestion/tradestation_auth.py`  
3. Check logs: `sudo journalctl -u gex-ingestion -n 100`  
  
### High Memory Usage  
```bash  
# Check memory  
free -h  
  
# Check PostgreSQL memory  
ps aux | grep postgres  
  
# Restart if needed  
sudo systemctl restart postgresql  
```  
  
### Disk Space  
```bash  
# Check disk usage  
df -h  
  
# Clean old logs  
sudo journalctl --vacuum-time=7d  
  
# Check database size  
du -sh /var/lib/postgresql/14/main/  
```  
  
## Monitoring Commands  
  
### Quick Status  
```bash  
systemctl is-active gex-ingestion gex-scheduler postgresql  
```  
  
### Full Monitor (runs continuously)  
```bash  
~/monitor.sh  
```  
  
# Watch data flowing in real-time  
watch -n 5 'psql -U gex_user -d gex_db -h localhost -c "SELECT COUNT(*) as records_last_min, MAX(timestamp) AT TIME ZONE '\''America/New_York'\'' as latest FROM options_quotes WHERE timestamp > NOW() - INTERVAL '\''1 minute'\'';"'  
  
### Network Check  
```bash  
# Check if TradeStation API is reachable  
curl -I https://api.tradestation.com  
```  
  
## URLs and Ports  
  
- EC2 Instance: `YOUR_EC2_IP`  
- SSH: `ssh -i your-key.pem ubuntu@YOUR_EC2_IP`  
- PostgreSQL: `localhost:5432`  
- Project Directory: `/home/ubuntu/gex-options-platform`  
  
## Restore database from backup  
```bash  
# List available backups  
sudo /usr/local/bin/restore-gex-db.sh  
# Restore from the latest backup  
sudo /usr/local/bin/restore-gex-db.sh /home/ubuntu/backups/gex_db_20241222_143052.dump  
```  
  
## Emergency Contacts  
  
- AWS Console: https://console.aws.amazon.com/  
- TradeStation API: https://api.tradestation.com/docs/  
- Project Repo: https://github.com/zero-gex-options/gex-options-platform  
