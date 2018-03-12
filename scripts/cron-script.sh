line="10 10 * * mon ~/prototype-scripts/ingestion-script.sh"
(crontab -l; echo "$line" ) | crontab -