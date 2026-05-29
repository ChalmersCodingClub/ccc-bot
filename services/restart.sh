#!/bin/bash
# Restart all three ccc-bot systemd services.
sudo systemctl restart cccbot cccbot-scraper cccbot-problem-scraper
systemctl status cccbot cccbot-scraper cccbot-problem-scraper --no-pager
