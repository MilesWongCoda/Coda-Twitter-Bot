#!/bin/bash
# Deploy bot updates to VPS
# Usage: ./deploy.sh
#
# Configure VPS connection in environment or edit below:
VPS="${DEPLOY_VPS:-user@your-server-ip}"
REMOTE="${DEPLOY_PATH:-/opt/chainmacrolab}"

set -e
echo "=== Deploying to $VPS:$REMOTE ==="
cd bot

scp ai/generate.py "$VPS:$REMOTE/ai/"
scp config.py "$VPS:$REMOTE/"
scp main.py "$VPS:$REMOTE/"
scp db/store.py "$VPS:$REMOTE/db/"
scp data/gifs.py "$VPS:$REMOTE/data/"
scp data/polymarket.py "$VPS:$REMOTE/data/"
scp data/fng.py "$VPS:$REMOTE/data/"
scp jobs/self_reply.py "$VPS:$REMOTE/jobs/"
scp jobs/trend_alert.py "$VPS:$REMOTE/jobs/"
scp jobs/engagement.py "$VPS:$REMOTE/jobs/"
scp jobs/hot_take.py "$VPS:$REMOTE/jobs/"
scp jobs/morning_brief.py "$VPS:$REMOTE/jobs/"
scp jobs/fng.py "$VPS:$REMOTE/jobs/"
scp twitter/engager.py "$VPS:$REMOTE/twitter/"
scp twitter/poster.py "$VPS:$REMOTE/twitter/"
scp twitter/watchlist.py "$VPS:$REMOTE/twitter/"
scp notifications/telegram.py "$VPS:$REMOTE/notifications/"

cd ..
echo ""
echo "=== Done ==="
echo ""
echo "Now SSH and restart:"
echo "  ssh $VPS"
echo "  systemctl restart chainmacrolab"
echo "  journalctl -u chainmacrolab -n 20 --no-pager"
echo ""
