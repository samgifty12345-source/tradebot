# Setup

## 1. Get a MetaApi token
- Sign up at https://metaapi.cloud (free tier = a few accounts)
- Dashboard → generate an API token
- This token authenticates YOUR app to MetaApi — it's not the same as your broker/prop firm login

## 2. Push to GitHub
```
cd tradeweb
git init
git add .
git commit -m "trading terminal"
git remote add origin https://github.com/yourname/tradeweb.git
git push -u origin main
```

## 3. Deploy on Render
- New → Web Service → connect the repo
- Build command: `pip install -r requirements.txt`
- Start command: `uvicorn main:app --host 0.0.0.0 --port $PORT`
- Environment → add `METAAPI_TOKEN` = the token from step 1
- Deploy

## 4. Use it
- Open your Render URL
- Enter the prop firm/demo/broker login, password, server name, platform (mt4/mt5)
- First connect takes 30-60s (MetaApi is spinning up a cloud terminal instance)
- Once connected: balance, positions, buy/sell all work from the browser

## Notes
- `server` is the exact MT4/MT5 server name (e.g. `FundingPips-Server` or `Exness-MT5Real`) — same as what you'd type into a real MT4/5 terminal login screen.
- This MVP stores active connections in memory — fine for you alone. If multiple people log in or Render restarts, connections reset (just reconnect).
- Free Render tier spins down when idle — first request after inactivity is slow. Get the paid tier ($7/mo) if this becomes your real trading interface.
