# OI Pulse clone (Kotak Neo)
./run.sh  ->  open http://localhost:8000  ->  paste your Neo Trade API token in the popup.
Optional: set KOTAK_CONSUMER_KEY env var to skip the popup. "Remember" stores the key in ~/.oipulse/config.json (chmod 600).
If live data errors, open /api/raw?underlying=NIFTY to see Kotak's raw response (parser is in backend/main.py: normalize()).
