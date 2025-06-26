# HONEST MPA Price Feed Monitor

This project is a **JavaScript-based web app** that monitors the status of all price feed producers for the HONEST Market-Pegged Assets (MPAs) on the BitShares blockchain.

Please note that this software is in the alpha stage, and things like display on mobile devices have not been ironed out yet.  That said, it works great on 16:9

## What it does

 - Connects to multiple BitShares API nodes via WebSocket
 - Fetches settlement price feeds for all HONEST MPAs
 - Calculates median settlement price per asset
 - Checks each producer's feed:
 - - Flags stale feeds (older than 1.5 hours)
 - - Flags feeds that differ from the median by more than 5%
 - Displays results in a dynamic HTML table:
 - - Shows producer names, tokens, and issues
 - - Highlights problematic feeds for quick visual inspection
 - Runs automatically every 5 minutes

## Where it runs

This app is designed for deployment **directly in a browser** — no server required.
You can host it on any static site (GitHub Pages, Netlify, your own domain).  It is currently hosted at https://squidKid-deluxe.github.io/honest-feed-status.

## Technologies used

* **Vanilla JavaScript**
* **WebSockets**
* **HTML/CSS** for the display
