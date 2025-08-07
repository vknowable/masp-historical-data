# Namada MASP Reward Historical Data

This repo uses a github action with python script to track the `last_inflation` and `last_locked` amounts for each token per MASP epoch. It runs once weekly and writes the results to a csv file.  

When running the python script, make sure to use an RPC node that has its max block look-back set high enough for the number of blocks behind you wish to query. There's a helper script to help you find which RPC nodes are configured as archive nodes.

**Note:** Each run, the script will attempt to query from the latest block height working backwards to the latest MASP epoch of the most recent job. Therefore, if you're stitching the files together into a single dataset, there shouldn't be any duplication between files; but you should still check when doing so to be safe (you can filter for duplicates by MASP epoch, since there should only be one entry per token per MASP epoch).

### Scripts:
- `fetch_data.py`: the masp data fetching script
- `find_latest_masp_epoch.py`: helper script to find the most recent masp epoch we have data for
- `archive-node-test.py`: this is not used directly in this repo; but it can be useful if you want to manually run the masp data fetch and need to find an archive node
