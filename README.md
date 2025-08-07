# Namada MASP Reward Historical Data

This repo uses a github action with python script to track the `last_inflation` and `last_locked` amounts for each token per MASP epoch. It runs once weekly and writes the results to a csv file.  

When running the python script, make sure to use an RPC node that has its max block look-back set high enough for the number of blocks behind you wish to query. There's a helper script to help you find which RPC nodes are configured as archive nodes.
