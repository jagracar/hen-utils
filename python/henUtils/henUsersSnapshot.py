import numpy as np
from henUtils.queryUtils import *

# Set the path to the directory where the transaction information will be saved
# to avoid to query for it again and again
transactions_dir = "../data/transactions"

# Set the path to the directory where the tezos wallets information will be
# saved to avoid to query for it again and again
tezos_dir = "../data/tezos"

# Get the complete list of tezos wallets
tezos_wallets = get_tezos_wallets(tezos_dir, sleep_time=1)

# Get the complete list of HEN mint, collect and swap transactions
mint_transactions = get_all_transactions("mint", transactions_dir, sleep_time=1)
collect_transactions = get_all_transactions(
    "collect", transactions_dir, sleep_time=1)
swap_transactions = get_all_transactions("swap", transactions_dir, sleep_time=1)

# Get the complete list of objkt.com bid, ask, english auction and dutch auction
# transactions
bid_transactions = get_all_transactions("bid", transactions_dir, sleep_time=1)
ask_transactions = get_all_transactions("ask", transactions_dir, sleep_time=1)
english_auction_transactions = get_all_transactions(
    "english_auction", transactions_dir, sleep_time=1)
dutch_auction_transactions = get_all_transactions(
    "dutch_auction", transactions_dir, sleep_time=1)

# Get the H=N bigmaps
swaps_bigmap = get_hen_bigmap("swaps", transactions_dir, sleep_time=1)
registries_bigmap = get_hen_bigmap("registries", transactions_dir, sleep_time=1)

# Get the objkt.com bigmaps associated to OBJKTs
objkt_bids_bigmap = get_objktcom_bigmap(
    "bids", "OBJKT", transactions_dir, sleep_time=1)
objkt_asks_bigmap = get_objktcom_bigmap(
    "asks", "OBJKT", transactions_dir, sleep_time=1)
objkt_english_auctions_bigmap = get_objktcom_bigmap(
    "english auctions", "OBJKT", transactions_dir, sleep_time=1)
objkt_dutch_auctions_bigmap = get_objktcom_bigmap(
    "dutch auctions", "OBJKT", transactions_dir, sleep_time=1)

# Select the objkt.com transactions related with H=N OBJKTs
objkt_bid_transactions = [
    transaction for transaction in bid_transactions if 
    transaction["parameter"]["value"] in objkt_bids_bigmap]
objkt_ask_transactions = [
    transaction for transaction in ask_transactions if 
    transaction["parameter"]["value"] in objkt_asks_bigmap]
objkt_english_auction_transactions = [
    transaction for transaction in english_auction_transactions if 
    transaction["parameter"]["value"] in objkt_english_auctions_bigmap]
objkt_dutch_auction_transactions = [
    transaction for transaction in dutch_auction_transactions if 
    transaction["parameter"]["value"] in objkt_dutch_auctions_bigmap]

# Get only the english auction transactions that resulted in a successful sell
objkt_english_auction_transactions = [
    transaction for transaction in objkt_english_auction_transactions if 
    objkt_english_auctions_bigmap[transaction["parameter"]["value"]]["current_price"] != "0"]

# Extract the artists, collector and patron accounts
artists = extract_artist_accounts(
    mint_transactions, registries_bigmap, tezos_wallets)
collectors = extract_collector_accounts(
    collect_transactions, registries_bigmap, swaps_bigmap, tezos_wallets)
swappers = extract_swapper_accounts(
    swap_transactions, registries_bigmap, tezos_wallets)
patrons = get_patron_accounts(artists, collectors)
users = get_user_accounts(artists, patrons, swappers)
objkt_objktcom_collectors = extract_objktcom_collector_accounts(
    objkt_bid_transactions, objkt_ask_transactions,
    objkt_english_auction_transactions, objkt_dutch_auction_transactions,
    objkt_bids_bigmap, objkt_asks_bigmap, objkt_english_auctions_bigmap,
    objkt_dutch_auctions_bigmap, registries_bigmap, tezos_wallets)

# Get the list of H=N reported users and add some extra ones that are suspect
# of buying their own OBJKTs with the only purpose to get the free hDAOs
reported_users = get_reported_users()

# Add the reported users information
add_reported_users_information(artists, reported_users)
add_reported_users_information(collectors, reported_users)
add_reported_users_information(swappers, reported_users)
add_reported_users_information(patrons, reported_users)
add_reported_users_information(users, reported_users)
add_reported_users_information(objkt_objktcom_collectors, reported_users)

# Get the objkt.com collectors that never used the H=N contracts
objkt_objktcom_only_collectors = {
    key: value for key, value in objkt_objktcom_collectors.items() if key not in users}

# Print some information about the total number of users
print("There are currently %i unique users in hic et nunc." % (
    len(users) + len(objkt_objktcom_only_collectors)))
print("Of those %i are artists and %i are patrons." % (
    len(artists), len(patrons) + len(objkt_objktcom_only_collectors)))
print("%i artists are also collectors." % (len(collectors) - len(patrons)))
print("%i users are in the block list." % len(reported_users))
print("There are %i unique users in objkt.com that collected an OBJKT "
      "using the objkt.com smart contracts." % len(objkt_objktcom_collectors))
print("%i objkt.com users never used the H=N smart contracts to collect "
      "OBJKTs." % len(objkt_objktcom_only_collectors))

# Combine the H=N users and the objkt.com OBJKT related users
combined_users = list(users.keys()) + list(objkt_objktcom_only_collectors.keys())

# Select only those that are not in the block list
combined_users = [user for user in combined_users if not user in reported_users]

# Save the users snapshot in a json file
save_json_file("users_snapshot.json", combined_users)
