import numpy as np
from henUtils.queryUtils import *
from henUtils.plotUtils import *

# Exclude the last day from most of the plots?
exclude_last_day = True

# Set the path to the directory where the transaction information will be saved
# to avoid to query for it again and again
transactions_dir = "../data/transactions"

# Set the path to the directory where the tezos wallets information will be
# saved to avoid to query for it again and again
tezos_dir = "../data/tezos"

# Set the path to the directory where the figures will be saved
figures_dir = "../figures"

# Get the complete list of tezos wallets
tezos_wallets = get_tezos_wallets(tezos_dir, sleep_time=1)

# Read the connected wallets information (wallets connected to the same user)
connected_wallets = read_json_file("../data/connected_wallets.json")

# Get the complete list of HEN mint, collect, swap, cancel swap and burn
# transactions
mint_transactions = get_all_transactions("mint", transactions_dir, sleep_time=1)
collect_transactions = get_all_transactions(
    "collect", transactions_dir, sleep_time=1)
swap_transactions = get_all_transactions("swap", transactions_dir, sleep_time=1)
cancel_swap_transactions = get_all_transactions(
    "cancel_swap", transactions_dir, sleep_time=1)
burn_transactions = get_all_transactions("burn", transactions_dir, sleep_time=1)

# Get the complete list of fxhash mint, collect, offer and cancel_offer
# transactions
fxhash_mint_issuer_transactions = get_all_transactions(
    "fxhash_mint_issuer", transactions_dir, sleep_time=1)
fxhash_update_issuer_transactions = get_all_transactions(
    "fxhash_update_issuer", transactions_dir, sleep_time=1)
fxhash_mint_transactions = get_all_transactions(
    "fxhash_mint", transactions_dir, sleep_time=1)
fxhash_collect_transactions = get_all_transactions(
    "fxhash_collect", transactions_dir, sleep_time=1)
fxhash_offer_transactions = get_all_transactions(
    "fxhash_offer", transactions_dir, sleep_time=1)
fxhash_cancel_offer_transactions = get_all_transactions(
    "fxhash_cancel_offer", transactions_dir, sleep_time=1)

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
royalties_bigmap = get_hen_bigmap("royalties", transactions_dir, sleep_time=1)
registries_bigmap = get_hen_bigmap("registries", transactions_dir, sleep_time=1)
subjkts_metadata_bigmap = get_hen_bigmap(
    "subjkts metadata", transactions_dir, sleep_time=1)

# Get the fxhash bigmaps
fxhash_offer_bigmap = get_fxhash_bigmap(
    "offers", transactions_dir, sleep_time=1)
fxhash_users_name_bigmap = get_fxhash_bigmap(
    "users_name", transactions_dir, sleep_time=1)
fxhash_collections_bigmap = get_fxhash_bigmap(
    "collections", transactions_dir, sleep_time=1)

# Get the objkt.com bigmaps associated to OBJKTs
objkt_bids_bigmap = get_objktcom_bigmap(
    "bids", "OBJKT", transactions_dir, sleep_time=1)
objkt_asks_bigmap = get_objktcom_bigmap(
    "asks", "OBJKT", transactions_dir, sleep_time=1)
objkt_english_auctions_bigmap = get_objktcom_bigmap(
    "english auctions", "OBJKT", transactions_dir, sleep_time=1)
objkt_dutch_auctions_bigmap = get_objktcom_bigmap(
    "dutch auctions", "OBJKT", transactions_dir, sleep_time=1)
objkt_minter_bigmap = get_objktcom_bigmap(
    "minter", "all", transactions_dir, sleep_time=1)

collections_contracts = [entry["contract"] for entry in objkt_minter_bigmap.values()]

# Get the objkt.com bigmaps associated to all the big tezos tokens
collections_bids_bigmap = get_objktcom_bigmap(
    "bids", "collections", transactions_dir, sleep_time=1,
    token_addresses=collections_contracts)
collections_asks_bigmap = get_objktcom_bigmap(
    "asks", "collections", transactions_dir, sleep_time=1,
    token_addresses=collections_contracts)
collections_english_auctions_bigmap = get_objktcom_bigmap(
    "english auctions", "collections", transactions_dir, sleep_time=1,
    token_addresses=collections_contracts)
collections_dutch_auctions_bigmap = get_objktcom_bigmap(
    "dutch auctions", "collections", transactions_dir, sleep_time=1,
    token_addresses=collections_contracts)

# Get the objkt.com bigmaps associated to all the big tezos tokens
all_bids_bigmap = get_objktcom_bigmap(
    "bids", "all", transactions_dir, sleep_time=1)
all_asks_bigmap = get_objktcom_bigmap(
    "asks", "all", transactions_dir, sleep_time=1)
all_english_auctions_bigmap = get_objktcom_bigmap(
    "english auctions", "all", transactions_dir, sleep_time=1)
all_dutch_auctions_bigmap = get_objktcom_bigmap(
    "dutch auctions", "all", transactions_dir, sleep_time=1)

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

# Select the objkt.com transactions related with objkt.com collections
collections_bid_transactions = [
    transaction for transaction in bid_transactions if 
    transaction["parameter"]["value"] in collections_bids_bigmap]
collections_ask_transactions = [
    transaction for transaction in ask_transactions if 
    transaction["parameter"]["value"] in collections_asks_bigmap]
collections_english_auction_transactions = [
    transaction for transaction in english_auction_transactions if 
    transaction["parameter"]["value"] in collections_english_auctions_bigmap]
collections_dutch_auction_transactions = [
    transaction for transaction in dutch_auction_transactions if 
    transaction["parameter"]["value"] in collections_dutch_auctions_bigmap]

# Select the objkt.com transactions related with the main tezos tokens
all_bid_transactions = [
    transaction for transaction in bid_transactions if 
    transaction["parameter"]["value"] in all_bids_bigmap]
all_ask_transactions = [
    transaction for transaction in ask_transactions if 
    transaction["parameter"]["value"] in all_asks_bigmap]
all_english_auction_transactions = [
    transaction for transaction in english_auction_transactions if 
    transaction["parameter"]["value"] in all_english_auctions_bigmap]
all_dutch_auction_transactions = [
    transaction for transaction in dutch_auction_transactions if 
    transaction["parameter"]["value"] in all_dutch_auctions_bigmap]

# Get only the english auction transactions that resulted in a successful sell
objkt_english_auction_transactions = [
    transaction for transaction in objkt_english_auction_transactions if 
    objkt_english_auctions_bigmap[transaction["parameter"]["value"]]["current_price"] != "0"]
collections_english_auction_transactions = [
    transaction for transaction in collections_english_auction_transactions if 
    collections_english_auctions_bigmap[transaction["parameter"]["value"]]["current_price"] != "0"]
all_english_auction_transactions = [
    transaction for transaction in all_english_auction_transactions if 
    all_english_auctions_bigmap[transaction["parameter"]["value"]]["current_price"] != "0"]

# Plot the number of operations per day
plot_operations_per_day(
    mint_transactions, "OBJKT mint operations per day",
    "Days since first minted OBJKT (1st of March)", "Mint operations per day",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "objkt_mint_operations_per_day.png"))

plot_operations_per_day(
    fxhash_mint_transactions, "fxhash mint operations per day",
    "Days since first minted GENTK (10th of November)", "Mint operations per day",
    exclude_last_day=exclude_last_day, first_month=11, first_day=10)
save_figure(os.path.join(figures_dir, "fxhash_mint_operations_per_day.png"))

plot_operations_per_day(
    collect_transactions, "OBJKT collect operations per day",
    "Days since first minted OBJKT (1st of March)", "Collect operations per day",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "objkt_collect_operations_per_day.png"))

plot_operations_per_day(
    fxhash_collect_transactions, "fxhash collect operations per day",
    "Days since first minted GENTK (10th of November)", "Collect operations per day",
    exclude_last_day=exclude_last_day, first_month=11, first_day=10)
save_figure(os.path.join(figures_dir, "fxhash_collect_operations_per_day.png"))

plot_operations_per_day(
    swap_transactions, "OBJKT swap operations per day",
    "Days since first minted OBJKT (1st of March)", "Swap operations per day",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "objkt_swap_operations_per_day.png"))

plot_operations_per_day(
    fxhash_offer_transactions, "fxhash offer operations per day",
    "Days since first minted GENTK (10th of November)", "Offer operations per day",
    exclude_last_day=exclude_last_day, first_month=11, first_day=10)
save_figure(os.path.join(figures_dir, "fxhash_offer_operations_per_day.png"))

plot_operations_per_day(
    cancel_swap_transactions, "OBJKT cancel_swap operations per day",
    "Days since first minted OBJKT (1st of March)", "cancel_swap operations per day",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "objkt_cancel_swap_operations_per_day.png"))

plot_operations_per_day(
    fxhash_cancel_offer_transactions, "fxhash cancel_offer operations per day",
    "Days since first minted GENTK (10th of November)", "Cancel_offer operations per day",
    exclude_last_day=exclude_last_day, first_month=11, first_day=10)
save_figure(os.path.join(figures_dir, "fxhash_cancel_offer_operations_per_day.png"))

plot_operations_per_day(
    burn_transactions, "OBJKT burn operations per day",
    "Days since first minted OBJKT (1st of March)", "burn operations per day",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "objkt_burn_operations_per_day.png"))

plot_operations_per_day(
    objkt_bid_transactions, "OBJKT objkt.com bid operations per day",
    "Days since first minted OBJKT (1st of March)", "Bid operations per day",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "objkt_bid_operations_per_day.png"))

plot_operations_per_day(
    collections_bid_transactions, "Collections objkt.com bid operations per day",
    "Days since first minted OBJKT (1st of March)", "Bid operations per day",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "collections_bid_operations_per_day.png"))

plot_operations_per_day(
    objkt_ask_transactions, "OBJKT objkt.com ask operations per day",
    "Days since first minted OBJKT (1st of March)", "Ask operations per day",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "objkt_ask_operations_per_day.png"))

plot_operations_per_day(
    collections_ask_transactions, "Collections objkt.com ask operations per day",
    "Days since first minted OBJKT (1st of March)", "Ask operations per day",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "collections_ask_operations_per_day.png"))

plot_operations_per_day(
    objkt_english_auction_transactions,
    "OBJKT objkt.com english auction operations per day",
    "Days since first minted OBJKT (1st of March)",
    "English auction operations per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "objkt_english_auction_operations_per_day.png"))

plot_operations_per_day(
    collections_english_auction_transactions,
    "Collections objkt.com english auction operations per day",
    "Days since first minted OBJKT (1st of March)",
    "English auction operations per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "collections_english_auction_operations_per_day.png"))

plot_operations_per_day(
    objkt_dutch_auction_transactions,
    "OBJKT objkt.com dutch auction operations per day",
    "Days since first minted OBJKT (1st of March)",
    "Dutch auction operations per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "objkt_dutch_auction_operations_per_day.png"))

plot_operations_per_day(
    collections_dutch_auction_transactions,
    "Collections objkt.com dutch auction operations per day",
    "Days since first minted OBJKT (1st of March)",
    "Dutch auction operations per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "collections_dutch_auction_operations_per_day.png"))

# Extract the artists, collector and patron accounts
artists = extract_artist_accounts(
    mint_transactions, registries_bigmap, tezos_wallets)
collectors = extract_collector_accounts(
    collect_transactions, registries_bigmap, swaps_bigmap, tezos_wallets)
swappers = extract_swapper_accounts(
    swap_transactions, registries_bigmap, tezos_wallets)
patrons = get_patron_accounts(artists, collectors)
users = get_user_accounts(artists, patrons, swappers)
fxhash_artists = {
    collection["artist"] : 0 for collection in fxhash_collections_bigmap.values()}
fxhash_collectors = extract_fxhash_collector_accounts(
    fxhash_mint_transactions, fxhash_collect_transactions, registries_bigmap,
    tezos_wallets)
objkt_objktcom_collectors = extract_objktcom_collector_accounts(
    objkt_bid_transactions, objkt_ask_transactions,
    objkt_english_auction_transactions, objkt_dutch_auction_transactions,
    objkt_bids_bigmap, objkt_asks_bigmap, objkt_english_auctions_bigmap,
    objkt_dutch_auctions_bigmap, registries_bigmap, tezos_wallets)
collections_objktcom_collectors = extract_objktcom_collector_accounts(
    collections_bid_transactions, collections_ask_transactions,
    collections_english_auction_transactions,
    collections_dutch_auction_transactions,
    collections_bids_bigmap, collections_asks_bigmap,
    collections_english_auctions_bigmap, collections_dutch_auctions_bigmap,
    registries_bigmap, tezos_wallets)
all_objktcom_collectors = extract_objktcom_collector_accounts(
    all_bid_transactions, all_ask_transactions,
    all_english_auction_transactions, all_dutch_auction_transactions,
    all_bids_bigmap, all_asks_bigmap, all_english_auctions_bigmap,
    all_dutch_auctions_bigmap, registries_bigmap, tezos_wallets)

# Get the list of H=N reported users and add some extra ones that are suspect
# of buying their own OBJKTs with the only purpose to get the free hDAOs
reported_users = get_reported_users()
reported_users.append("tz1eee5rapGDbq2bcZYTQwNbrkB4jVSQSSHx")
reported_users.append("tz1Uby674S4xEw8w7iuM3GEkWZ3fHeHjT696")
reported_users.append("tz1bhMc5uPJynkrHpw7pAiBt6YMhQktn7owF")

# Add the reported users information
add_reported_users_information(artists, reported_users)
add_reported_users_information(collectors, reported_users)
add_reported_users_information(swappers, reported_users)
add_reported_users_information(patrons, reported_users)
add_reported_users_information(users, reported_users)
add_reported_users_information(fxhash_collectors, reported_users)
add_reported_users_information(objkt_objktcom_collectors, reported_users)
add_reported_users_information(collections_objktcom_collectors, reported_users)
add_reported_users_information(all_objktcom_collectors, reported_users)

# Get the objkt.com collectors that never used the H=N contracts
objkt_objktcom_only_collectors = {
    key: value for key, value in objkt_objktcom_collectors.items() if key not in users}

all_objktcom_only_collectors = {
    key: value for key, value in all_objktcom_collectors.items() if key not in users}

# Get a dictionary with the OBJKT creators
objkt_creators = get_objkt_creators(mint_transactions)

# Get a dictionary with the users connections
users_connections, serialized_users_connections = extract_users_connections(
    objkt_creators, collect_transactions, swaps_bigmap, users,
    objkt_objktcom_collectors, reported_users)
save_json_file("users_connections.json", users_connections)
save_json_file(
    "serialized_users_connections.json", serialized_users_connections,
    compact=True)

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
print("There are %i unique users in objkt.com that collected one of the main "
      "tezos tokens using the objkt.com smart contracts." % len(
          all_objktcom_collectors))
print("There are %i unique fxhash artists." % len(fxhash_artists))
print("There are %i unique fxhash collectors." % len(fxhash_collectors))

# Get the total money spent by H=N collectors
wallet_ids = [wallet_id for wallet_id in collectors]
aliases = [collector["alias"] for collector in collectors.values()]
total_money_spent = [
    collector["total_money_spent"] for collector in collectors.values()]
is_reported_collector = [
    collector["reported"] for collector in collectors.values()]

# Add the money spent in objkt.com
for i, wallet_id in enumerate(wallet_ids):
    if wallet_id in objkt_objktcom_collectors:
        total_money_spent[i] += objkt_objktcom_collectors[
            wallet_id]["total_money_spent"]

# Add the objkt.com only collectors
wallet_ids = np.array(
    wallet_ids + [wallet_id for wallet_id in objkt_objktcom_only_collectors])
aliases = np.array(aliases + [
    collector["alias"] for collector in objkt_objktcom_only_collectors.values()])
total_money_spent = np.array(total_money_spent + [
    collector["total_money_spent"] for collector in objkt_objktcom_only_collectors.values()])
is_reported_collector = np.array(is_reported_collector + [
    collector["reported"] for collector in objkt_objktcom_only_collectors.values()])

# Select only the data from non-reported collectors
wallet_ids = wallet_ids[~is_reported_collector]
aliases = aliases[~is_reported_collector]
total_money_spent = total_money_spent[~is_reported_collector]

# Combine those wallets that are connected to the same user
is_secondary_wallet = np.full(wallet_ids.shape, False)

for main_wallet_id, secondary_wallet_ids in connected_wallets.items():
    main_wallet_index = np.where(wallet_ids == main_wallet_id)[0]

    if len(main_wallet_index) == 1:
        for secondary_wallet_id in secondary_wallet_ids:
            secondary_wallet_index = np.where(
                wallet_ids == secondary_wallet_id)[0]

            if len(secondary_wallet_index) == 1:
                total_money_spent[main_wallet_index] += total_money_spent[
                    secondary_wallet_index]
                is_secondary_wallet[secondary_wallet_index] = True

wallet_ids = wallet_ids[~is_secondary_wallet]
aliases = aliases[~is_secondary_wallet]
total_money_spent = total_money_spent[~is_secondary_wallet]

# Plot a histogram of the collectors that spent more than 100tez
plot_histogram(
    total_money_spent[total_money_spent >= 100],
    title="OBJKT collectors that spent more than 100tez",
    x_label="Total money spent (tez)", y_label="Number of collectors", bins=100)
save_figure(os.path.join(figures_dir, "objkt_top_collectors_histogram.png"))

# Order the collectors by the money that they spent
sorted_indices = np.argsort(total_money_spent)[::-1]
wallet_ids = wallet_ids[sorted_indices]
aliases = aliases[sorted_indices]
total_money_spent = total_money_spent[sorted_indices]
collectors_ranking = []

for i in range(len(wallet_ids)):
    collectors_ranking.append({
            "ranking": i + 1,
            "wallet_id": wallet_ids[i],
            "alias": aliases[i],
            "total_money_spent": total_money_spent[i]
        })

print("Non-reported collectors spent a total of %.0f tez." % np.sum(total_money_spent))

for i in [10, 100, 500, 1000, 10000]:
    print("%.1f%% of that was spent by the top %i collectors." % (
        100 * np.sum(total_money_spent[:i]) / np.sum(total_money_spent), i))

# Print the list of the top 100 collectors
print("\n This is the list of the top 100 collectors:\n")

for i, collector in enumerate(collectors_ranking[:100]):
    if collector["alias"] != "":
        print(" %3i: Collector %s spent %6.0f tez (%s)" % (
            i + 1, collector["wallet_id"], collector["total_money_spent"], collector["alias"]))
    else:
        print(" %3i: Collector %s spent %6.0f tez" % (
            i + 1, collector["wallet_id"], collector["total_money_spent"]))

# Save the collectors ranking list in a json file
save_json_file("objkt_collectors_ranking.json", collectors_ranking)

# Get the collected money that doesn't come from a reported user
collect_is_secondary = []
collect_from_patron = []
collect_timestamps = []
collect_money = []

for transaction in collect_transactions:
    wallet_id = transaction["sender"]["address"]

    if wallet_id not in reported_users:
        if "swap_id" in transaction["parameter"]["value"]:
            swap_id = transaction["parameter"]["value"]["swap_id"]
        else:
            swap_id = transaction["parameter"]["value"]

        swap = swaps_bigmap[swap_id]
        collect_is_secondary.append(
            objkt_creators[swap["objkt_id"]] != swap["issuer"])
        collect_from_patron.append(wallet_id in patrons)
        collect_timestamps.append(transaction["timestamp"])
        collect_money.append(transaction["amount"] / 1e6)

collect_is_secondary = np.array(collect_is_secondary)
collect_from_patron = np.array(collect_from_patron)
collect_timestamps = np.array(collect_timestamps)
collect_money = np.array(collect_money)

# Get the fxhash collected money that doesn't come from a reported user
fxhash_collect_timestamps = []
fxhash_collect_money = []

for wallet_id, collector in fxhash_collectors.items():
    if wallet_id not in reported_users:
        fxhash_collect_timestamps += collector["mint_timestamps"]
        fxhash_collect_timestamps += collector["collect_timestamps"]
        fxhash_collect_money += collector["mint_money_spent"]
        fxhash_collect_money += collector["collect_money_spent"]

fxhash_collect_timestamps = np.array(fxhash_collect_timestamps)
fxhash_collect_money = np.array(fxhash_collect_money)

# Get the objkt.com OBJKT collected money that doesn't come from a reported user
objktcom_collect_timestamps = []
objktcom_collect_money = []

for wallet_id, collector in objkt_objktcom_collectors.items():
    if wallet_id not in reported_users:
        objktcom_collect_timestamps += collector["bid_timestamps"]
        objktcom_collect_timestamps += collector["ask_timestamps"]
        objktcom_collect_timestamps += collector["english_auction_timestamps"]
        objktcom_collect_timestamps += collector["dutch_auction_timestamps"]
        objktcom_collect_money += collector["bid_money_spent"]
        objktcom_collect_money += collector["ask_money_spent"]
        objktcom_collect_money += collector["english_auction_money_spent"]
        objktcom_collect_money += collector["dutch_auction_money_spent"]

objktcom_collect_timestamps = np.array(objktcom_collect_timestamps)
objktcom_collect_money = np.array(objktcom_collect_money)

# Get the collections objkt.com collected money that doesn't come from a reported user
collections_objktcom_collect_timestamps = []
collections_objktcom_collect_money = []

for wallet_id, collector in collections_objktcom_collectors.items():
    if wallet_id not in reported_users:
        collections_objktcom_collect_timestamps += collector["bid_timestamps"]
        collections_objktcom_collect_timestamps += collector["ask_timestamps"]
        collections_objktcom_collect_timestamps += collector["english_auction_timestamps"]
        collections_objktcom_collect_timestamps += collector["dutch_auction_timestamps"]
        collections_objktcom_collect_money += collector["bid_money_spent"]
        collections_objktcom_collect_money += collector["ask_money_spent"]
        collections_objktcom_collect_money += collector["english_auction_money_spent"]
        collections_objktcom_collect_money += collector["dutch_auction_money_spent"]

collections_objktcom_collect_timestamps = np.array(collections_objktcom_collect_timestamps)
collections_objktcom_collect_money = np.array(collections_objktcom_collect_money)

# Get all the objkt.com collected money that doesn't come from a reported user
all_objktcom_collect_timestamps = []
all_objktcom_collect_money = []

for wallet_id, collector in all_objktcom_collectors.items():
    if wallet_id not in reported_users:
        all_objktcom_collect_timestamps += collector["bid_timestamps"]
        all_objktcom_collect_timestamps += collector["ask_timestamps"]
        all_objktcom_collect_timestamps += collector["english_auction_timestamps"]
        all_objktcom_collect_timestamps += collector["dutch_auction_timestamps"]
        all_objktcom_collect_money += collector["bid_money_spent"]
        all_objktcom_collect_money += collector["ask_money_spent"]
        all_objktcom_collect_money += collector["english_auction_money_spent"]
        all_objktcom_collect_money += collector["dutch_auction_money_spent"]

all_objktcom_collect_timestamps = np.array(all_objktcom_collect_timestamps)
all_objktcom_collect_money = np.array(all_objktcom_collect_money)

# Plot a histogram of the collected editions prices
adapted_collect_money = np.hstack((collect_money, objktcom_collect_money))
adapted_collect_money[adapted_collect_money > 1000] = 999

plot_histogram(
    adapted_collect_money,
    title="OBJKT collected editions price distribution",
    x_label="Edition price (tez)", y_label="Number of collected editions",
    bins=[0, 0.1, 0.5, 1, 1.5, 3, 5, 8, 15, 30, 50, 100, 200, 400, 800, 1000],
    log=True)
save_figure(os.path.join(figures_dir, "objkt_edition_price_histogram.png"))

# Plot the money spent in collect operations per day
plot_data_per_day(
    collect_money, collect_timestamps,
    "Money spent in collect operations per day (H=N contract)",
    "Days since first minted OBJKT (1st of March)", "Money spent (tez)",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "objkt_hen_money_per_day.png"))

plot_data_per_day(
    objktcom_collect_money, objktcom_collect_timestamps,
    "Money spent in collect operations per day (objkt.com contracts)",
    "Days since first minted OBJKT (1st of March)", "Money spent (tez)",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "objkt_objktcom_money_per_day.png"))

plot_data_per_day(
    collections_objktcom_collect_money, collections_objktcom_collect_timestamps,
    "Money spent in collect operations per day (objkt.com contracts)",
    "Days since first minted OBJKT (1st of March)", "Money spent (tez)",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "collections_objktcom_money_per_day.png"))

plot_data_per_day(
    all_objktcom_collect_money, all_objktcom_collect_timestamps,
    "Money spent in collect operations per day (objkt.com contracts)",
    "Days since first minted OBJKT (1st of March)", "Money spent (tez)",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "all_objktcom_money_per_day.png"))

plot_data_per_day(
    np.hstack((collect_money, objktcom_collect_money)),
    np.hstack((collect_timestamps, objktcom_collect_timestamps)),
    "Money spent in collect operations per day (H=N + objkt.com contracts)",
    "Days since first minted OBJKT (1st of March)", "Money spent (tez)",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "objkt_money_per_day.png"))

plot_data_per_day(
    fxhash_collect_money, fxhash_collect_timestamps,
    "Money spent in mint and collect operations per day (fxhash contract)",
    "Days since first minted GENTK (10th of November)", "Money spent (tez)",
    exclude_last_day=exclude_last_day, first_month=11, first_day=10)
save_figure(os.path.join(figures_dir, "fxhash_money_per_day.png"))

plot_data_per_day(
    np.hstack((collect_money, all_objktcom_collect_money, fxhash_collect_money)),
    np.hstack((collect_timestamps, all_objktcom_collect_timestamps, fxhash_collect_timestamps)),
    "Money spent in collect operations per day (H=N + objkt.com + fxhash)",
    "Days since first minted OBJKT (1st of March)", "Money spent (tez)",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "all_money_per_day.png"))

plot_data_per_day(
    collect_money[~collect_is_secondary],
    collect_timestamps[~collect_is_secondary],
    "Money spent in primary market collect operations per day",
    "Days since first minted OBJKT (1st of March)", "Money spent (tez)",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "objkt_hen_money_primary_per_day.png"))

plot_data_per_day(
    collect_money[collect_is_secondary],
    collect_timestamps[collect_is_secondary],
    "Money spent in secondary market collect operations per day",
    "Days since first minted OBJKT (1st of March)", "Money spent (tez)",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "objkt_hen_money_secondary_per_day.png"))

plot_data_per_day(
    collect_money[~collect_from_patron],
    collect_timestamps[~collect_from_patron],
    "Money spent in collect operations per day by artists",
    "Days since first minted OBJKT (1st of March)", "Money spent (tez)",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "objkt_hen_money_per_day_by_artists.png"))

plot_data_per_day(
    collect_money[collect_from_patron], collect_timestamps[collect_from_patron],
    "Money spent in collect operations per day by patrons",
    "Days since first minted OBJKT (1st of March)", "Money spent (tez)",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "objkt_hen_money_per_day_by_patrons.png"))

plot_price_distribution_per_day(
    collect_money, collect_timestamps, [0.01, 1, 5, 50],
    "Price distribution of collected OBJKTs per day (H=N contract)",
    "Days since first minted OBJKT (1st of March)", "Number of collected OBJKTs",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "objkt_hen_price_distribution_per_day.png"))

plot_price_distribution_per_day(
    objktcom_collect_money, objktcom_collect_timestamps, [0.01, 1, 5, 50],
    "Price distribution of collected OBJKTs per day (objkt.com contracts)",
    "Days since first minted OBJKT (1st of March)", "Number of collected OBJKTs",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "objkt_objktcom_price_distribution_per_day.png"))

plot_price_distribution_per_day(
    np.hstack((collect_money, objktcom_collect_money)),
    np.hstack((collect_timestamps, objktcom_collect_timestamps)),
    [0.01, 1, 5, 50],
    "Price distribution of collected OBJKTs per day (H=N + objkt.com contracts)",
    "Days since first minted OBJKT (1st of March)", "Number of collected OBJKTs",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "objkt_price_distribution_per_day.png"))

plot_price_distribution_per_day(
    collect_money[~collect_is_secondary],
    collect_timestamps[~collect_is_secondary], [0.01, 1, 5, 50],
    "Price distribution of collected OBJKTs per day on the primary market",
    "Days since first minted OBJKT (1st of March)", "Number of collected OBJKTs",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "objkt_hen_price_distribution_per_day_primary.png"))

plot_price_distribution_per_day(
    collect_money[collect_is_secondary],
    collect_timestamps[collect_is_secondary], [0.01, 1, 5, 50],
    "Price distribution of collected OBJKTs per day on the secondary market",
    "Days since first minted OBJKT (1st of March)", "Number of collected OBJKTs",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "objkt_hen_price_distribution_per_day_secondary.png"))

plot_price_distribution_per_day(
    collect_money[~collect_from_patron],
    collect_timestamps[~collect_from_patron], [0.01, 1, 5, 50],
    "Price distribution of collected OBJKTs per day by artists",
    "Days since first minted OBJKT (1st of March)", "Number of collected OBJKTs",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "objkt_hen_price_distribution_per_day_by_artists.png"))

plot_price_distribution_per_day(
    collect_money[collect_from_patron], collect_timestamps[collect_from_patron],
    [0.01, 1, 5, 50],
    "Price distribution of collected OBJKTs per day by patrons",
    "Days since first minted OBJKT (1st of March)", "Number of collected OBJKTs",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "objkt_hen_price_distribution_per_day_by_patrons.png"))

# Print some information about the collect operations
combined_collect_money = np.hstack((collect_money, objktcom_collect_money))
print(" Non-reported users performed %i collect operations." % len(combined_collect_money))

for i in [0.0, 0.1, 0.5, 1, 2, 3, 5, 10, 100]:
    print(" %.1f%% of them were for editions with a value <= %.1ftez." % (
        100 * np.sum(combined_collect_money <= i) / len(combined_collect_money), i))

print(" Non-reported users spent a total of %.0f tez." % np.sum(combined_collect_money))

for i in [0.1, 0.5, 1, 2, 3, 5, 10, 100]:
    print(" %.1f%% of that was on editions with a value <= %.1ftez." % (
        100 * np.sum(combined_collect_money[combined_collect_money <= i]) / np.sum(combined_collect_money), i))

# Plot the new users per day
plot_new_users_per_day(
    artists, title="New artists per day",
    x_label="Days since first minted OBJKT (1st of March)",
    y_label="New artists per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "objkt_new_artists_per_day.png"))

plot_new_users_per_day(
    collectors, title="New collectors per day",
    x_label="Days since first minted OBJKT (1st of March)",
    y_label="New collectors per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "objkt_new_collectors_per_day.png"))

plot_new_users_per_day(
    patrons, title="New patrons per day",
    x_label="Days since first minted OBJKT (1st of March)",
    y_label="New patrons per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "objkt_new_patros_per_day.png"))

plot_new_users_per_day(
    users, title="New users per day",
    x_label="Days since first minted OBJKT (1st of March)",
    y_label="New users per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "objkt_new_users_per_day.png"))

# Get the wallet ids and the time stamps of each transaction
transactions_wallet_ids = []
transactions_timestamps = []

for transaction in mint_transactions:
    transactions_wallet_ids.append(transaction["initiator"]["address"])
    transactions_timestamps.append(transaction["timestamp"])

for transaction in collect_transactions:
    transactions_wallet_ids.append(transaction["sender"]["address"])
    transactions_timestamps.append(transaction["timestamp"])

for transaction in swap_transactions:
    transactions_wallet_ids.append(transaction["sender"]["address"])
    transactions_timestamps.append(transaction["timestamp"])

for transaction in burn_transactions:
    transactions_wallet_ids.append(transaction["sender"]["address"])
    transactions_timestamps.append(transaction["timestamp"])

transactions_wallet_ids = np.array(transactions_wallet_ids)
transactions_timestamps = np.array(transactions_timestamps)
transactions_is_artist = np.array([wallet in artists for wallet in transactions_wallet_ids])
transactions_is_patron = np.array([wallet in patrons for wallet in transactions_wallet_ids])

# Plot the active users per day
plot_active_users_per_day(
    transactions_wallet_ids, transactions_timestamps, users,
    "Active users per day",
    "Days since first minted OBJKT (1st of March)", "Active users per day",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "objkt_active_users_per_day.png"))

# Plot the users last active day
plot_users_last_active_day(
    transactions_wallet_ids, transactions_timestamps,
    "Users last active day",
    "Days since first minted OBJKT (1st of March)", "Users",
    exclude_last_day=False)
save_figure(os.path.join(figures_dir, "objkt_users_last_active_day.png"))

plot_users_last_active_day(
    transactions_wallet_ids[transactions_is_artist],
    transactions_timestamps[transactions_is_artist],
    "Artists last active day",
    "Days since first minted OBJKT (1st of March)", "Artists",
    exclude_last_day=False)
save_figure(os.path.join(figures_dir, "objkt_artists_last_active_day.png"))

plot_users_last_active_day(
    transactions_wallet_ids[transactions_is_patron],
    transactions_timestamps[transactions_is_patron],
    "Patrons last active day",
    "Days since first minted OBJKT (1st of March)", "Patrons",
    exclude_last_day=False)
save_figure(os.path.join(figures_dir, "objkt_patrons_last_active_day.png"))
