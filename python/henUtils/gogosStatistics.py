import numpy as np
from henUtils.queryUtils import *
from henUtils.plotUtils import *

# Exclude the last day from most of the plots?
exclude_last_day = False

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

# Get the complete list of objkt.com bid, ask, english auction and dutch auction
# transactions
bid_transactions = get_all_transactions("bid", transactions_dir, sleep_time=1)
ask_transactions = get_all_transactions("ask", transactions_dir, sleep_time=1)
english_auction_transactions = get_all_transactions(
    "english_auction", transactions_dir, sleep_time=1)
dutch_auction_transactions = get_all_transactions(
    "dutch_auction", transactions_dir, sleep_time=1)

# Get the objkt.com bigmaps
bids_bigmap = get_objktcom_bigmap(
    "bids", "gogo", transactions_dir, sleep_time=1)
asks_bigmap = get_objktcom_bigmap(
    "asks", "gogo", transactions_dir, sleep_time=1)
english_auctions_bigmap = get_objktcom_bigmap(
    "english auctions", "gogo", transactions_dir, sleep_time=1)
dutch_auctions_bigmap = get_objktcom_bigmap(
    "dutch auctions", "gogo", transactions_dir, sleep_time=1)

# Select only the bids and asks transactions related with GOGOs
bid_transactions = [transaction for transaction in bid_transactions if 
                    transaction["parameter"]["value"] in bids_bigmap]
ask_transactions = [transaction for transaction in ask_transactions if 
                    transaction["parameter"]["value"] in asks_bigmap]
english_auction_transactions = [
    transaction for transaction in english_auction_transactions if 
    transaction["parameter"]["value"] in english_auctions_bigmap]
dutch_auction_transactions = [
    transaction for transaction in dutch_auction_transactions if 
    transaction["parameter"]["value"] in dutch_auctions_bigmap]

# Get only the english auction transactions that resulted in a successful sell
english_auction_transactions = [
    transaction for transaction in english_auction_transactions if 
    english_auctions_bigmap[transaction["parameter"]["value"]]["current_price"] != "0"]

# Get the H=N registries bigmap
registries_bigmap = get_hen_bigmap("registries", transactions_dir, sleep_time=1)

# Plot the number of operations per day
plot_operations_per_day(
    bid_transactions, "GOGO objkt.com bid operations per day",
    "Days since first minted GOGO (18th of October)", "Bid operations per day",
    exclude_last_day=exclude_last_day, first_month=10, first_day=18)
save_figure(os.path.join(figures_dir, "gogo_bid_operations_per_day.png"))

plot_operations_per_day(
    ask_transactions, "GOGO objkt.com ask operations per day",
    "Days since first minted GOGO (18th of October)", "Ask operations per day",
    exclude_last_day=exclude_last_day, first_month=10, first_day=18)
save_figure(os.path.join(figures_dir, "gogo_ask_operations_per_day.png"))

plot_operations_per_day(
    english_auction_transactions, "GOGO objkt.com english auction operations per day",
    "Days since first minted GOGO (18th of October)",
    "English auction operations per day", exclude_last_day=exclude_last_day,
    first_month=10, first_day=18)
save_figure(os.path.join(figures_dir, "gogo_english_auction_operations_per_day.png"))

plot_operations_per_day(
    dutch_auction_transactions, "GOGO objkt.com dutch auction operations per day",
    "Days since first minted GOGO (18th of October)",
    "Dutch auction operations per day", exclude_last_day=exclude_last_day,
    first_month=10, first_day=18)
save_figure(os.path.join(figures_dir, "gogo_dutch_auction_operations_per_day.png"))

# Extract the collector accounts
collectors = extract_objktcom_collector_accounts(
    bid_transactions, ask_transactions, english_auction_transactions,
    dutch_auction_transactions, bids_bigmap, asks_bigmap,
    english_auctions_bigmap, dutch_auctions_bigmap, registries_bigmap,
    tezos_wallets)

# Get the list of H=N reported users and add some extra ones that are suspect
# of buying their own OBJKTs with the only purpose to get the free hDAOs
reported_users = get_reported_users()
reported_users.append("tz1eee5rapGDbq2bcZYTQwNbrkB4jVSQSSHx")
reported_users.append("tz1Uby674S4xEw8w7iuM3GEkWZ3fHeHjT696")
reported_users.append("tz1bhMc5uPJynkrHpw7pAiBt6YMhQktn7owF")

# Add the reported users information
add_reported_users_information(collectors, reported_users)

# Print some information about the total number of users
print("There are currently %i unique GOGOs collectors." % len(collectors))

# Get the total money spent by non-reported collectors
wallet_ids = np.array([wallet_id for wallet_id in collectors])
aliases = np.array([collector["alias"] for collector in collectors.values()])
total_money_spent = np.array(
    [collector["total_money_spent"] for collector in collectors.values()])
items = np.array([collector["items"] for collector in collectors.values()])
is_reported_collector = np.array(
    [collector["reported"] for collector in collectors.values()])
wallet_ids = wallet_ids[~is_reported_collector]
aliases = aliases[~is_reported_collector]
total_money_spent = total_money_spent[~is_reported_collector]
items = items[~is_reported_collector]

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
                items[main_wallet_index] += items[secondary_wallet_index]
                is_secondary_wallet[secondary_wallet_index] = True

wallet_ids = wallet_ids[~is_secondary_wallet]
aliases = aliases[~is_secondary_wallet]
total_money_spent = total_money_spent[~is_secondary_wallet]
items = items[~is_secondary_wallet]

# Plot a histogram of the collectors that spent more than 0tez
plot_histogram(
    total_money_spent[total_money_spent >= 0],
    title="GOGOs collectors",
    x_label="Total money spent (tez)", y_label="Number of collectors", bins=100)
save_figure(os.path.join(figures_dir, "gogo_top_collectors_histogram.png"))

# Order the collectors by the money that they spent
sorted_indices = np.argsort(total_money_spent)[::-1]
wallet_ids = wallet_ids[sorted_indices]
aliases = aliases[sorted_indices]
total_money_spent = total_money_spent[sorted_indices]
items = items[sorted_indices]
collectors_ranking = []

for i in range(len(wallet_ids)):
    collectors_ranking.append({
            "ranking": i + 1,
            "wallet_id": wallet_ids[i],
            "alias": aliases[i],
            "total_money_spent": total_money_spent[i],
            "items": int(items[i])
        })

print("Non-reported collectors spent a total of %.0f tez." % np.sum(total_money_spent))

for i in [10, 100, 200, 300, 500]:
    print("%.1f%% of that was spent by the top %i collectors." % (
        100 * np.sum(total_money_spent[:i]) / np.sum(total_money_spent), i))

# Print the list of the top 100 collectors
print("\n This is the list of the top 100 collectors:\n")

for i, collector in enumerate(collectors_ranking[:100]):
    if collector["alias"] != "":
        print(" %3i: Collector %s spent %5.0f tez for %2i GOGOs (%s)" % (
            i + 1, collector["wallet_id"], collector["total_money_spent"], collector["items"], collector["alias"]))
    else:
        print(" %3i: Collector %s spent %5.0f tez for %2i GOGOs" % (
            i + 1, collector["wallet_id"], collector["total_money_spent"], collector["items"]))

# Save the collectors ranking list in a json file
save_json_file("gogo_collectors_ranking.json", collectors_ranking)

# Get the collected money that doesn't come from a reported user
collect_timestamps = []
collect_money = []

for wallet_id, collector in collectors.items():
    if wallet_id not in reported_users:
        collect_timestamps += collector["bid_timestamps"]
        collect_timestamps += collector["ask_timestamps"]
        collect_timestamps += collector["english_auction_timestamps"]
        collect_timestamps += collector["dutch_auction_timestamps"]
        collect_money += collector["bid_money_spent"]
        collect_money += collector["ask_money_spent"]
        collect_money += collector["english_auction_money_spent"]
        collect_money += collector["dutch_auction_money_spent"]

collect_timestamps = np.array(collect_timestamps)
collect_money = np.array(collect_money)

# Plot the money spent in collect operations per day
plot_data_per_day(
    collect_money, collect_timestamps,
    "Money spent per day",
    "Days since first minted GOGO (18th of October)", "Money spent (tez)",
    exclude_last_day=exclude_last_day, first_month=10, first_day=18)
save_figure(os.path.join(figures_dir, "gogo_money_per_day.png"))
