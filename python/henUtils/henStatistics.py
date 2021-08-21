import numpy as np
from henUtils.queryUtils import *
from henUtils.plotUtils import *

# Exclude the last day from most of the plots?
exclude_last_day = False

# Set the path to the directory where the transaction information will be saved
# to avoid to query for it again and again
transactions_dir = "../data/transactions"

# Set the path to the directory where the figures will be saved
figures_dir = "../figures"

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
subjkts_metadata_bigmap = get_hen_bigmap(
    "subjkts metadata", transactions_dir, sleep_time=1)

# Get the objkt.com bigmaps
bids_bigmap = get_objktcom_bigmap("bids", transactions_dir, sleep_time=1)
asks_bigmap = get_objktcom_bigmap("asks", transactions_dir, sleep_time=1)
english_auctions_bigmap = get_objktcom_bigmap(
    "english auctions", transactions_dir, sleep_time=1)
dutch_auctions_bigmap = get_objktcom_bigmap(
    "dutch auctions", transactions_dir, sleep_time=1)

# Select only the bids and asks transactions related with H=N OBJKTs
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

# Plot the number of operations per day
plot_operations_per_day(
    mint_transactions, "Mint operations per day",
    "Days since first minted OBJKT (1st of March)", "Mint operations per day",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "mint_operations_per_day.png"))

plot_operations_per_day(
    collect_transactions, "Collect operations per day",
    "Days since first minted OBJKT (1st of March)", "Collect operations per day",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "collect_operations_per_day.png"))

plot_operations_per_day(
    swap_transactions, "Swap operations per day",
    "Days since first minted OBJKT (1st of March)", "Swap operations per day",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "swap_operations_per_day.png"))

plot_operations_per_day(
    cancel_swap_transactions, "cancel_swap operations per day",
    "Days since first minted OBJKT (1st of March)", "cancel_swap operations per day",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "cancel_swap_operations_per_day.png"))

plot_operations_per_day(
    burn_transactions, "burn operations per day",
    "Days since first minted OBJKT (1st of March)", "burn operations per day",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "burn_operations_per_day.png"))

plot_operations_per_day(
    bid_transactions, "objkt.com bid operations per day",
    "Days since first minted OBJKT (1st of March)", "Bid operations per day",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "bid_operations_per_day.png"))

plot_operations_per_day(
    ask_transactions, "objkt.com ask operations per day",
    "Days since first minted OBJKT (1st of March)", "Ask operations per day",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "ask_operations_per_day.png"))

plot_operations_per_day(
    english_auction_transactions, "objkt.com english auction operations per day",
    "Days since first minted OBJKT (1st of March)",
    "English auction operations per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "english_auction_operations_per_day.png"))

plot_operations_per_day(
    dutch_auction_transactions, "objkt.com dutch auction operations per day",
    "Days since first minted OBJKT (1st of March)",
    "Dutch auction operations per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "dutch_auction_operations_per_day.png"))

# Extract the artists, collector and patron accounts
artists = extract_artist_accounts(mint_transactions)
collectors = extract_collector_accounts(collect_transactions)
patrons = get_patron_accounts(artists, collectors)
users = get_user_accounts(artists, patrons)
objktcom_collectors = extract_objktcom_collector_accounts(
    bid_transactions, ask_transactions, english_auction_transactions,
    dutch_auction_transactions, bids_bigmap, english_auctions_bigmap)

# Get the list of H=N reported users and add some extra ones that are suspect
# of buying their own OBJKTs with the only purpose to get the free hDAOs
reported_users = get_reported_users()
reported_users.append("tz1eee5rapGDbq2bcZYTQwNbrkB4jVSQSSHx")
reported_users.append("tz1Uby674S4xEw8w7iuM3GEkWZ3fHeHjT696")
reported_users.append("tz1bhMc5uPJynkrHpw7pAiBt6YMhQktn7owF")

# Add the reported users information
add_reported_users_information(artists, reported_users)
add_reported_users_information(collectors, reported_users)
add_reported_users_information(patrons, reported_users)
add_reported_users_information(users, reported_users)
add_reported_users_information(objktcom_collectors, reported_users)

# Group the users by the day of their first interaction
artists_per_day = group_users_per_day(artists)
collectors_per_day = group_users_per_day(collectors)
patrons_per_day = group_users_per_day(patrons)
users_per_day = group_users_per_day(users)

# Get a dictionary with the OBJKT creators
objkt_creators = get_objkt_creators(mint_transactions)

# Print some information about the total number of users
print("There are currently %i unique users in hic et nunc." % len(users))
print("Of those %i are artists and %i are patrons." % (len(artists), len(patrons)))
print("%i artists are also collectors." % (len(collectors) - len(patrons)))

# Get the collectors that collected multiple editions in one call
multiple_editions_collectors = {}

for transaction in collect_transactions: 
    if "objkt_amount" in transaction["parameter"]["value"]:
        wallet_id = transaction["sender"]["address"]
        editions = int(transaction["parameter"]["value"]["objkt_amount"])

        if editions > 50:
            if wallet_id not in multiple_editions_collectors:
                multiple_editions_collectors[wallet_id] = {
                        "alias" : transaction[
                            "sender"]["alias"] if "alias" in transaction["sender"] else "",
                        "editions": editions
                    }
            else:
                multiple_editions_collectors[wallet_id]["editions"] += editions

# Get the total money spent by non-reported collectors
wallet_ids = np.array([wallet_id for wallet_id in collectors])
aliases = np.array([collector["alias"] for collector in collectors.values()])
total_money_spent = np.array(
    [collector["total_money_spent"] for collector in collectors.values()])
is_reported_collector = np.array(
    [collector["reported"] for collector in collectors.values()])
wallet_ids = wallet_ids[~is_reported_collector]
aliases = aliases[~is_reported_collector]
total_money_spent = total_money_spent[~is_reported_collector]

# Add the name registered with the SUBJKT contract if the alias is not present
for i, wallet_id in enumerate(wallet_ids):
    if aliases[i] == "" and wallet_id in registries_bigmap:
        aliases[i] = registries_bigmap[wallet_id]["user"]

# Add the money spent in objkt.com
for i, wallet_id in enumerate(wallet_ids):
    if wallet_id in objktcom_collectors:
        total_money_spent[i] += objktcom_collectors[
            wallet_id]["total_money_spent"]

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
    title="Collectors that spent more than 100tez",
    x_label="Total money spent (tez)", y_label="Number of collectors", bins=100)
save_figure(os.path.join(figures_dir, "top_collectors_histogram.png"))

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
        print(" %3i: Collector %s spent %5.0f tez (%s)" % (
            i + 1, collector["wallet_id"], collector["total_money_spent"], collector["alias"]))
    else:
        print(" %3i: Collector %s spent %5.0f tez" % (
            i + 1, collector["wallet_id"], collector["total_money_spent"]))

# Save the collectors ranking list in a json file
save_json_file("collectors_ranking.json", collectors_ranking)

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

# Plot a histogram of the collected editions prices
adapted_collect_money = collect_money.copy()
adapted_collect_money[adapted_collect_money > 1000] = 999

plot_histogram(
    adapted_collect_money,
    title="Collected editions price distribution",
    x_label="Edition price (tez)", y_label="Number of collected editions",
    bins=[0, 0.1, 0.5, 1, 1.5, 3, 5, 8, 15, 30, 50, 100, 200, 400, 800, 1000],
    log=True)
save_figure(os.path.join(figures_dir, "edition_price_histogram.png"))

# Plot the money spent in collect operations per day
plot_data_per_day(
    collect_money, collect_timestamps,
    "Money spent in collect operations per day",
    "Days since first minted OBJKT (1st of March)", "Money spent (tez)",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "money_per_day.png"))

plot_data_per_day(
    collect_money[~collect_is_secondary],
    collect_timestamps[~collect_is_secondary],
    "Money spent in primary market collect operations per day",
    "Days since first minted OBJKT (1st of March)", "Money spent (tez)",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "money_primary_per_day.png"))

plot_data_per_day(
    collect_money[collect_is_secondary],
    collect_timestamps[collect_is_secondary],
    "Money spent in secondary market collect operations per day",
    "Days since first minted OBJKT (1st of March)", "Money spent (tez)",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "money_secondary_per_day.png"))

plot_data_per_day(
    collect_money[~collect_from_patron],
    collect_timestamps[~collect_from_patron],
    "Money spent in collect operations per day by artists",
    "Days since first minted OBJKT (1st of March)", "Money spent (tez)",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "money_per_day_by_artists.png"))

plot_data_per_day(
    collect_money[collect_from_patron], collect_timestamps[collect_from_patron],
    "Money spent in collect operations per day by patrons",
    "Days since first minted OBJKT (1st of March)", "Money spent (tez)",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "money_per_day_by_patrons.png"))

plot_price_distribution_per_day(
    collect_money, collect_timestamps, [0.01, 1, 5, 50],
    "Price distribution of collected OBJKTs per day",
    "Days since first minted OBJKT (1st of March)", "Number of collected OBJKTs",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "price_distribution_per_day.png"))

plot_price_distribution_per_day(
    collect_money[~collect_is_secondary],
    collect_timestamps[~collect_is_secondary], [0.01, 1, 5, 50],
    "Price distribution of collected OBJKTs per day on the primary market",
    "Days since first minted OBJKT (1st of March)", "Number of collected OBJKTs",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "price_distribution_per_day_primary.png"))

plot_price_distribution_per_day(
    collect_money[collect_is_secondary],
    collect_timestamps[collect_is_secondary], [0.01, 1, 5, 50],
    "Price distribution of collected OBJKTs per day on the secondary market",
    "Days since first minted OBJKT (1st of March)", "Number of collected OBJKTs",
    exclude_last_day=exclude_last_day)
save_figure(
    os.path.join(figures_dir, "price_distribution_per_day_secondary.png"))

plot_price_distribution_per_day(
    collect_money[~collect_from_patron],
    collect_timestamps[~collect_from_patron], [0.01, 1, 5, 50],
    "Price distribution of collected OBJKTs per day by artists",
    "Days since first minted OBJKT (1st of March)", "Number of collected OBJKTs",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "price_distribution_per_day_by_artists.png"))

plot_price_distribution_per_day(
    collect_money[collect_from_patron], collect_timestamps[collect_from_patron],
    [0.01, 1, 5, 50],
    "Price distribution of collected OBJKTs per day by patrons",
    "Days since first minted OBJKT (1st of March)", "Number of collected OBJKTs",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "price_distribution_per_day_by_patrons.png"))

# Print some information about the collect operations
print(" Non-reported users performed %i collect operations." % len(collect_money))

for i in [0.0, 0.1, 0.5, 1, 2, 3, 5, 10, 100]:
    print(" %.1f%% of them were for editions with a value <= %.1ftez." % (
        100 * np.sum(collect_money <= i) / len(collect_money), i))

print(" Non-reported users spent a total of %.0f tez." % np.sum(collect_money))

for i in [0.1, 0.5, 1, 2, 3, 5, 10, 100]:
    print(" %.1f%% of that was on editions with a value <= %.1ftez." % (
        100 * np.sum(collect_money[collect_money <= i]) / np.sum(collect_money), i))

# Plot the new users per day
plot_new_users_per_day(
    artists, title="New artists per day",
    x_label="Days since first minted OBJKT (1st of March)",
    y_label="New artists per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "new_artists_per_day.png"))

plot_new_users_per_day(
    collectors, title="New collectors per day",
    x_label="Days since first minted OBJKT (1st of March)",
    y_label="New collectors per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "new_collectors_per_day.png"))

plot_new_users_per_day(
    patrons, title="New patrons per day",
    x_label="Days since first minted OBJKT (1st of March)",
    y_label="New patrons per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "new_patros_per_day.png"))

plot_new_users_per_day(
    users, title="New users per day",
    x_label="Days since first minted OBJKT (1st of March)",
    y_label="New users per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "new_users_per_day.png"))

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

# Plot the active users per day
plot_active_users_per_day(
    transactions_wallet_ids, transactions_timestamps,
    "Active users per day",
    "Days since first minted OBJKT (1st of March)", "Active users per day",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "active_users_per_day.png"))

# Plot the users last active day
plot_users_last_active_day(
    transactions_wallet_ids, transactions_timestamps,
    "Users last active day",
    "Days since first minted OBJKT (1st of March)", "Users",
    exclude_last_day=False)
save_figure(os.path.join(figures_dir, "users_last_active_day.png"))
