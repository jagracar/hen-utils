import numpy as np
from henUtils.queryUtils import *
from henUtils.plotUtils import *

# Set the path to the directory where the transaction information will be saved
# to avoid to query for it again and again
transactions_dir = "../data/transactions"

# Set the path to the directory where the figures will be saved
figures_dir = "../figures"

# Get the complete list of mint and collect transactions
mint_transactions = get_all_mint_transactions(transactions_dir, sleep_time=1)
collect_transactions = get_all_collect_transactions(transactions_dir, sleep_time=1)

# Extract the artists, collector and patron accounts
artists = extract_artist_accounts(mint_transactions)
collectors = extract_collector_accounts(collect_transactions)
patrons = get_patron_accounts(artists, collectors)
users = get_user_accounts(artists, patrons)

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

# Print some information about the total number of users
print("There are currently %i unique users in hic et nunc." % len(users))
print("Of those %i are artists and %i are patrons." % (len(artists), len(patrons)))
print("%i artists are also collectors." % (len(collectors) - len(patrons)))

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

# Print the list of the top 10 collectors
print("This is the list of the top 10 collectors:")

for collector in collectors_ranking[:10]:
    if collector["alias"] != "":
        print("  Collector %s spent %5.0f tez (%s)" % (
            collector["wallet_id"], collector["total_money_spent"], collector["alias"]))
    else:
        print("  Collector %s spent %5.0f tez" % (
            collector["wallet_id"], collector["total_money_spent"]))

# Save the collectors ranking list in a json file
save_json_file("collectors_ranking.json", collectors_ranking)

# Get the collected money that doesn't come from a reported user
collect_from_patron = []
collect_timestamps = []
collect_money = []

for transaction in collect_transactions:
    wallet_id = transaction["sender"]["address"]

    if wallet_id not in reported_users:
        collect_from_patron.append(wallet_id in patrons)
        collect_timestamps.append(transaction["timestamp"])
        collect_money.append(transaction["amount"] / 1e6)

collect_from_patron = np.array(collect_from_patron)
collect_timestamps = np.array(collect_timestamps)
collect_money = np.array(collect_money)

# Plot the money spent in collect operations per day
plot_collected_money_per_day(
    collect_money, collect_timestamps,
    "Money spent in collect operations per day",
    "Days since first minted OBJKT (1st of March)", "Money spent (tez)",
    exclude_last_day=True)
save_figure(os.path.join(figures_dir, "money_per_day.png"))

plot_collected_money_per_day(
    collect_money[~collect_from_patron],
    collect_timestamps[~collect_from_patron],
    "Money spent in collect operations per day by artists",
    "Days since first minted OBJKT (1st of March)", "Money spent (tez)",
    exclude_last_day=True)
save_figure(os.path.join(figures_dir, "money_per_day_by_artists.png"))

plot_collected_money_per_day(
    collect_money[collect_from_patron], collect_timestamps[collect_from_patron],
    "Money spent in collect operations per day by patrons",
    "Days since first minted OBJKT (1st of March)", "Money spent (tez)",
    exclude_last_day=True)
save_figure(os.path.join(figures_dir, "money_per_day_by_patrons.png"))

plot_price_distribution_per_day(
    collect_money, collect_timestamps, [0.01, 1, 5, 50],
    "Price distribution of collected OBJKTs per day",
    "Days since first minted OBJKT (1st of March)", "Number of collected OBJKTs",
    exclude_last_day=True)
save_figure(os.path.join(figures_dir, "price_distribution_per_day.png"))

plot_price_distribution_per_day(
    collect_money[~collect_from_patron],
    collect_timestamps[~collect_from_patron], [0.01, 1, 5, 50],
    "Price distribution of collected OBJKTs per day by artists",
    "Days since first minted OBJKT (1st of March)", "Number of collected OBJKTs",
    exclude_last_day=True)
save_figure(os.path.join(figures_dir, "price_distribution_per_day_by_artists.png"))

plot_price_distribution_per_day(
    collect_money[collect_from_patron], collect_timestamps[collect_from_patron],
    [0.01, 1, 5, 50],
    "Price distribution of collected OBJKTs per day by patrons",
    "Days since first minted OBJKT (1st of March)", "Number of collected OBJKTs",
    exclude_last_day=True)
save_figure(os.path.join(figures_dir, "price_distribution_per_day_by_patrons.png"))

# Print some information about the collect operations
print("Non-reported users performed %i collect operations." % len(collect_money))

for i in [0.0, 0.1, 0.5, 1, 2, 3, 5, 10, 100]:
    print("%.1f%% of them were for editions with a value <= %.1ftez." % (
        100 * np.sum(collect_money <= i) / len(collect_money), i))

print("Non-reported users spent a total of %.0f tez." % np.sum(collect_money))

for i in [0.1, 0.5, 1, 2, 3, 5, 10, 100]:
    print("%.1f%% of that was on editions with a value <= %.1ftez." % (
        100 * np.sum(collect_money[collect_money <= i]) / np.sum(collect_money), i))

# Plot the new users per day
plot_new_users_per_day(
    artists, title="New artists per day",
    x_label="Days since first minted OBJKT (1st of March)",
    y_label="New artists per day", exclude_last_day=True)
save_figure(os.path.join(figures_dir, "new_artists_per_day.png"))

plot_new_users_per_day(
    collectors, title="New collectors per day",
    x_label="Days since first minted OBJKT (1st of March)",
    y_label="New collectors per day", exclude_last_day=True)
save_figure(os.path.join(figures_dir, "new_collectors_per_day.png"))

plot_new_users_per_day(
    patrons, title="New patrons per day",
    x_label="Days since first minted OBJKT (1st of March)",
    y_label="New patrons per day", exclude_last_day=True)
save_figure(os.path.join(figures_dir, "new_patros_per_day.png"))

plot_new_users_per_day(
    users, title="New users per day",
    x_label="Days since first minted OBJKT (1st of March)",
    y_label="New users per day", exclude_last_day=True)
save_figure(os.path.join(figures_dir, "new_users_per_day.png"))
