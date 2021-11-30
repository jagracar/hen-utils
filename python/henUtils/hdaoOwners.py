import numpy as np
from henUtils.queryUtils import *
from henUtils.plotUtils import *

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

# Get the complete list of HEN mint, collect, swap, cancel swap and burn
# transactions
mint_transactions = get_all_transactions("mint", transactions_dir, sleep_time=1)
collect_transactions = get_all_transactions(
    "collect", transactions_dir, sleep_time=1)
swap_transactions = get_all_transactions("swap", transactions_dir, sleep_time=1)
cancel_swap_transactions = get_all_transactions(
    "cancel_swap", transactions_dir, sleep_time=1)
burn_transactions = get_all_transactions("burn", transactions_dir, sleep_time=1)

# Get the H=N bigmaps
swaps_bigmap = get_hen_bigmap("swaps", transactions_dir, sleep_time=1)
royalties_bigmap = get_hen_bigmap("royalties", transactions_dir, sleep_time=1)
registries_bigmap = get_hen_bigmap("registries", transactions_dir, sleep_time=1)
subjkts_metadata_bigmap = get_hen_bigmap(
    "subjkts metadata", transactions_dir, sleep_time=1)

# Extract the H=N artists, collector and patron accounts
artists = extract_artist_accounts(
    mint_transactions, registries_bigmap, tezos_wallets)
collectors = extract_collector_accounts(
    collect_transactions, registries_bigmap, swaps_bigmap, tezos_wallets)
swappers = extract_swapper_accounts(
    swap_transactions, registries_bigmap, tezos_wallets)
patrons = get_patron_accounts(artists, collectors)
users = get_user_accounts(artists, patrons, swappers)

# Get the list of H=N reported users
reported_users = get_reported_users()

# Get the hDAO token ledger bigmap
ledger_bigmap = get_token_bigmap("ledger","hDAO", transactions_dir, sleep_time=1)

# Get the hDAO owners
hdao_owners = {}

for entry in ledger_bigmap.values():
    wallet = entry["key"]["address"]
    hdaos = int(entry["value"]) / 1e6

    if hdaos != 0:
        alias = ""

        if wallet in registries_bigmap:
            alias = registries_bigmap[wallet]["user"]
        elif wallet in tezos_wallets and "alias" in tezos_wallets[wallet]:
            alias = tezos_wallets[wallet]["alias"]

        hdao_owners[wallet] = {
            "alias" : alias,
            "hDAOs" : hdaos,
            "henUser": wallet in users,
            "reported": wallet in reported_users,
            "contract": wallet.startswith("KT")}

wallets = np.array(list(hdao_owners.keys()))
aliases = np.array([hdao_owners[wallet]["alias"] for wallet in wallets])
hdaos = np.array([hdao_owners[wallet]["hDAOs"] for wallet in wallets])
is_hen_user = np.array([hdao_owners[wallet]["henUser"] for wallet in wallets])
is_reported = np.array([hdao_owners[wallet]["reported"] for wallet in wallets])
is_contract = np.array([hdao_owners[wallet]["contract"] for wallet in wallets])

# Order the owners by the amount of hDAOs they own
sorted_indices = np.argsort(hdaos)[::-1]
wallets = wallets[sorted_indices]
aliases = aliases[sorted_indices]
hdaos = hdaos[sorted_indices]
is_hen_user = is_hen_user[sorted_indices]
is_reported = is_reported[sorted_indices]
is_contract = is_contract[sorted_indices]

# Calculate the voting power with a quadratic formula
voting_power = np.sqrt(hdaos)

# Print some information about the owners
print("Total amount of hDAOs in circulation: %i hDAOs" % round(np.sum(hdaos)))
print("Number of wallets in posession of hDAOs: %s wallets" % len(wallets))
print("Of those, %s wallets are in the H=N block list and %s wallets are "
      "directly associated to H=N users." % (len(wallets[is_reported]), len(wallets[is_hen_user])))
print("H=N users own %i hDAOs." % round(np.sum(hdaos[is_hen_user])))
print("Reported users own %i hDAOs." % round(np.sum(hdaos[is_reported])))
print("%i hDAOs are in KT contract addresses." % round(np.sum(hdaos[is_contract])))

# Print the list of the top 100 hDAO owners
print("\n This is the list of the top 100 hDAO owners:\n")

for i in range(100):
    if aliases[i] != "":
        print(" %3i: %s has %5i hDAOs (%s)" % (
            i + 1, wallets[i], round(hdaos[i]), aliases[i]))
    else:
        print(" %3i: %s has %5i hDAOS" % (
            i + 1, wallets[i], round(hdaos[i])))

# Save the data in a csv file
with open("hdaoOwners.csv", "w") as file:
    file.write("alias, wallet, is_hen_user, is_reported, hDAOs \n")
    for i in range(len(wallets)):
        text = "%s, %s, %s, %s, %s" % (
            aliases[i].replace(",","_").replace(";","_"),
            wallets[i],
            is_hen_user[i],
            is_reported[i],
            hdaos[i])
        file.write(text + "\n")
