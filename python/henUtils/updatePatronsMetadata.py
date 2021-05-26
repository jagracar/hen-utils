import time
from henUtils.queryUtils import *

# Read the previously saved patrons metadata
saved_patrons = read_json_file("../data/patrons.json")

# Set the path to the directory where the transaction information will be saved
# to avoid to query for it again and again
transactions_dir = "../data/transactions"

# Get the complete list of mint and collect transactions
mint_transactions = get_all_mint_transactions(transactions_dir, sleep_time=10)
collect_transactions = get_all_collect_transactions(transactions_dir, sleep_time=10)

# Extract the artists, collectors and patrons accounts
artists = extract_artist_accounts(mint_transactions)
collectors = extract_collector_accounts(collect_transactions)
patrons = get_patron_accounts(artists, collectors)
print_info("Found %i patrons (collectors that are not artists at the same "
           "time)." % len(patrons))

# Use the saved metadata information for the old patrons and select the new ones
new_patrons = {}

for wallet_id in patrons:
    if wallet_id in saved_patrons:
        patrons[wallet_id] = saved_patrons[wallet_id]
    else:
        new_patrons[wallet_id] = patrons[wallet_id]

print_info("Found %i new patrons." % len(new_patrons))

# Get the list of H=N reported users
reported_users = get_reported_users()

# Add the reported users information
add_reported_users_information(artists, reported_users)
add_reported_users_information(patrons, reported_users)

# Get the account metadata for all the new patrons
print_info("Adding new patrons metadata...")
batch_size = 50
from_index = 0
to_index = min(from_index + batch_size, len(new_patrons))
counter = 1

while True:
    print_info("Processing batch %i: patrons %i to %i" % (
        counter, from_index, to_index))
    add_accounts_metadata(new_patrons, from_index, to_index, sleep_time=1)
    add_first_collected_objkt_id(new_patrons, from_index, to_index, sleep_time=1)

    if to_index == len(new_patrons):
        break

    from_index = to_index
    to_index = min(from_index + batch_size, len(new_patrons))
    counter += 1
    time.sleep(60)

# Save the patrons information into a json file, but clean first the money_spent
# array
for patron in patrons.values():
    patron["money_spent"] = []

save_json_file("patrons.json", patrons)

# Save the patrons aliases into a json file
patrons_aliases = {}

for walletId, patron in patrons.items():
    patrons_aliases[walletId] = patron["alias"] if "alias" in patron else ""

save_json_file("patrons_aliases.json", patrons_aliases)

# Save the patrons twitter accounts into a json file
patrons_twitter_accounts = {}

for walletId, patron in patrons.items():
    patrons_twitter_accounts[walletId] = patron["twitter"] if "twitter" in patron else ""

save_json_file("patrons_twitter_accounts.json", patrons_twitter_accounts)
