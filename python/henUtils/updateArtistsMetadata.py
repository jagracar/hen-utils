import time
from henUtils.queryUtils import *

# Read the previously saved artist metadata
saved_artists = read_json_file("../data/artists.json")

# Set the path to the directory where the transaction information will be saved
# to avoid to query for it again and again
transactions_dir = "../data/transactions"

# Get the complete list of mint transactions
mint_transactions = get_all_transactions("mint", transactions_dir, sleep_time=10)

# Extract the artists accounts
artists = extract_artist_accounts(mint_transactions)
print_info("Found %i artists." % len(artists))

# Use the saved metadata information for the old artists and select the new ones
new_artists = {}

for wallet_id in artists:
    if wallet_id in saved_artists:
        artists[wallet_id] = saved_artists[wallet_id]
    else:
        new_artists[wallet_id] = artists[wallet_id]

print_info("Found %i new artists." % len(new_artists))

# Get the list of H=N reported users
reported_users = get_reported_users()

# Add the reported users information
add_reported_users_information(artists, reported_users)

# Get the account metadata for all the new artists
print_info("Adding new artists metadata...")
batch_size = 50
from_index = 0
to_index = min(from_index + batch_size, len(new_artists))
counter = 1

while True:
    print_info("Processing batch %i: artists %i to %i" % (
        counter, from_index, to_index))
    add_accounts_metadata(new_artists, from_index, to_index, sleep_time=1)

    if to_index == len(new_artists):
        break

    from_index = to_index
    to_index = min(from_index + batch_size, len(new_artists))
    counter += 1
    time.sleep(60)

# Save the artists information into a json file
save_json_file("artists.json", artists)

# Save the artists aliases into a json file
artists_aliases = {}

for walletId, artist in artists.items():
    artists_aliases[walletId] = artist["alias"] if "alias" in artist else ""

save_json_file("artists_aliases.json", artists_aliases)

# Save the artists twitter accounts into a json file
artists_twitter_accounts = {}

for walletId, artist in artists.items():
    artists_twitter_accounts[walletId] = artist["twitter"] if "twitter" in artist else ""

save_json_file("artists_twitter_accounts.json", artists_twitter_accounts)
