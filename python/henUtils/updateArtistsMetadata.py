from datetime import datetime
from henUtils.queryUtils import *

# Get a time stamp for the file names
time_stamp = datetime.now().strftime("%Y-%m-%d")

# Read the previously saved artist metadata
saved_artists = read_json_file("../data/artists_2021-04-22.json")

# Get the complete list of mint transactions and save them into a json file
transactions = get_all_mint_transactions()
transactions_file_name = "mintTransactions_%s.json" % time_stamp
save_json_file(transactions_file_name, transactions)

# Extract the artists accounts from the mint transactions
artists = extract_artist_accounts(transactions)

# Select the new artists
new_artists = {}

for wallet_id, artist in artists.items():
    if wallet_id not in saved_artists:
        new_artists[wallet_id] = artist

# Get the account metadata for all the new artists
print_info("Adding new artists metadata...")
batch_size = 250
from_index = 0
to_index = min(from_index + batch_size, len(new_artists))
counter = 1

while True:
    print_info("Processing batch %i: artists %i to %i" % (counter, from_index, to_index))
    add_accounts_metadata(new_artists, from_index, to_index)

    if to_index == len(new_artists):
        break

    from_index = to_index
    to_index = min(from_index + batch_size, len(new_artists))
    counter += 1
    time.sleep(120)

# Add the new artists to the saved artists
artists = saved_artists

for wallet_id, artist in new_artists.items():
    artists[wallet_id] = artist

# Add the last copyminter information
copyminters = get_copyminters()
add_copyminter_information(artists, copyminters)

# Save the artists information into a json file
artists_file_name = "artists_%s.json" % time_stamp
save_json_file(artists_file_name, artists)

# Save the artists aliases into a json file
artists_aliases = {walletId: artist["alias"] if "alias" in artist else ""  for walletId, artist in artists.items()}
artists_aliases_file_name = "artists_aliases_%s.json" % time_stamp
save_json_file(artists_aliases_file_name, artists_aliases)
