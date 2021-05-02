from datetime import datetime
from henUtils.queryUtils import *

# Get a time stamp for the file names
time_stamp = datetime.now().strftime("%Y-%m-%d")

# Get the complete list of mint transactions and save them into a json file
transactions = get_all_mint_transactions()
transactions_file_name = "mintTransactions_%s.json" % time_stamp
save_json_file(transactions_file_name, transactions)

# Extract the artists accounts from the mint transactions
artists = extract_artist_accounts(transactions)

# Add the copyminter information
copyminters = get_copyminters()
add_copyminter_information(artists, copyminters)

# Get the account metadata for all the artists
print_info("Adding artists metadata...")
batch_size = 250
from_index = 0
to_index = min(from_index + batch_size, len(artists))
counter = 1

while True:
    print_info("Processing batch %i: artists %i to %i" % (counter, from_index, to_index))
    add_accounts_metadata(artists, from_index, to_index, sleep_time=2)

    if to_index == len(artists):
        break

    from_index = to_index
    to_index = min(from_index + batch_size, len(artists))
    counter += 1
    time.sleep(120)

# Save the artists information into a json file
artists_file_name = "artists_%s.json" % time_stamp
save_json_file(artists_file_name, artists)

# Save the artists aliases into a json file
artists_aliases = {walletId: artist["alias"] if "alias" in artist else ""  for walletId, artist in artists.items()}
artists_aliases_file_name = "artists_aliases_%s.json" % time_stamp
save_json_file(artists_aliases_file_name, artists_aliases)
