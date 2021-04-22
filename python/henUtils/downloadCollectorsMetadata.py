from datetime import datetime
from henUtils.queryUtils import *

# Get a time stamp for the file names
time_stamp = datetime.now().strftime("%Y-%m-%d")

# Get the complete list of mint and collect transactions
mint_transactions = get_all_mint_transactions()
collect_transactions = get_all_collect_transactions()

# Save the collect transactions into a json file
collect_transactions_file_name = "collectTransactions_%s.json" % time_stamp
save_json_file(collect_transactions_file_name, collect_transactions)

# Extract the artists accounts from the mint transactions
artists = extract_artist_accounts(mint_transactions)

# Extract the collector accounts from the collect transactions
collectors = extract_collector_accounts(collect_transactions)

# Add the copyminter information
copyminters = get_copyminters()
add_copyminter_information(artists, copyminters)
add_copyminter_information(collectors, copyminters)

# Select only those collectors that are not artists at the same time
only_collectors = {}

for wallet_id, collector in collectors.items():
    if wallet_id not in artists:
        only_collectors[wallet_id] = collector

print_info("Found %i collectors that are not artists at the same time." % len(only_collectors))

# Get the account metadata and the first collected object id for the collectors
print_info("Adding collectors metadata...")
batch_size = 100
from_index = 0
to_index = min(from_index + batch_size, len(only_collectors))
counter = 1

while True:
    print_info("Processing batch %i: collectors %i to %i" % (counter, from_index, to_index))
    add_accounts_metadata(only_collectors, from_index, to_index)
    add_first_collect_objkt_id(only_collectors, from_index, to_index)

    if to_index == len(only_collectors):
        break

    from_index = to_index
    to_index = min(from_index + batch_size, len(only_collectors))
    counter += 1
    time.sleep(120)

# Save the collectors information into a json file
only_collectors_file_name = "collectors_%s.json" % time_stamp
save_json_file(only_collectors_file_name, only_collectors)
