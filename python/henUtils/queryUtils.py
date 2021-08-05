import json
import time
import os.path
import numpy as np
from datetime import datetime
from calendar import monthrange
from urllib.request import urlopen


def print_info(info):
    """Prints some information with a time stamp added.

    Parameters
    ----------
    info: str
        The information to print.

    """
    print("%s  %s" % (datetime.now(), info))


def read_json_file(file_name):
    """Reads a json file from disk.

    Parameters
    ----------
    file_name: str
        The complete path to the json file.

    Returns
    -------
    object
        The content of the json file.

    """
    with open(file_name, "r", encoding="utf-8") as json_file:
        return json.load(json_file)


def save_json_file(file_name, data):
    """Saves some data as a json file.

    Parameters
    ----------
    file_name: str
        The complete path to the json file where the data will be saved.
    data: object
        The data to save.

    """
    with open(file_name, "w", encoding="utf-8") as json_file:
        json.dump(data, json_file, indent=4)


def get_query_result(query, timeout=10):
    """Executes the given query and returns the result.

    Parameters
    ----------
    query: str
        The complete query.
    timeout: float, optional
        The query timeout in seconds. Default is 10 seconds.

    Returns
    -------
    object
        The query result.

    """
    with urlopen(query, timeout=timeout) as request:
        if request.status == 200:
            return json.loads(request.read().decode())

    return None


def get_reported_users():
    """Returns the list of reported users stored in the hic et nunc github
    repository.

    Returns
    -------
    list
        A python list with the wallet ids of all the reported users.

    """
    github_repository = "hicetnunc2000/hicetnunc"
    file_path = "filters/w.json"
    query = "https://raw.githubusercontent.com//%s/main/%s" % (
        github_repository, file_path)

    return list(set(get_query_result(query)))


def get_objkt_metadata(objkt_id):
    """Returns the metadata information for a given OBJKT.

    Parameters
    ----------
    objkt_id: int
        The OBJKT id number.

    Returns
    -------
    dict
        A python dictionary with the OBJKT metadata. None if the OBJKT was not
        found or it didn't have any metadata information.

    """
    query = "https://api.better-call.dev/v1"
    query += "/tokens/mainnet/metadata?"
    query += "contract=KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton"
    query += "&token_id=%i" % objkt_id
    result = get_query_result(query)

    return None if result is None or len(result) == 0 else result[0]


def get_account_metadata(wallet_id):
    """Returns the account metadata information for a wallet id.

    Parameters
    ----------
    wallet_id: int
        The account wallet id.

    Returns
    -------
    dict
        A python dictionary with the account metadata. None if the account was
        not found or it didn't have any metadata information.

    """
    query = "https://api.tzkt.io/v1/"
    query += "accounts/%s/metadata" % wallet_id

    return get_query_result(query)


def get_operation_group(operation_hash):
    """Returns the operation group information from a given operation hash.

    Parameters
    ----------
    operation_hash: str
        The operation hash.

    Returns
    -------
    list
        A python list with the operation group information. None if the
        operation group was not found.

    """
    query = "https://api.tzkt.io/v1/"
    query += "operations/%s" % operation_hash

    return get_query_result(query)


def get_object_id(operation_hash):
    """Returns the OBJKT id from a collect operation.

    Parameters
    ----------
    operation_hash: str
        The operation hash.

    Returns
    -------
    str
        The OBJKT id.

    """
    operation_group = get_operation_group(operation_hash)

    for operation in operation_group:
        if operation["type"] == "transaction" and "storage" in operation:
            return operation["storage"]["objkt_id"]

    return ""


def get_transactions(entrypoint, contract, offset=0, limit=100, timestamp=None,
                     parameter_query=None):
    """Returns a list of applied hic et nunc transactions ordered by
    increasing time stamp.

    Parameters
    ----------
    entrypoint: str
        The transaction entrypoint: mint, collect, swap or cancel_swap.
    contract: str
        The contract address.
    offset: int, optional
        The number of initial transactions that should be skipped. This is
        mostly used for pagination. Default is 0.
    limit: int, optional
        The maximum number of transactions to return. Default is 100. The
        maximum allowed by the API is 10000.
    timestamp: str, optional
        The maximum transaction time stamp. Only earlier transactions will be
        returned. It should follow the ISO format (e.g. 2021-04-20T00:00:00Z).
        Default is no limit.
    parameter_query: str, optional
        The parameter query. Default is no query.

    Returns
    -------
    list
        A python list with the transactions information.

    """
    query = "https://api.tzkt.io/v1/"
    query += "operations/transactions?"
    query += "target=%s" % contract
    query += "&status=applied"
    query += "&entrypoint=%s" % entrypoint
    query += "&offset=%i" % offset
    query += "&limit=%i" % limit
    query += "&timestamp.le=%s" % timestamp if timestamp is not None else ""
    query += "&%s" % parameter_query if parameter_query is not None else ""

    return get_query_result(query)


def get_all_transactions(type, data_dir, transactions_per_batch=10000, sleep_time=1):
    """Returns the complete list of applied hic et nunc transactions of a given
    type ordered by increasing time stamp.

    Parameters
    ----------
    type: str
        The transaction type: mint, collect, swap or cancel_swap.
    data_dir: str
        The complete path to the directory where the transactions information
        should be saved.
    transactions_per_batch: int, optional
        The maximum number of transactions per API query. Default is 10000. The
        maximum allowed by the API is 10000.
    sleep_time: float, optional
        The sleep time between API queries in seconds. This is used to avoid
        being blocked by the server. Default is 1 second.

    Returns
    -------
    list
        A python list with the transactions information.

    """
    # Set the contract addresses
    if type in ["mint", "burn"]:
        contracts = ["KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton"]
    else:
        contracts = ["KT1Hkg5qeNhfwpKW4fXvq7HGZB9z2EnmCCA9",
                     "KT1HbQepzV1nVGg8QVznG7z4RcHseD5kwqBn"]

    # Set the entry point
    entrypoint = "transfer" if type == "burn" else type

    # Set the parameter query
    parameter_query = None

    if type == "burn":
        parameter_query = "parameter.[0].txs.[0].to_=tz1burnburnburnburnburnburnburjAYjjX"

    # Download the transactions
    print_info("Downloading %s transactions..." % type)
    transactions = []
    counter = 1
    total_counter = 1

    for contract in contracts:
        while True:
            file_name = os.path.join(
                data_dir, "%s_transactions_%s_%i-%i.json" % (
                    type, contract, (counter - 1) * transactions_per_batch,
                    counter * transactions_per_batch))

            if os.path.exists(file_name):
                print_info(
                    "Batch %i has been already downloaded. Reading it from "
                    "local json file." % total_counter)
                transactions += read_json_file(file_name)
            else:
                print_info("Downloading batch %i" % total_counter)
                new_transactions = get_transactions(
                    entrypoint, contract,
                    (counter - 1) * transactions_per_batch,
                    transactions_per_batch, parameter_query=parameter_query)
                transactions += new_transactions

                if len(new_transactions) != transactions_per_batch:
                    counter = 1
                    total_counter += 1
                    break

                print_info(
                    "Saving batch %i in the output directory" % total_counter)
                save_json_file(file_name, new_transactions)

                time.sleep(sleep_time)

            counter += 1
            total_counter += 1

    print_info("Downloaded %i %s transactions." % (len(transactions), type))

    return transactions


def get_swaps_bigmap_keys(data_dir, keys_per_batch=10000, sleep_time=1):
    """Returns the complete swaps bigmap key list.

    Parameters
    ----------
    data_dir: str
        The complete path to the directory where the swaps bigmap keys 
        information should be saved.
    keys_per_batch: int, optional
        The maximum number of swap bigmap keys per API query. Default is 10000.
        The maximum allowed by the API is 10000.
    sleep_time: float, optional
        The sleep time between API queries in seconds. This is used to avoid
        being blocked by the server. Default is 1 second.

    Returns
    -------
    list
        A python list with the swaps bigmap keys.

    """
    # Set the contract addresses
    contracts = ["KT1Hkg5qeNhfwpKW4fXvq7HGZB9z2EnmCCA9",
                 "KT1HbQepzV1nVGg8QVznG7z4RcHseD5kwqBn"]

    # Download the swaps bigmap keys
    print_info("Downloading swaps bigmap keys...")
    swaps_bigmap_keys = []
    counter = 1
    total_counter = 1

    for contract in contracts:
        while True:
            file_name = os.path.join(
                data_dir, "swaps_bigmap_keys_%s_%i-%i.json" % (
                    contract, (counter - 1) * keys_per_batch,
                    counter * keys_per_batch))

            if os.path.exists(file_name):
                print_info(
                    "Batch %i has been already downloaded. Reading it from "
                    "local json file." % total_counter)
                swaps_bigmap_keys += read_json_file(file_name)
            else:
                print_info("Downloading batch %i" % total_counter)
                query = "https://api.tzkt.io/v1/bigmaps/updates?"
                query += "contract=%s" % contract
                query += "&path=swaps"
                query += "&action=add_key"
                query += "&offset=%i" % ((counter - 1) * keys_per_batch)
                query += "&limit=%i" % keys_per_batch
                new_swaps_bigmap_keys = get_query_result(query)
                swaps_bigmap_keys += new_swaps_bigmap_keys

                if len(new_swaps_bigmap_keys) != keys_per_batch:
                    counter = 1
                    total_counter += 1
                    break

                print_info(
                    "Saving batch %i in the output directory" % total_counter)
                save_json_file(file_name, new_swaps_bigmap_keys)

                time.sleep(sleep_time)

            counter += 1
            total_counter += 1

    print_info("Downloaded %i swap bigmap keys." % len(swaps_bigmap_keys))

    return swaps_bigmap_keys


def build_swaps_bigmap(swaps_bigmap_keys):
    """Builds the swaps bigmap from the swaps bigmap keys.

    Parameters
    ----------
    swaps_bigmap_keys: list
        A python list with the swaps bigmap keys.

    Returns
    -------
    dict
        A python dictionary with the swaps bigmap.

    """
    swaps_bigmap = {}

    for bigmap_key in swaps_bigmap_keys:
        content = bigmap_key["content"]
        swaps_bigmap[content["key"]] = content["value"]

    return swaps_bigmap


def extract_artist_accounts(transactions):
    """Extracts the artists accounts information from a list of mint
    transactions.

    Parameters
    ----------
    transactions: list
        The list of mint transactions.

    Returns
    -------
    dict
        A python dictionary with the unique artists accounts.

    """
    artists = {}
    counter = 1

    for transaction in transactions:
        wallet_id = transaction["initiator"]["address"]

        if wallet_id.startswith("tz") and wallet_id not in artists:
            artists[wallet_id] = {
                "order": counter,
                "type": "artist",
                "wallet_id": wallet_id,
                    "alias": transaction["initiator"][
                        "alias"] if "alias" in transaction["initiator"] else "",
                "first_objkt": {
                    "id" : transaction["parameter"]["value"]["token_id"],
                    "amount": transaction["parameter"]["value"]["amount"],
                    "timestamp": transaction["timestamp"]},
                "first_interaction": {
                    "type": "mint",
                    "timestamp": transaction["timestamp"]},
                "reported": False,
                "money_spent": [],
                "total_money_spent": 0}

            counter += 1

    return artists


def extract_collector_accounts(transactions):
    """Extracts the collector accounts information from a list of collect
    transactions.

    Parameters
    ----------
    transactions: list
        The list of collect transactions.

    Returns
    -------
    dict
        A python dictionary with the unique collector accounts.

    """
    collectors = {}
    counter = 1

    for transaction in transactions:
        wallet_id = transaction["sender"]["address"]

        if wallet_id.startswith("tz"):
            if wallet_id not in collectors:
                collectors[wallet_id] = {
                    "order": counter,
                    "type": "collector",
                    "wallet_id": wallet_id,
                    "alias": transaction["sender"][
                        "alias"] if "alias" in transaction["sender"] else "",
                    "first_collect": {
                        "objkt_id": "",
                        "operation_hash": transaction["hash"],
                        "timestamp": transaction["timestamp"]},
                    "first_interaction": {
                        "type": "collect",
                        "timestamp": transaction["timestamp"]},
                    "reported": False,
                    "money_spent": [transaction["amount"] / 1e6]}
                counter += 1
            else:
                collectors[wallet_id]["money_spent"].append(
                    transaction["amount"] / 1e6)

    for collector in collectors.values():
        collector["total_money_spent"] = sum(collector["money_spent"])

    return collectors


def get_patron_accounts(artists, collectors):
    """Gets the patron accounts from a set of artists and collectors.

    Parameters
    ----------
    artists: dict
        The python dictionary with the artists accounts.
    collectors: dict
        The python dictionary with the collectors accounts.

    Returns
    -------
    dict
        A python dictionary with the patron accounts.

    """
    patrons = {}

    for wallet_id, collector in collectors.items():
        # Check if the collector is also an artist
        if wallet_id in artists:
            # Set the collector type to artist
            collector["type"] = "artist"

            # Save the first collect information and the money spent
            artist = artists[wallet_id]
            artist["first_collect"] = collector["first_collect"]
            artist["money_spent"] = collector["money_spent"]
            artist["total_money_spent"] = collector["total_money_spent"]

            # Check which was the first artist interation
            datetime_format = "%Y-%m-%dT%H:%M:%SZ"
            first_objkt_date = datetime.strptime(
                artist["first_objkt"]["timestamp"], datetime_format)
            first_collect_date = datetime.strptime(
                artist["first_collect"]["timestamp"], datetime_format)

            if first_objkt_date > first_collect_date:
                artist["first_interaction"]["type"] = "collect"
                artist["first_interaction"]["timestamp"] = artist[
                    "first_collect"]["timestamp"]
        else:
            # Set the collector type to patron
            collector["type"] = "patron"

            # Add the collector to the patrons dictionary
            patrons[wallet_id] = collector

    return patrons


def get_user_accounts(artists, patrons):
    """Gets the user accounts from a set of artists and patrons.

    Parameters
    ----------
    artists: dict
        The python dictionary with the artists accounts.
    patrons: dict
        The python dictionary with the patrons accounts.

    Returns
    -------
    dict
        A python dictionary with the user accounts.

    """
    # Get the combined wallet ids and time stamps
    wallet_ids = np.array(
        [artists[i]["wallet_id"] for i in artists] + 
        [patrons[i]["wallet_id"] for i in patrons])
    timestamps = np.array(
        [artists[i]["first_interaction"]["timestamp"] for i in artists] + 
        [patrons[i]["first_interaction"]["timestamp"] for i in patrons])

    # Order the users by their time stamps
    datetime_format = "%Y-%m-%dT%H:%M:%SZ"
    dates = np.array(
        [datetime.strptime(timestamp, datetime_format) for timestamp in timestamps])
    wallet_ids = wallet_ids[np.argsort(dates)]
    users = {}

    for wallet_id in wallet_ids:
        if wallet_id in artists:
            users[wallet_id] = artists[wallet_id]
        else:
            users[wallet_id] = patrons[wallet_id]

    return users


def get_objkt_creators(transactions):
    """Gets a dictionary with the OBJKT creators from a list of mint
    transactions.

    Parameters
    ----------
    transactions: list
        The list of mint transactions.

    Returns
    -------
    dict
        A python dictionary with the OBJKT creators.

    """
    objkt_creators = {}

    for transaction in transactions:
        objkt_id = transaction["parameter"]["value"]["token_id"]
        creator = transaction["initiator"]["address"]
        objkt_creators[objkt_id] = creator

    return objkt_creators


def add_reported_users_information(accounts, reported_users):
    """Adds the reported users information to a set of accounts.

    Parameters
    ----------
    accounts: dict
        The python dictionary with the accounts information.
    reported_users: list
        The python list with the wallet ids of all H=N reported users.

    """
    for wallet_id in reported_users:
        if wallet_id in accounts:
            accounts[wallet_id]["reported"] = True


def add_accounts_metadata(accounts, from_account_index=0, to_account_index=None, sleep_time=1):
    """Adds the TzKT profile metadata information to a set of accounts.

    Be careful, this will send a lot of API queries and you might be temporally
    blocked!

    Parameters
    ----------
    accounts: dict
        The python dictionary with the accounts information.
    from_account_index: int, optional
        The index of the first account where the metadata should be added. This
        is used to avoid being blocked by the server or to only update a set of
        new accounts. Default is 0.
    to_account_index: int, optional
        The index of the account starting from which the metadata will not be
        added. This is used to avoid being blocked by the server or to only
        update a set of new accounts. Default is None, which indicates that the
        metadata information will be added for all accounts starting from
        from_account_index.
    sleep_time: float, optional
        The sleep time between API queries in seconds. This is used to avoid
        being blocked by the server. Default is 1 second.

    """
    if to_account_index is None or to_account_index > len(accounts):
        to_account_index = len(accounts)

    wallet_ids = list(accounts.keys())[from_account_index:to_account_index]

    for i, wallet_id in enumerate(wallet_ids):
        account = accounts[wallet_id]

        try:
            metadata = get_account_metadata(wallet_id)
        except:
            print_info("Blocked by the server? Trying again...")
            metadata = get_account_metadata(wallet_id)

        if metadata is not None:
            for keyword, value in metadata.items():
                account[keyword] = value

        if i != 0 and (i + 1) % 10 == 0:
            print_info("Downloaded the profile metadata for %i accounts" % (i + 1))

        time.sleep(sleep_time)


def add_first_collected_objkt_id(accounts, from_account_index=0, to_account_index=None, sleep_time=1):
    """Adds the id of the first collected OBJKT to a set of collector accounts.

    Be careful, this will send a lot of API queries and you might be temporally
    blocked!

    Parameters
    ----------
    accounts: dict
        The python dictionary with the collector accounts information.
    from_account_index: int, optional
        The index of the first account where the metadata should be added. This
        is used to avoid being blocked by the server or to only update a set of
        new accounts. Default is 0.
    to_account_index: int, optional
        The index of the account starting from which the metadata will not be
        added. This is used to avoid being blocked by the server or to only
        update a set of new accounts. Default is None, which indicates that the
        metadata information will be added for all accounts starting from
        from_account_index.
    sleep_time: float, optional
        The sleep time between API queries in seconds. This is used to avoid
        being blocked by the server. Default is 1 second.

    """
    if to_account_index is None or to_account_index > len(accounts):
        to_account_index = len(accounts)

    wallet_ids = list(accounts.keys())[from_account_index:to_account_index]

    for i, wallet_id in enumerate(wallet_ids):
        account = accounts[wallet_id]

        try:
            account["first_collect"]["objkt_id"] = get_object_id(
                account["first_collect"]["operation_hash"])
        except:
            print_info("Blocked by the server? Trying again...")
            account["first_collect"]["objkt_id"] = get_object_id(
                account["first_collect"]["operation_hash"])

        if i != 0 and (i + 1) % 10 == 0:
            print_info("Added the OBJKT id for %i accounts" % (i + 1))

        time.sleep(sleep_time)


def split_timestamps(timestamps):
    """Splits the input time stamps in 3 arrays containing the years, months
    and days.

    Parameters
    ----------
    timestamps: list
        A python list with the time stamps.

    Returns
    -------
    tuple
        A python tuple with the years, months and days numpy arrays.

    """
    datetime_format = "%Y-%m-%dT%H:%M:%SZ"
    dates = [datetime.strptime(
        timestamp, datetime_format) for timestamp in timestamps]
    years = np.array([date.year for date in dates])
    months = np.array([date.month for date in dates])
    days = np.array([date.day for date in dates])

    return years, months, days


def get_counts_per_day(timestamps):
    """Calculates the counts per day for a list of time stamps.

    Parameters
    ----------
    timestamps: list
        A python list with ordered time stamps.

    Returns
    -------
    list
        A python list with the counts per day, starting from 2021-03-01.

    """
    # Extract the years, months and days from the time stamps
    years, months, days = split_timestamps(timestamps)

    # Get the counts per day
    counts_per_day = []
    started = False
    finished = False

    for year in range(2021, np.max(years) + 1):
        for month in range(1, 13):
            for day in range(1, monthrange(year, month)[1] + 1):
                # Check if we passed the starting day: 2021-03-01
                if not started:
                    started = (year == 2021) and (month == 3) and (day == 1)

                # Check that we started and didn't finish yet
                if started and not finished:
                    # Add the number of operations for the current day
                    counts_per_day.append(np.sum(
                        (years == year) & (months == month) & (days == day)))

                    # Check if we reached the last day
                    finished = (year == years[-1]) and (
                        month == months[-1]) and (day == days[-1])

    return counts_per_day


def group_users_per_day(users):
    """Groups the given users per the day of their first interaction.

    Parameters
    ----------
    users: dict
        A python dictionary with the users information.

    Returns
    -------
    list
        A python list with the users grouped by day.

    """
    # Get the users wallet ids and their first interation time stamp
    wallet_ids = np.array(list(users.keys()))
    timestamps = [user["first_interaction"]["timestamp"] for user in users.values()]

    # Extract the years, months and days from the time stamps
    years, months, days = split_timestamps(timestamps)

    # Get the users per day
    users_per_day = []
    started = False
    finished = False

    for year in range(2021, np.max(years) + 1):
        for month in range(1, 13):
            for day in range(1, monthrange(year, month)[1] + 1):
                # Check if we passed the starting day: 2021-03-01
                if not started:
                    started = (year == 2021) and (month == 3) and (day == 1)

                # Check that we started and didn't finish yet
                if started and not finished:
                    selected_wallets_ids = wallet_ids[
                        (years == year) & (months == month) & (days == day)]
                    users_per_day.append(
                        [users[wallet_id] for wallet_id in selected_wallets_ids])

                    # Check if we reached the last day
                    finished = (year == years[-1]) and (
                        month == months[-1]) and (day == days[-1])

    return users_per_day
