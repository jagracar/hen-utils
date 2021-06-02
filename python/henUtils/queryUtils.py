import json
import time
import os.path
import numpy as np
from datetime import datetime
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


def get_mint_transactions(offset=0, limit=100, timestamp=None):
    """Returns a list of applied hic et nunc mint transactions ordered by
    increasing time stamp.

    Parameters
    ----------
    offset: int, optional
        The number of initial mint transactions that should be skipped. This is
        mostly used for pagination. Default is 0.
    limit: int, optional
        The maximum number of transactions to return. Default is 100. The
        maximum allowed by the API is 10000.
    timestamp: str, optional
        The maximum transaction time stamp. Only earlier transactions will be
        returned. It should follow the ISO format (e.g. 2021-04-20T00:00:00Z).
        Default is no limit.

    Returns
    -------
    list
        A python list with the mint transactions information.

    """
    query = "https://api.tzkt.io/v1/"
    query += "operations/transactions?"
    query += "target=KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton"
    query += "&status=applied"
    query += "&entrypoint=mint"
    query += "&offset=%i" % offset
    query += "&limit=%i" % limit
    query += "&timestamp.le=%s" % timestamp if timestamp is not None else ""

    return get_query_result(query)


def get_collect_transactions(offset=0, limit=100, timestamp=None):
    """Returns a list of applied hic et nunc collect transactions ordered by
    increasing time stamp.

    Parameters
    ----------
    offset: int, optional
        The number of initial collect transactions that should be skipped. This
        is mostly used for pagination. Default is 0.
    limit: int, optional
        The maximum number of transactions to return. Default is 100. The
        maximum allowed by the API is 10000.
    timestamp: str, optional
        The maximum transaction time stamp. Only earlier transactions will be
        returned. It should follow the ISO format (e.g. 2021-04-20T00:00:00Z).
        Default is no limit.

    Returns
    -------
    list
        A python list with the collect transactions information.

    """
    query = "https://api.tzkt.io/v1/"
    query += "operations/transactions?"
    query += "target=KT1Hkg5qeNhfwpKW4fXvq7HGZB9z2EnmCCA9"
    query += "&status=applied"
    query += "&entrypoint=collect"
    query += "&offset=%i" % offset
    query += "&limit=%i" % limit
    query += "&timestamp.le=%s" % timestamp if timestamp is not None else ""

    return get_query_result(query)


def get_all_mint_transactions(data_dir, transactions_per_batch=10000, sleep_time=1):
    """Returns the complete list of applied hic et nunc mint transactions
    ordered by increasing time stamp.

    Parameters
    ----------
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
        A python list with the mint transactions information.

    """
    print_info("Downloading mint transactions...")
    transactions = []
    counter = 1

    while True:
        file_name = os.path.join(
            data_dir, "mint_transactions_%i-%i.json" % (
                len(transactions), len(transactions) + transactions_per_batch))

        if os.path.exists(file_name):
            print_info(
                "Batch %i has been already downloaded. Reading it from local "
                "json file." % counter)
            transactions += read_json_file(file_name)
        else:
            print_info("Downloading batch %i" % counter)
            new_transactions = get_mint_transactions(
                len(transactions), transactions_per_batch)
            transactions += new_transactions

            if len(new_transactions) != transactions_per_batch:
                break

            print_info("Saving batch %i in the output directory" % counter)
            save_json_file(file_name, new_transactions)

            time.sleep(sleep_time)

        counter += 1

    print_info("Downloaded %i mint transactions." % len(transactions))

    return transactions


def get_all_collect_transactions(data_dir, transactions_per_batch=10000, sleep_time=1):
    """Returns the complete list of applied hic et nunc collect transactions
    ordered by increasing time stamp.

    Parameters
    ----------
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
        A python list with the collect transactions information.

    """
    print_info("Downloading collect transactions...")
    transactions = []
    counter = 1

    while True:
        file_name = os.path.join(
            data_dir, "collect_transactions_%i-%i.json" % (
                len(transactions), len(transactions) + transactions_per_batch))

        if os.path.exists(file_name):
            print_info(
                "Batch %i has been already downloaded. Reading it from local "
                "json file." % counter)
            transactions += read_json_file(file_name)
        else:
            print_info("Downloading batch %i" % counter)
            new_transactions = get_collect_transactions(
                len(transactions), transactions_per_batch)
            transactions += new_transactions

            if len(new_transactions) != transactions_per_batch:
                break

            print_info("Saving batch %i in the output directory" % counter)
            save_json_file(file_name, new_transactions)

            time.sleep(sleep_time)

        counter += 1

    print_info("Downloaded %i collect transactions." % len(transactions))

    return transactions


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
