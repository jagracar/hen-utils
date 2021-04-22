import json
import time
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


def get_query_result(query):
    """Executes the given query and returns the result.

    Parameters
    ----------
    query: str
        The complete query.

    Returns
    -------
    object
        The query result.

    """
    with urlopen(query) as request:
        if request.status == 200:
            return json.loads(request.read().decode())

    return None


def get_copyminters():
    """Returns the list of copyminters stored in the hic et nunc github
    repository.

    Returns
    -------
    list
        A python list with the wallet ids of all known copyminters.

    """
    github_repository = "hicetnunc2000/hicetnunc"
    file_path = "filters/w.json"
    query = "https://raw.githubusercontent.com//%s/main/%s" % (
        github_repository, file_path)

    return get_query_result(query)


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


def get_all_mint_transactions(transactions_per_batch=1000, sleep_time=1):
    """Returns the complete list of applied hic et nunc mint transactions
    ordered by increasing time stamp.

    Parameters
    ----------
    transactions_per_batch: int, optional
        The maximum number of transactions per API query. Default is 1000. The
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
        print_info("Downloading batch %i" % counter)
        new_transactions = get_mint_transactions(len(transactions), transactions_per_batch)
        transactions += new_transactions

        if len(new_transactions) != transactions_per_batch:
            break

        counter += 1
        time.sleep(sleep_time)

    print_info("Downloaded %i mint transactions." % len(transactions))

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

        if wallet_id not in artists:
            artists[wallet_id] = {
                "wallet_id": wallet_id,
                "first_objkt": {
                    "id" : transaction["parameter"]["value"]["token_id"],
                    "amount": transaction["parameter"]["value"]["amount"],
                    "timestamp": transaction["timestamp"]},
                "order": counter,
                "copyminter": False}
            counter += 1

    print_info("Found %i unique artists." % len(artists))

    return artists


def add_copyminter_information(accounts, copyminters):
    """Adds the copyminter information to a set of accounts.

    Parameters
    ----------
    accounts: dict
        The python dictionary with the accounts information.
    copyminters: list
        The python list with the wallet ids of all known copyminters.

    """
    for wallet_id in copyminters:
        if wallet_id in accounts:
            accounts[wallet_id]["copyminter"] = True


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
        metadata = get_account_metadata(wallet_id)

        if metadata is not None:
            for keyword, value in metadata.items():
                account[keyword] = value

        if i != 0 and (i + 1) % 10 == 0:
            print_info("Downloaded the profile metadata for %i accounts" % (i + 1))

        time.sleep(0.5)
