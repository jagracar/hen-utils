import json
import time
import os.path
import numpy as np
from datetime import datetime
from datetime import timezone
from calendar import monthrange
from urllib.request import urlopen


def print_info(info):
    """Prints some information with a time stamp added.

    Parameters
    ----------
    info: str
        The information to print.

    """
    print("%s  %s" % (datetime.utcnow(), info))


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


def save_json_file(file_name, data, compact=False):
    """Saves some data as a json file.

    Parameters
    ----------
    file_name: str
        The complete path to the json file where the data will be saved.
    data: object
        The data to save.
    compact: bool, optinal
        If True, the json file will be save in a compact form. Default is False.

    """
    with open(file_name, "w", encoding="utf-8") as json_file:
        if compact:
            json.dump(data, json_file, indent=None, separators=(",", ":"))
        else:
            json.dump(data, json_file, indent=4)


def get_datetime_from_timestamp(timestamp):
    """Returns a datetime instance from a time stamp.

    Parameters
    ----------
    timestamp: str
        The time stamp.

    Returns
    -------
    object
        A datetime instance.

    """
    return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")


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


def get_transactions(entrypoint, contract, offset=0, limit=100, timestamp=None,
                     parameter_query=None):
    """Returns a list of applied transactions ordered by increasing time stamp.

    Parameters
    ----------
    entrypoint: str
        The contract entrypoint (e.g. mint, collect, swap, cancel_swap).
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


def extract_relevant_transaction_information(transactions):
    """Extracts the most relevant information from a list of transactions.

    This is mostly done to save memory.

    Parameters
    ----------
    transactions: list
        The list of transactions.

    Returns
    -------
    list
        A python list with the most relevant transactions information.

    """
    relevant_transaction_information = []
    relevant_keywords = [
        "timestamp", "initiator", "sender", "target", "amount", "parameter"]

    for transaction in transactions:
        relevant_transaction_information.append({
            keyword: transaction[keyword] for keyword in relevant_keywords if 
            keyword in transaction})

    return relevant_transaction_information


def get_all_transactions(type, data_dir, transactions_per_batch=10000,
                         sleep_time=1):
    """Returns the complete list of applied transactions of a given type
    ordered by increasing time stamp.

    Parameters
    ----------
    type: str
        The transaction type (e.g. mint, collect, swap, cancel_swap, bid, ask).
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
    # Set the contract addresses, the entrypoint and the parameter query
    entrypoint = type
    parameter_query = None

    if type in "mint":
        contracts = ["KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton"]
    elif type in "burn":
        contracts = ["KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton"]
        entrypoint = "transfer"
        parameter_query = "parameter.[0].txs.[0].to_=tz1burnburnburnburnburnburnburjAYjjX"
    elif type in ["collect", "swap", "cancel_swap"]:
        contracts = ["KT1Hkg5qeNhfwpKW4fXvq7HGZB9z2EnmCCA9",
                     "KT1HbQepzV1nVGg8QVznG7z4RcHseD5kwqBn"]
    elif type in ["bid", "ask"]:
        contracts = ["KT1FvqJwEDWb1Gwc55Jd1jjTHRVWbYKUUpyq"]
        entrypoint = "fulfill_" + type
    elif type == "english_auction":
        contracts = ["KT1XjcRq5MLAzMKQ3UHsrue2SeU2NbxUrzmU"]
        entrypoint = "conclude_auction"
    elif type == "dutch_auction":
        contracts = ["KT1QJ71jypKGgyTNtXjkCAYJZNhCKWiHuT2r"]
        entrypoint = "buy"
    elif type == "fxhash_mint_issuer":
        contracts = ["KT1AEVuykWeuuFX7QkEAMNtffzwhe1Z98hJS"]
        entrypoint = "mint_issuer"
    elif type == "fxhash_mint":
        contracts = ["KT1AEVuykWeuuFX7QkEAMNtffzwhe1Z98hJS"]
        entrypoint = "mint"
    elif type == "fxhash_collect":
        contracts = ["KT1Xo5B7PNBAeynZPmca4bRh6LQow4og1Zb9"]
        entrypoint = "collect"
    elif type == "fxhash_offer":
        contracts = ["KT1Xo5B7PNBAeynZPmca4bRh6LQow4og1Zb9"]
        entrypoint = "offer"
    elif type == "fxhash_cancel_offer":
        contracts = ["KT1Xo5B7PNBAeynZPmca4bRh6LQow4og1Zb9"]
        entrypoint = "cancel_offer"
    elif type == "fxhash_update_issuer":
        contracts = ["KT1AEVuykWeuuFX7QkEAMNtffzwhe1Z98hJS"]
        entrypoint = "update_issuer"

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
                transactions += extract_relevant_transaction_information(
                    read_json_file(file_name))
            else:
                print_info("Downloading batch %i" % total_counter)
                new_transactions = get_transactions(
                    entrypoint, contract,
                    (counter - 1) * transactions_per_batch,
                    transactions_per_batch, parameter_query=parameter_query)
                transactions += extract_relevant_transaction_information(
                    new_transactions)

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


def get_bigmap_keys(bigmap_ids, data_dir, keys_per_batch=10000, sleep_time=1):
    """Returns the complete bigmap key list.

    Parameters
    ----------
    bigmap_ids: list
        A list with the bigmap ids to query.
    data_dir: str
        The complete path to the directory where the bigmap keys information
        should be saved.
    keys_per_batch: int, optional
        The maximum number of bigmap keys per API query. Default is 10000.
        The maximum allowed by the API is 10000.
    sleep_time: float, optional
        The sleep time between API queries in seconds. This is used to avoid
        being blocked by the server. Default is 1 second.

    Returns
    -------
    list
        A python list with the bigmap keys.

    """
    # Download the bigmap keys
    print_info("Downloading bigmap keys...")
    bigmap_keys = []
    counter = 1
    total_counter = 1

    for bigmap_id in bigmap_ids:
        while True:
            file_name = os.path.join(
                data_dir, "bigmap_keys_%s_%i-%i.json" % (
                    bigmap_id, (counter - 1) * keys_per_batch,
                    counter * keys_per_batch))

            if os.path.exists(file_name):
                print_info(
                    "Batch %i has been already downloaded. Reading it from "
                    "local json file." % total_counter)
                bigmap_keys += read_json_file(file_name)
            else:
                print_info("Downloading batch %i" % total_counter)
                query = "https://api.tzkt.io/v1/bigmaps/%s/keys?" % bigmap_id
                query += "&offset=%i" % ((counter - 1) * keys_per_batch)
                query += "&limit=%i" % keys_per_batch
                new_bigmap_keys = get_query_result(query)
                bigmap_keys += new_bigmap_keys

                if len(new_bigmap_keys) != keys_per_batch:
                    counter = 1
                    total_counter += 1
                    break

                print_info(
                    "Saving batch %i in the output directory" % total_counter)
                save_json_file(file_name, new_bigmap_keys)

                time.sleep(sleep_time)

            counter += 1
            total_counter += 1

    print_info("Downloaded %i bigmap keys." % len(bigmap_keys))

    return bigmap_keys


def get_hen_bigmap(name, data_dir, keys_per_batch=10000, sleep_time=1):
    """Returns one of the HEN bigmaps.

    Parameters
    ----------
    name: str
        The bigmap name: swaps, royalties, registries, subjkts metadata.
    data_dir: str
        The complete path to the directory where the HEN bigmap keys information
        should be saved.
    keys_per_batch: int, optional
        The maximum number of bigmap keys per API query. Default is 10000. The
        maximum allowed by the API is 10000.
    sleep_time: float, optional
        The sleep time between API queries in seconds. This is used to avoid
        being blocked by the server. Default is 1 second.

    Returns
    -------
    dict
        A python dictionary with the HEN bigmap.

    """
    # Set the HEN bigmap ids
    if name == "swaps":
        bigmap_ids = [
            "523",  # KT1Hkg5qeNhfwpKW4fXvq7HGZB9z2EnmCCA9
            "6072"  # KT1HbQepzV1nVGg8QVznG7z4RcHseD5kwqBn
        ]
    if name == "royalties":
        bigmap_ids = [
            "522"   # KT1Hkg5qeNhfwpKW4fXvq7HGZB9z2EnmCCA9
        ]
    elif name == "registries":
        bigmap_ids = [
            "3919"  # KT1My1wDZHDGweCrJnQJi3wcFaS67iksirvj
        ]
    elif name == "subjkts metadata":
        bigmap_ids = [
            "3921"  # KT1My1wDZHDGweCrJnQJi3wcFaS67iksirvj
        ]

    # Get the HEN bigmap keys
    bigmap_keys = get_bigmap_keys(
        bigmap_ids, data_dir, keys_per_batch, sleep_time)

    # Build the bigmap
    bigmap = {}

    for bigmap_key in bigmap_keys:
        if name == "swaps" or name == "royalties":
            key = bigmap_key["key"]
            bigmap[key] = bigmap_key["value"]
        elif name == "registries":
            key = bigmap_key["key"]
            value = bytes.fromhex(bigmap_key["value"]).decode(
                "utf-8", errors="replace")
            bigmap[key] = {"user": value}
        elif name == "subjkts metadata":
            key = bytes.fromhex(bigmap_key["key"]).decode(
                "utf-8", errors="replace")
            value = bytes.fromhex(bigmap_key["value"]).decode(
                "utf-8", errors="replace")
            bigmap[key] = {"user_metadata": value}

        bigmap[key]["active"] = bigmap_key["active"]

    return bigmap


def get_objktcom_bigmap(name, token, data_dir, keys_per_batch=10000,
                        sleep_time=1, token_addresses=None):
    """Returns one of the objkt.com bigmaps.

    Parameters
    ----------
    name: str
        The bigmap name: bids, asks, english auctions or ductch auctions,
        minter.
    token: str
        The token name: OBJKT, tezzardz, prjktneon, artcardz, gogo, neonz,
        skele, collections, all.
    data_dir: str
        The complete path to the directory where the objkt.com bigmap keys
        information should be saved.
    keys_per_batch: int, optional
        The maximum number of bigmap keys per API query. Default is 10000. The
        maximum allowed by the API is 10000.
    sleep_time: float, optional
        The sleep time between API queries in seconds. This is used to avoid
        being blocked by the server. Default is 1 second.

    Returns
    -------
    dict
        A python dictionary with the objkt.com bigmap.

    """
    # Set the objkt.bid bigmap ids
    if name == "bids":
        bigmap_ids = [
            "5910"  # KT1FvqJwEDWb1Gwc55Jd1jjTHRVWbYKUUpyq
        ]
    elif name == "asks":
        bigmap_ids = [
            "5909"  # KT1FvqJwEDWb1Gwc55Jd1jjTHRVWbYKUUpyq
        ]
    elif name == "english auctions":
        bigmap_ids = [
            "6210"  # KT1XjcRq5MLAzMKQ3UHsrue2SeU2NbxUrzmU
        ]
    elif name == "dutch auctions":
        bigmap_ids = [
            "6212"  # KT1QJ71jypKGgyTNtXjkCAYJZNhCKWiHuT2r
        ]
    elif name == "minter":
        bigmap_ids = [
            "24157"  # KT1Aq4wWmVanpQhq4TTfjZXB5AjFpx15iQMM
        ]

    # Set the token contract
    if token == "OBJKT":
        token_contracts = ["KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton"]
    elif token == "tezzardz":
        token_contracts = ["KT1LHHLso8zQWQWg1HUukajdxxbkGfNoHjh6"]
    elif token == "prjktneon":
        token_contracts = [
            "KT1VbHpQmtkA3D4uEbbju26zS8C42M5AGNjZ",
            "KT1H8sxNSgnkCeZsij4z76pkXu8BCZNvPZEx"]
    elif token == "artcardz":
        token_contracts = ["KT1LbLNTTPoLgpumACCBFJzBEHDiEUqNxz5C"]
    elif token == "gogo":
        token_contracts = ["KT1SyPgtiXTaEfBuMZKviWGNHqVrBBEjvtfQ"]
    elif token == "neonz":
        token_contracts = ["KT1MsdyBSAMQwzvDH4jt2mxUKJvBSWZuPoRJ"]
    elif token == "skele":
        token_contracts = ["KT1HZVd9Cjc2CMe3sQvXgbxhpJkdena21pih"]
    elif token == "collections":
        token_contracts = token_addresses

    # Get the objkt.com bigmap keys
    bigmap_keys = get_bigmap_keys(
        bigmap_ids, data_dir, keys_per_batch, sleep_time)

    # Build the bigmap
    bigmap = {}

    for bigmap_key in bigmap_keys:
        if name == "minter" or token == "all":
            bigmap[bigmap_key["key"]] = bigmap_key["value"]
            bigmap[bigmap_key["key"]]["active"] = bigmap_key["active"]
        elif bigmap_key["value"]["fa2"] in token_contracts:
            bigmap[bigmap_key["key"]] = bigmap_key["value"]
            bigmap[bigmap_key["key"]]["active"] = bigmap_key["active"]

    return bigmap


def get_fxhash_bigmap(name, data_dir, keys_per_batch=10000, sleep_time=1):
    """Returns one of the fxhash bigmaps.

    Parameters
    ----------
    name: str
        The bigmap name: offers.
    data_dir: str
        The complete path to the directory where the fxhash bigmap keys
        information should be saved.
    keys_per_batch: int, optional
        The maximum number of bigmap keys per API query. Default is 10000. The
        maximum allowed by the API is 10000.
    sleep_time: float, optional
        The sleep time between API queries in seconds. This is used to avoid
        being blocked by the server. Default is 1 second.

    Returns
    -------
    dict
        A python dictionary with the fxhash bigmap.

    """
    # Set the fxhash bigmap ids
    if name == "offers":
        bigmap_ids = [
            "22799"  # KT1Xo5B7PNBAeynZPmca4bRh6LQow4og1Zb9
        ]

    # Get the fxhash bigmap keys
    bigmap_keys = get_bigmap_keys(
        bigmap_ids, data_dir, keys_per_batch, sleep_time)

    # Build the bigmap
    bigmap = {}

    for bigmap_key in bigmap_keys:
        if name == "offers":
            key = bigmap_key["key"]
            bigmap[key] = bigmap_key["value"]

        bigmap[key]["active"] = bigmap_key["active"]

    return bigmap


def get_token_bigmap(name, token, data_dir, keys_per_batch=10000, sleep_time=1):
    """Returns one of the token bigmaps.

    Parameters
    ----------
    name: str
        The bigmap name: ledger, token_metadata, operators.
    token: str
        The token name: OBJKT, tezzardz, prjktneon, artcardz, gogo, neonz,
        skele, GENTK.
    data_dir: str
        The complete path to the directory where the token bigmap keys
        information should be saved.
    keys_per_batch: int, optional
        The maximum number of bigmap keys per API query. Default is 10000. The
        maximum allowed by the API is 10000.
    sleep_time: float, optional
        The sleep time between API queries in seconds. This is used to avoid
        being blocked by the server. Default is 1 second.

    Returns
    -------
    dict
        A python dictionary with the token bigmap.

    """
    # Set the token bigmap ids
    if name == "ledger":
        if token == "OBJKT":
            bigmap_ids = ["511"]   # KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton
        elif token == "tezzardz":
            bigmap_ids = ["12112"] # KT1LHHLso8zQWQWg1HUukajdxxbkGfNoHjh6
        elif token == "prjktneon":
            bigmap_ids = ["16117"] # KT1VbHpQmtkA3D4uEbbju26zS8C42M5AGNjZ
        elif token == "artcardz":
            bigmap_ids = ["19083"] # KT1LbLNTTPoLgpumACCBFJzBEHDiEUqNxz5C
        elif token == "gogo":
            bigmap_ids = ["20608"] # KT1SyPgtiXTaEfBuMZKviWGNHqVrBBEjvtfQ
        elif token == "neonz":
            bigmap_ids = ["21217"] # KT1MsdyBSAMQwzvDH4jt2mxUKJvBSWZuPoRJ
        elif token == "skele":
            bigmap_ids = ["22381"] # KT1HZVd9Cjc2CMe3sQvXgbxhpJkdena21pih
        elif token == "GENTK":
            bigmap_ids = ["22785"] # KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE
    elif name == "token_metadata":
        if token == "OBJKT":
            bigmap_ids = ["514"]   # KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton
        elif token == "tezzardz":
            bigmap_ids = ["12115"] # KT1LHHLso8zQWQWg1HUukajdxxbkGfNoHjh6
        elif token == "prjktneon":
            bigmap_ids = ["16120"] # KT1VbHpQmtkA3D4uEbbju26zS8C42M5AGNjZ
        elif token == "artcardz":
            bigmap_ids = ["19086"] # KT1LbLNTTPoLgpumACCBFJzBEHDiEUqNxz5C
        elif token == "gogo":
            bigmap_ids = ["20611"] # KT1SyPgtiXTaEfBuMZKviWGNHqVrBBEjvtfQ
        elif token == "neonz":
            bigmap_ids = ["21220"] # KT1MsdyBSAMQwzvDH4jt2mxUKJvBSWZuPoRJ
        elif token == "skele":
            bigmap_ids = ["22384"] # KT1HZVd9Cjc2CMe3sQvXgbxhpJkdena21pih
        elif token == "GENTK":
            bigmap_ids = ["22789"] # KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE
    elif name == "operators":
        if token == "OBJKT":
            bigmap_ids = ["513"]   # KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton
        elif token == "tezzardz":
            bigmap_ids = ["12114"] # KT1LHHLso8zQWQWg1HUukajdxxbkGfNoHjh6
        elif token == "prjktneon":
            bigmap_ids = ["16119"] # KT1VbHpQmtkA3D4uEbbju26zS8C42M5AGNjZ
        elif token == "artcardz":
            bigmap_ids = ["19085"] # KT1LbLNTTPoLgpumACCBFJzBEHDiEUqNxz5C
        elif token == "gogo":
            bigmap_ids = ["20610"] # KT1SyPgtiXTaEfBuMZKviWGNHqVrBBEjvtfQ
        elif token == "neonz":
            bigmap_ids = ["21219"] # KT1MsdyBSAMQwzvDH4jt2mxUKJvBSWZuPoRJ
        elif token == "skele":
            bigmap_ids = ["22383"] # KT1HZVd9Cjc2CMe3sQvXgbxhpJkdena21pih
        elif token == "GENTK":
            bigmap_ids = ["22787"] # KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE
        
    # Get the token bigmap keys
    bigmap_keys = get_bigmap_keys(
        bigmap_ids, data_dir, keys_per_batch, sleep_time)

    # Build the bigmap
    bigmap = {}
    counter = 0

    for bigmap_key in bigmap_keys:
        if isinstance(bigmap_key["key"], dict):
            bigmap[counter] = bigmap_key["key"]
            counter += 1
        else:
            key = bigmap_key["key"]
            bigmap[key] = bigmap_key["value"]

    return bigmap


def extract_relevant_wallet_information(wallets):
    """Extracts the most relevant information from a list of tezos wallets.

    This is mostly done to save memory.

    Parameters
    ----------
    wallets: list
        The list of wallets.

    Returns
    -------
    list
        A python list with the most relevant wallets information.

    """
    relevant_wallet_information = []
    relevant_keywords = ["type", "address", "alias", "firstActivityTime",
                         "lastActivityTime"]

    for wallet in wallets:
        relevant_wallet_information.append({
            keyword: wallet[keyword] for keyword in relevant_keywords if 
            keyword in wallet})

    return relevant_wallet_information


def get_tezos_wallets(data_dir, wallets_per_batch=10000, sleep_time=1):
    """Returns the complete list of tezos wallets ordered by increasing first
    activity.

    Parameters
    ----------
    data_dir: str
        The complete path to the directory where the wallets information should
        be saved.
    wallets_per_batch: int, optional
        The maximum number of wallets per API query. Default is 10000. The
        maximum allowed by the API is 10000.
    sleep_time: float, optional
        The sleep time between API queries in seconds. This is used to avoid
        being blocked by the server. Default is 1 second.

    Returns
    -------
    dict
        A python dictionary with the wallets information.

    """
    # Download the wallets
    print_info("Downloading the complete list of tezos wallets...")
    wallets = []
    counter = 1

    while True:
        file_name = os.path.join(
            data_dir, "wallets_%i-%i.json" % (
                (counter - 1) * wallets_per_batch, counter * wallets_per_batch))

        if os.path.exists(file_name):
            print_info(
                "Batch %i has been already downloaded. Reading it from "
                "local json file." % counter)
            wallets += extract_relevant_wallet_information(
                read_json_file(file_name))
        else:
            print_info("Downloading batch %i" % counter)
            query = "https://api.tzkt.io/v1/accounts?&sort=firstActivity"
            query += "&offset=%i" % ((counter - 1) * wallets_per_batch)
            query += "&limit=%i" % wallets_per_batch
            new_wallets = get_query_result(query)
            wallets += extract_relevant_wallet_information(new_wallets)

            if len(new_wallets) != wallets_per_batch:
                break

            print_info(
                "Saving batch %i in the output directory" % counter)
            save_json_file(file_name, new_wallets)

            time.sleep(sleep_time)

        counter += 1

    print_info("Downloaded %i wallets." % len(wallets))

    return {wallet["address"]: wallet for wallet in wallets}


def get_tez_exchange_rates(coin, start_date="2018-10-16T00:00:00Z",
                           end_date=None, sampling="1d"):
    """Returns the tez exchange rates for a given time range and sampling
    interval.

    Parameters
    ----------
    coin: str
        The coin to consider: USD or EUR.
    start_date: str, optional
        The start date.  It should follow the ISO format (e.g.
        2021-04-20T00:00:00Z). Default is the first day with exchange rate data
        (2018-10-16T00:00:00Z).
    end_date: str, optional
        The end date. It should follow the ISO format (e.g.
        2021-04-20T00:00:00Z). Default is today.
    sampling: str, optional
        The sampling interval: 1m, 5m, 15m, 30m, 1h, 2h, 3h, 4h, 6h, 12h, 1d,
        1w, 1M, 3M or 1y. Default is 1d.

    Returns
    -------
    tuple
        A python tuple with the lists of time stamps and exchange rates.

    """
    # Get the exchange rate information
    query = "https://api.tzstats.com/series/kraken/XTZ_%s/ohlcv?" % coin
    query += "start_date=%s" % start_date
    query += "" if end_date is None else "&end_date=%s" % end_date
    query += "&collapse=%s" % sampling
    query += "&limit=500000"
    query += "&columns=time,open,close"
    exchange_rate_information = get_query_result(query)

    # Get the time stamps
    datetime_format = "%Y-%m-%dT%H:%M:%SZ"
    timestamps = [row[0] / 1000 for row in exchange_rate_information]
    timestamps = [
        datetime.fromtimestamp(
            timestamp, tz=timezone.utc).strftime(datetime_format) for 
        timestamp in timestamps]

    # Calculate the average exchange rates
    exchange_rates = [
        (row[1] + row[2]) / 2 for row in exchange_rate_information]

    return timestamps, exchange_rates


def extract_artist_accounts(transactions, registries_bigmap, tezos_wallets):
    """Extracts the artists accounts information from a list of mint
    transactions.

    Parameters
    ----------
    transactions: list
        The list of mint transactions.
    registries_bigmap: dict
        The H=N registries bigmap.
    tezos_wallets: dict
        The complete list of tezos wallets.

    Returns
    -------
    dict
        A python dictionary with the unique artists accounts.

    """
    artists = {}
    counter = 1

    for transaction in transactions:
        wallet_id = transaction["initiator"]["address"]
        objkt_id = transaction["parameter"]["value"]["token_id"]

        if wallet_id.startswith("tz"):
            if wallet_id not in artists:
                # Get the artist alias
                if wallet_id in registries_bigmap:
                    alias = registries_bigmap[wallet_id]["user"]
                elif "alias" in tezos_wallets[wallet_id]:
                    alias = tezos_wallets[wallet_id]["alias"]
                else:
                    alias = ""

                # Add the artist information
                artists[wallet_id] = {
                    "order": counter,
                    "type": "artist",
                    "wallet_id": wallet_id,
                    "alias": alias,
                    "reported": False,
                    "first_objkt": {
                        "id": objkt_id,
                        "timestamp": transaction["timestamp"]},
                    "last_objkt": {
                        "id": objkt_id,
                        "timestamp": transaction["timestamp"]},
                    "first_interaction": {
                        "type": "mint",
                        "timestamp": transaction["timestamp"]},
                    "minted_objkts": [objkt_id],
                    "money_spent": [],
                    "total_money_spent": 0}

                counter += 1
            else:
                # Update the last minted OBJKT information
                artists[wallet_id]["last_objkt"]["id"] = objkt_id
                artists[wallet_id]["last_objkt"]["timestamp"] = transaction["timestamp"]

                # Add the OBJKT id to the minted OBJKTs list
                artists[wallet_id]["minted_objkts"].append(objkt_id)

    return artists


def extract_collector_accounts(transactions, registries_bigmap, swaps_bigmap,
                               tezos_wallets):
    """Extracts the collector accounts information from a list of collect
    transactions.

    Parameters
    ----------
    transactions: list
        The list of collect transactions.
    registries_bigmap: dict
        The H=N registries bigmap.
    swaps_bigmap: dict
        The H=N marketplace swaps bigmap.
    tezos_wallets: dict
        The complete list of tezos wallets.

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
                # Get the collector alias
                if wallet_id in registries_bigmap:
                    alias = registries_bigmap[wallet_id]["user"]
                elif "alias" in tezos_wallets[wallet_id]:
                    alias = tezos_wallets[wallet_id]["alias"]
                else:
                    alias = ""

                # Get the swap id from the collect entrypoint input parameters
                parameters = transaction["parameter"]["value"]

                if isinstance(parameters, dict):
                    swap_id = parameters["swap_id"]
                else:
                    swap_id = parameters

                # Add the collector information
                collectors[wallet_id] = {
                    "order": counter,
                    "type": "collector",
                    "wallet_id": wallet_id,
                    "alias": alias,
                    "reported": False,
                    "first_collect": {
                        "objkt_id": swaps_bigmap[swap_id]["objkt_id"],
                        "timestamp": transaction["timestamp"]},
                    "last_collect": {
                        "objkt_id": swaps_bigmap[swap_id]["objkt_id"],
                        "timestamp": transaction["timestamp"]},
                    "first_interaction": {
                        "type": "collect",
                        "timestamp": transaction["timestamp"]},
                    "money_spent": [transaction["amount"] / 1e6]}
                counter += 1
            else:
                # Update the last collect information
                collectors[wallet_id]["last_collect"]["id"] = swaps_bigmap[swap_id]["objkt_id"]
                collectors[wallet_id]["last_collect"]["timestamp"] = transaction["timestamp"]

                # Add the money spent
                collectors[wallet_id]["money_spent"].append(
                    transaction["amount"] / 1e6)

    for collector in collectors.values():
        collector["total_money_spent"] = sum(collector["money_spent"])

    return collectors


def extract_swapper_accounts(transactions, registries_bigmap, tezos_wallets):
    """Extracts the swapper accounts information from a list of swap
    transactions.

    Parameters
    ----------
    transactions: list
        The list of swap transactions.
    registries_bigmap: dict
        The H=N registries bigmap.
    tezos_wallets: dict
        The complete list of tezos wallets.

    Returns
    -------
    dict
        A python dictionary with the unique swapper accounts.

    """
    swappers = {}
    counter = 1

    for transaction in transactions:
        wallet_id = transaction["sender"]["address"]

        if wallet_id.startswith("tz"):
            if wallet_id not in swappers:
                # Get the swapper alias
                if wallet_id in registries_bigmap:
                    alias = registries_bigmap[wallet_id]["user"]
                elif "alias" in tezos_wallets[wallet_id]:
                    alias = tezos_wallets[wallet_id]["alias"]
                else:
                    alias = ""

                # Add the swapper information
                swappers[wallet_id] = {
                    "order": counter,
                    "type": "swapper",
                    "wallet_id": wallet_id,
                    "alias": alias,
                    "reported": False,
                    "first_interaction": {
                        "type": "swap",
                        "timestamp": transaction["timestamp"]},
                    "money_spent": [],
                    "total_money_spent": 0}
                counter += 1

    return swappers


def extract_objktcom_collector_accounts(bid_transactions, ask_transactions,
                                        english_auction_transactions,
                                        dutch_auction_transactions,
                                        bids_bigmap, asks_bigmap,
                                        english_auctions_bigmap,
                                        dutch_auctions_bigmap,
                                        registries_bigmap, tezos_wallets):
    """Extracts the objkt.com collector accounts information from several lists
    of transactions.

    Parameters
    ----------
    bid_transactions: list
        The list of objkt.com bid transactions.
    ask_transactions: list
        The list of objkt.com ask transactions.
    english_auction_transactions: list
        The list of objkt.com english auction transactions.
    dutch_auction_transactions: list
        The list of objkt.com dutch auction transactions.
    bids_bigmap: dict
        The objkt.com bid bigmap.
    asks_bigmap: dict
        The objkt.com ask bigmap.
    english_auctions_bigmap: dict
        The objkt.com english auctions bigmap.
    dutch_auctions_bigmap: dict
        The objkt.com dutch auctions bigmap.
    registries_bigmap: dict
        The H=N registries bigmap.
    tezos_wallets: dict
        The complete list of tezos wallets.

    Returns
    -------
    dict
        A python dictionary with the objkt.com unique collector accounts.

    """
    collectors = {}

    for transaction in bid_transactions:
        bid = bids_bigmap[transaction["parameter"]["value"]]
        collector_wallet_id = bid["issuer"]
        objkt_id = bid["objkt_id"]
        amount = int(bid["xtz_per_objkt"]) / 1e6

        if collector_wallet_id.startswith("tz"):
            if collector_wallet_id not in collectors:
                collectors[collector_wallet_id] = {
                    "type": "collector",
                    "wallet_id": collector_wallet_id,
                    "alias": "",
                    "reported": False,
                    "first_collect": {
                        "id": objkt_id,
                        "timestamp": transaction["timestamp"]},
                    "last_collect": {
                        "id": objkt_id,
                        "timestamp": transaction["timestamp"]},
                    "first_interaction": {
                        "type": "bid",
                        "timestamp": transaction["timestamp"]},
                    "bid_objkts": [],
                    "bid_money_spent": [],
                    "bid_timestamps": [],
                    "ask_objkts": [],
                    "ask_money_spent": [],
                    "ask_timestamps": [],
                    "english_auction_objkts": [],
                    "english_auction_money_spent": [],
                    "english_auction_timestamps": [],
                    "dutch_auction_objkts": [],
                    "dutch_auction_money_spent": [],
                    "dutch_auction_timestamps": []}

            collector = collectors[collector_wallet_id]
            collector["last_collect"]["id"] = objkt_id
            collector["last_collect"]["timestamp"] = transaction["timestamp"]
            collector["bid_objkts"].append(objkt_id)
            collector["bid_money_spent"].append(amount)
            collector["bid_timestamps"].append(transaction["timestamp"])

    for transaction in ask_transactions:
        ask = asks_bigmap[transaction["parameter"]["value"]]
        collector_wallet_id = transaction["sender"]["address"]
        objkt_id = ask["objkt_id"]
        amount = transaction["amount"] / 1e6

        if collector_wallet_id.startswith("tz"):
            if collector_wallet_id not in collectors:
                collectors[collector_wallet_id] = {
                    "type": "collector",
                    "wallet_id": collector_wallet_id,
                    "alias": "",
                    "reported": False,
                    "first_collect": {
                        "id": objkt_id,
                        "timestamp": transaction["timestamp"]},
                    "last_collect": {
                        "id": objkt_id,
                        "timestamp": transaction["timestamp"]},
                    "first_interaction": {
                        "type": "ask",
                        "timestamp": transaction["timestamp"]},
                    "bid_objkts": [],
                    "bid_money_spent": [],
                    "bid_timestamps": [],
                    "ask_objkts": [],
                    "ask_money_spent": [],
                    "ask_timestamps": [],
                    "english_auction_objkts": [],
                    "english_auction_money_spent": [],
                    "english_auction_timestamps": [],
                    "dutch_auction_objkts": [],
                    "dutch_auction_money_spent": [],
                    "dutch_auction_timestamps": []}

            collector = collectors[collector_wallet_id]
            first_collect_date = get_datetime_from_timestamp(
                collector["first_collect"]["timestamp"])
            last_collect_date = get_datetime_from_timestamp(
                collector["last_collect"]["timestamp"])
            date = get_datetime_from_timestamp(transaction["timestamp"])

            if date < first_collect_date:
                collector["first_collect"]["id"] = objkt_id
                collector["first_collect"]["timestamp"] = transaction["timestamp"]
                collector["first_interaction"]["type"] = "ask"
                collector["first_interaction"]["timestamp"] = transaction["timestamp"]

            if date > last_collect_date:
                collector["last_collect"]["id"] = objkt_id
                collector["last_collect"]["timestamp"] = transaction["timestamp"]

            collector["ask_objkts"].append(objkt_id)
            collector["ask_money_spent"].append(amount)
            collector["ask_timestamps"].append(transaction["timestamp"])

    for transaction in english_auction_transactions:
        auction = english_auctions_bigmap[transaction["parameter"]["value"]]
        collector_wallet_id = auction["highest_bidder"]
        amount = int(auction["current_price"]) / 1e6
        objkt_id = auction["objkt_id"]

        if amount == 0:
            continue

        if collector_wallet_id.startswith("tz"):
            if collector_wallet_id not in collectors:
                collectors[collector_wallet_id] = {
                    "type": "collector",
                    "wallet_id": collector_wallet_id,
                    "alias": "",
                    "reported": False,
                    "first_collect": {
                        "id": objkt_id,
                        "timestamp": transaction["timestamp"]},
                    "last_collect": {
                        "id": objkt_id,
                        "timestamp": transaction["timestamp"]},
                    "first_interaction": {
                        "type": "english_auction",
                        "timestamp": transaction["timestamp"]},
                    "bid_objkts": [],
                    "bid_money_spent": [],
                    "bid_timestamps": [],
                    "ask_objkts": [],
                    "ask_money_spent": [],
                    "ask_timestamps": [],
                    "english_auction_objkts": [],
                    "english_auction_money_spent": [],
                    "english_auction_timestamps": [],
                    "dutch_auction_objkts": [],
                    "dutch_auction_money_spent": [],
                    "dutch_auction_timestamps": []}

            collector = collectors[collector_wallet_id]
            first_collect_date = get_datetime_from_timestamp(
                collector["first_collect"]["timestamp"])
            last_collect_date = get_datetime_from_timestamp(
                collector["last_collect"]["timestamp"])
            date = get_datetime_from_timestamp(transaction["timestamp"])

            if date < first_collect_date:
                collector["first_collect"]["id"] = objkt_id
                collector["first_collect"]["timestamp"] = transaction["timestamp"]
                collector["first_interaction"]["type"] = "english_auction"
                collector["first_interaction"]["timestamp"] = transaction["timestamp"]

            if date > last_collect_date:
                collector["last_collect"]["id"] = objkt_id
                collector["last_collect"]["timestamp"] = transaction["timestamp"]

            collector["english_auction_objkts"].append(objkt_id)
            collector["english_auction_money_spent"].append(amount)
            collector["english_auction_timestamps"].append(transaction["timestamp"])

    for transaction in dutch_auction_transactions:
        auction = dutch_auctions_bigmap[transaction["parameter"]["value"]]
        collector_wallet_id = transaction["sender"]["address"]
        amount = transaction["amount"] / 1e6
        objkt_id = auction["objkt_id"]

        if collector_wallet_id.startswith("tz"):
            if collector_wallet_id not in collectors:
                collectors[collector_wallet_id] = {
                    "type": "collector",
                    "wallet_id": collector_wallet_id,
                    "alias": "",
                    "reported": False,
                    "first_collect": {
                        "id": objkt_id,
                        "timestamp": transaction["timestamp"]},
                    "last_collect": {
                        "id": objkt_id,
                        "timestamp": transaction["timestamp"]},
                    "first_interaction": {
                        "type": "dutch_auction",
                        "timestamp": transaction["timestamp"]},
                    "bid_objkts": [],
                    "bid_money_spent": [],
                    "bid_timestamps": [],
                    "ask_objkts": [],
                    "ask_money_spent": [],
                    "ask_timestamps": [],
                    "english_auction_objkts": [],
                    "english_auction_money_spent": [],
                    "english_auction_timestamps": [],
                    "dutch_auction_objkts": [],
                    "dutch_auction_money_spent": [],
                    "dutch_auction_timestamps": []}

            collector = collectors[collector_wallet_id]
            first_collect_date = get_datetime_from_timestamp(
                collector["first_collect"]["timestamp"])
            last_collect_date = get_datetime_from_timestamp(
                collector["last_collect"]["timestamp"])
            date = get_datetime_from_timestamp(transaction["timestamp"])

            if date < first_collect_date:
                collector["first_collect"]["id"] = objkt_id
                collector["first_collect"]["timestamp"] = transaction["timestamp"]
                collector["first_interaction"]["type"] = "dutch_auction"
                collector["first_interaction"]["timestamp"] = transaction["timestamp"]

            if date > last_collect_date:
                collector["last_collect"]["id"] = objkt_id
                collector["last_collect"]["timestamp"] = transaction["timestamp"]

            collector["dutch_auction_objkts"].append(objkt_id)
            collector["dutch_auction_money_spent"].append(amount)
            collector["dutch_auction_timestamps"].append(transaction["timestamp"])

    for collector_wallet_id, collector in collectors.items():
        collector["total_money_spent"] = (
            sum(collector["bid_money_spent"]) + 
            sum(collector["ask_money_spent"]) + 
            sum(collector["english_auction_money_spent"]) + 
            sum(collector["dutch_auction_money_spent"]))
        collector["items"] = (
            len(collector["bid_money_spent"]) + 
            len(collector["ask_money_spent"]) + 
            len(collector["english_auction_money_spent"]) + 
            len(collector["dutch_auction_money_spent"]))

        if collector_wallet_id in registries_bigmap:
            collector["alias"] = registries_bigmap[collector_wallet_id]["user"]
        elif "alias" in tezos_wallets[collector_wallet_id]:
            collector["alias"] = tezos_wallets[collector_wallet_id]["alias"]

    return collectors


def extract_fxhash_collector_accounts(mint_transactions, collect_transactions,
                                      registries_bigmap, tezos_wallets):
    """Extracts the fxhash collector accounts information from several lists of
    transactions.

    Parameters
    ----------
    mint_transactions: list
        The list of fxhash min transactions.
    collect_transactions: list
        The list of fxhash collect transactions.
    registries_bigmap: dict
        The H=N registries bigmap.
    tezos_wallets: dict
        The complete list of tezos wallets.

    Returns
    -------
    dict
        A python dictionary with the fxhash unique collector accounts.

    """
    collectors = {}

    for transaction in mint_transactions:
        collector_wallet_id = transaction["sender"]["address"]
        amount = transaction["amount"] / 1e6

        if collector_wallet_id.startswith("tz"):
            if collector_wallet_id not in collectors:
                collectors[collector_wallet_id] = {
                    "type": "collector",
                    "wallet_id": collector_wallet_id,
                    "alias": "",
                    "reported": False,
                    "first_collect": {
                        "timestamp": transaction["timestamp"]},
                    "last_collect": {
                        "timestamp": transaction["timestamp"]},
                    "first_interaction": {
                        "type": "mint",
                        "timestamp": transaction["timestamp"]},
                    "mint_money_spent": [],
                    "mint_timestamps": [],
                    "collect_money_spent": [],
                    "collect_timestamps": []}

            collector = collectors[collector_wallet_id]
            collector["last_collect"]["timestamp"] = transaction["timestamp"]
            collector["mint_money_spent"].append(amount)
            collector["mint_timestamps"].append(transaction["timestamp"])

    for transaction in collect_transactions:
        collector_wallet_id = transaction["sender"]["address"]
        amount = transaction["amount"] / 1e6

        if collector_wallet_id.startswith("tz"):
            if collector_wallet_id not in collectors:
                collectors[collector_wallet_id] = {
                    "type": "collector",
                    "wallet_id": collector_wallet_id,
                    "alias": "",
                    "reported": False,
                    "first_collect": {
                        "timestamp": transaction["timestamp"]},
                    "last_collect": {
                        "timestamp": transaction["timestamp"]},
                    "first_interaction": {
                        "type": "collect",
                        "timestamp": transaction["timestamp"]},
                    "mint_money_spent": [],
                    "mint_timestamps": [],
                    "collect_money_spent": [],
                    "collect_timestamps": []}

            collector = collectors[collector_wallet_id]
            first_collect_date = get_datetime_from_timestamp(
                collector["first_collect"]["timestamp"])
            last_collect_date = get_datetime_from_timestamp(
                collector["last_collect"]["timestamp"])
            date = get_datetime_from_timestamp(transaction["timestamp"])

            if date < first_collect_date:
                collector["first_collect"]["timestamp"] = transaction["timestamp"]
                collector["first_interaction"]["type"] = "collect"
                collector["first_interaction"]["timestamp"] = transaction["timestamp"]

            if date > last_collect_date:
                collector["last_collect"]["timestamp"] = transaction["timestamp"]

            collector["collect_money_spent"].append(amount)
            collector["collect_timestamps"].append(transaction["timestamp"])

    for collector_wallet_id, collector in collectors.items():
        collector["total_money_spent"] = (
            sum(collector["mint_money_spent"]) + 
            sum(collector["collect_money_spent"]))
        collector["items"] = (
            len(collector["mint_money_spent"]) + 
            len(collector["collect_money_spent"]))

        if collector_wallet_id in registries_bigmap:
            collector["alias"] = registries_bigmap[collector_wallet_id]["user"]
        elif "alias" in tezos_wallets[collector_wallet_id]:
            collector["alias"] = tezos_wallets[collector_wallet_id]["alias"]

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
            artist["last_collect"] = collector["last_collect"]
            artist["money_spent"] = collector["money_spent"]
            artist["total_money_spent"] = collector["total_money_spent"]

            # Check which was the first artist interation
            first_objkt_date = get_datetime_from_timestamp(
                artist["first_objkt"]["timestamp"])
            first_collect_date = get_datetime_from_timestamp(
                artist["first_collect"]["timestamp"])

            if first_collect_date < first_objkt_date:
                artist["first_interaction"]["type"] = "collect"
                artist["first_interaction"]["timestamp"] = artist[
                    "first_collect"]["timestamp"]
        else:
            # Set the collector type to patron
            collector["type"] = "patron"

            # Add the collector to the patrons dictionary
            patrons[wallet_id] = collector

    return patrons


def get_user_accounts(artists, patrons, swappers):
    """Gets the user accounts from a set of artists, patrons and swappers.

    Parameters
    ----------
    artists: dict
        The python dictionary with the artists accounts.
    patrons: dict
        The python dictionary with the patrons accounts.
    swappers: dict
        The python dictionary with the swappers accounts.

    Returns
    -------
    dict
        A python dictionary with the user accounts.

    """
    # Get the only swappers accounts
    only_swappers = {}

    for wallet, swapper in swappers.items():
        if wallet not in artists and wallet not in patrons:
            only_swappers[wallet] = swapper

    # Get the combined wallet ids and time stamps
    wallet_ids = np.array(
        [wallet_id for wallet_id in artists] +
        [wallet_id for wallet_id in patrons] + 
        [wallet_id for wallet_id in only_swappers])
    timestamps = np.array(
        [artist["first_interaction"]["timestamp"] for artist in artists.values()] + 
        [patron["first_interaction"]["timestamp"] for patron in patrons.values()] +
        [swapper["first_interaction"]["timestamp"] for swapper in only_swappers.values()])

    # Order the users by their time stamps
    dates = np.array(
        [get_datetime_from_timestamp(timestamp) for timestamp in timestamps])
    wallet_ids = wallet_ids[np.argsort(dates)]
    users = {}

    for wallet_id in wallet_ids:
        if wallet_id in artists:
            users[wallet_id] = artists[wallet_id]
        elif wallet_id in patrons:
            users[wallet_id] = patrons[wallet_id]
        else:
            users[wallet_id] = only_swappers[wallet_id]

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


def extract_users_connections(objkt_creators, transactions, swaps_bigmap,
                              users, objktcom_collectors, reported_users):
    """Extracts the users connections.

    Parameters
    ----------
    objkt_creators: dict
        The OBJKT creators.
    transactions: list
        The list of collect transactions.
    swaps_bigmap: dict
        The H=N marketplace swaps bigmap.
    users: dict
        The H=N users.
    objktcom_collectors: dict
        The objkt.com collectors.
    reported_users: list
        The python list with the wallet ids of all H=N reported users.

    Returns
    -------
    tuple
        A python tuple with the users connections information and a serialized
        version of it.

    """
    users_connections = {}
    user_counter = 0

    for artist_wallet_id in objkt_creators.values():
        # Add the artists wallets, since it could be that they might not have
        # connections from collect operations
        if artist_wallet_id.startswith("KT"):
            continue
        elif artist_wallet_id not in users_connections:
            if artist_wallet_id in users:
                alias = users[artist_wallet_id]["alias"]
            else:
                alias = ""

            users_connections[artist_wallet_id] = {
                "alias": alias,
                "artists" : {},
                "collectors" : {},
                "reported": False,
                "counter": user_counter}
            user_counter += 1

    for transaction in transactions:
        # Get the swap id from the collect entrypoint input parameters
        parameters = transaction["parameter"]["value"]

        if isinstance(parameters, dict):
            swap_id = parameters["swap_id"]
        else:
            swap_id = parameters

        # Get the objkt id from the swaps bigmap
        objkt_id = swaps_bigmap[swap_id]["objkt_id"]

        # Get the collector and artist wallet ids
        collector_wallet_id = transaction["sender"]["address"]
        artist_wallet_id = objkt_creators[objkt_id]

        # Move to the next transaction if one of the walles is a contract
        if (artist_wallet_id.startswith("KT") or 
            collector_wallet_id.startswith("KT")):
            continue

        # Move to the next transaction if the artist and the collector coincide
        if artist_wallet_id == collector_wallet_id:
            continue

        # Add the collector to the artist collectors list
        collectors = users_connections[artist_wallet_id]["collectors"]

        if collector_wallet_id in collectors:
            collectors[collector_wallet_id] += 1
        else:
            collectors[collector_wallet_id] = 1

        # Add the artist to the collector artists list
        if collector_wallet_id in users_connections:
            artists = users_connections[collector_wallet_id]["artists"]

            if artist_wallet_id in artists:
                artists[artist_wallet_id] += 1
            else:
                artists[artist_wallet_id] = 1
        else:
            if collector_wallet_id in users:
                alias = users[collector_wallet_id]["alias"]
            else:
                alias = ""

            users_connections[collector_wallet_id] = {
                "alias": alias,
                "artists" : {artist_wallet_id: 1},
                "collectors" : {},
                "reported": False,
                "counter": user_counter}
            user_counter += 1

    for collector_wallet_id, objktcom_collector in objktcom_collectors.items():
        # Get the objkt ids
        objkt_ids = (objktcom_collector["bid_objkts"] + 
                     objktcom_collector["ask_objkts"] + 
                     objktcom_collector["english_auction_objkts"] + 
                     objktcom_collector["dutch_auction_objkts"])

        # Loop over the collected OBJKT ids
        for objkt_id in objkt_ids:
            # Get the artist wallet id
            artist_wallet_id = objkt_creators[objkt_id]

            # Move to the next OBJKT if the wallet is a contract
            if artist_wallet_id.startswith("KT"):
                continue

            # Move to the next OBJKT if the artist and the collector coincide
            if artist_wallet_id == collector_wallet_id:
                continue

            # Add the collector to the artist collectors list
            collectors = users_connections[artist_wallet_id]["collectors"]

            if collector_wallet_id in collectors:
                collectors[collector_wallet_id] += 1
            else:
                collectors[collector_wallet_id] = 1

            # Add the artist to the collector artists list
            if collector_wallet_id in users_connections:
                artists = users_connections[collector_wallet_id]["artists"]

                if artist_wallet_id in artists:
                    artists[artist_wallet_id] += 1
                else:
                    artists[artist_wallet_id] = 1
            else:
                if collector_wallet_id in users:
                    alias = users[collector_wallet_id]["alias"]
                else:
                    alias = objktcom_collector["alias"]

                users_connections[collector_wallet_id] = {
                    "alias": alias,
                    "artists" : {artist_wallet_id: 1},
                    "collectors" : {},
                    "reported": False,
                    "counter": user_counter}
                user_counter += 1

    # Fill the reported user information
    for reported_user_wallet_id in reported_users:
        if reported_user_wallet_id in users_connections:
            users_connections[reported_user_wallet_id]["reported"] = True

    # Process the connections information to a different format
    for user in users_connections.values():
        # Get the lists of artist and collectors wallets
        artists_and_collectors_wallets = [
            wallet for wallet in user["artists"] if wallet in user["collectors"]]
        artists_wallets = [
            wallet for wallet in user["artists"] if not wallet in user["collectors"]]
        collectors_wallets = [
            wallet for wallet in user["collectors"] if not wallet in user["artists"]]

        # Get the lists of artists and collectors weights
        artists_and_collectors_weights = [
            user["artists"][wallet] + user["collectors"][wallet] for wallet in artists_and_collectors_wallets]
        artists_weights = [
            user["artists"][wallet] for wallet in artists_wallets]
        collectors_weights = [
            user["collectors"][wallet] for wallet in collectors_wallets]

        # Add these lists to the user information
        user["artists_and_collectors"] = artists_and_collectors_wallets
        user["artists_and_collectors_weights"] = artists_and_collectors_weights
        user["artists"] = artists_wallets
        user["artists_weights"] = artists_weights
        user["collectors"] = collectors_wallets
        user["collectors_weights"] = collectors_weights

    # Create the serialized users connections
    serialized_users_connections = {}

    for wallet_id, user in users_connections.items():
        serialized_artists_and_collectors = [
            users_connections[artist_and_collector]["counter"] for artist_and_collector in user["artists_and_collectors"]]
        serialized_artists = [
            users_connections[artist]["counter"] for artist in user["artists"]]
        serialized_collectors = [
            users_connections[collector]["counter"] for collector in user["collectors"]]

        serialized_users_connections[user["counter"]] = {
            "wallet": wallet_id,
            "alias": user["alias"],
            "artists_and_collectors": serialized_artists_and_collectors,
            "artists_and_collectors_weights": user["artists_and_collectors_weights"],
            "artists": serialized_artists,
            "artists_weights": user["artists_weights"],
            "collectors": serialized_collectors,
            "collectors_weights": user["collectors_weights"],
            "reported": user["reported"]}

    return users_connections, serialized_users_connections


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


def add_accounts_metadata(accounts, from_account_index=0, to_account_index=None,
                          sleep_time=1):
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
    dates = [get_datetime_from_timestamp(timestamp) for timestamp in timestamps]
    years = np.array([date.year for date in dates])
    months = np.array([date.month for date in dates])
    days = np.array([date.day for date in dates])

    return years, months, days


def get_counts_per_day(timestamps, first_year=2021, first_month=3, first_day=1):
    """Calculates the counts per day for a list of time stamps.

    Parameters
    ----------
    timestamps: list
        A python list with ordered time stamps.
    first_year: int, optional
        The first year to count. Default is 2021.
    first_month: int, optional
        The first month to count. Default is 3 (March).
    first_day: int, optional
        The first day to count. Default is 1.

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
    now = datetime.utcnow()

    for year in range(first_year, np.max(years) + 1):
        for month in range(1, 13):
            for day in range(1, monthrange(year, month)[1] + 1):
                # Check if we passed the first day
                if not started:
                    started = ((year == first_year) and 
                               (month == first_month) and 
                               (day == first_day))

                # Check that we started and didn't finish yet
                if started and not finished:
                    # Add the number of operations for the current day
                    counts_per_day.append(np.sum(
                        (years == year) & (months == month) & (days == day)))

                    # Check if we reached the current day
                    finished = (year == now.year) and (
                        month == now.month) and (day == now.day)

    return counts_per_day


def group_users_per_day(users, first_year=2021, first_month=3, first_day=1):
    """Groups the given users per the day of their first interaction.

    Parameters
    ----------
    users: dict
        A python dictionary with the users information.
    first_year: int, optional
        The first year to count. Default is 2021.
    first_month: int, optional
        The first month to count. Default is 3 (March).
    first_day: int, optional
        The first day to count. Default is 1.

    Returns
    -------
    list
        A python list with the users grouped by day.

    """
    # Get the users wallet ids and their first interation time stamp
    wallet_ids = np.array(list(users.keys()))
    timestamps = np.array(
        [user["first_interaction"]["timestamp"] for user in users.values()])

    # Extract the years, months and days from the time stamps
    years, months, days = split_timestamps(timestamps)

    # Get the users per day
    users_per_day = []
    started = False
    finished = False
    now = datetime.utcnow()

    for year in range(first_year, np.max(years) + 1):
        for month in range(1, 13):
            for day in range(1, monthrange(year, month)[1] + 1):
                # Check if we passed the starting day
                if not started:
                    started = ((year == first_year) and 
                               (month == first_month) and 
                               (day == first_day))

                # Check that we started and didn't finish yet
                if started and not finished:
                    selected_wallets_ids = wallet_ids[
                        (years == year) & (months == month) & (days == day)]
                    users_per_day.append(
                        [users[wallet_id] for wallet_id in selected_wallets_ids])

                    # Check if we reached the current day
                    finished = (year == now.year) and (
                        month == now.month) and (day == now.day)

    return users_per_day


def get_swapped_objkts(swaps_bigmap, min_objkt_id=0, max_objkt_id=np.Inf,
                       min_price=0, max_price=np.Inf):
    """Gets a list of swapped OBJKTs with the lowest price.

    Parameters
    ----------
    swaps_bigmap: dict
        A python dictionary with the H=N swaps bigmap.
    min_objkt_id: int, optional
        The minimum OBJKT id to use for the selection. Default is 0.
    max_objkt_id: int, optional
        The maximum OBJKT id to use for the selection. Default is no upper
        limit.
    min_price: float, optional
        The minimum OBJKT edition price in tez to use for the selection.
        Default is 0.
    max_price: float, optional
        The maximum OBJKT edition price in tez to use for the selection.
        Default is no upper limit.

    Returns
    -------
    dict
        A python dictionary with selected OBJKTs lowest price.

    """
    # Select the swaps that satisfy the specified conditions
    selected_swaps = {}

    for key, swap in swaps_bigmap.items():
        # Select only active swaps from the H=N marketplace v2 smart contract
        if int(key) >= 500000 and swap["active"] and int(swap["objkt_amount"]) > 0:
            objkt_id = int(swap["objkt_id"])
            price = float(swap["xtz_per_objkt"]) / 1e6

            if (objkt_id >= min_objkt_id and objkt_id <= max_objkt_id) and (
                price >= min_price and price <= max_price):
                if objkt_id not in selected_swaps:
                    selected_swaps[objkt_id] = price
                elif price < selected_swaps[objkt_id]:
                    selected_swaps[objkt_id] = price

    return {key: selected_swaps[key] for key in sorted(selected_swaps.keys())}
