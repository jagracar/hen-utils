import numpy as np
from henUtils.queryUtils import *
from henUtils.plotUtils import *

# Exclude the last day from most of the plots?
exclude_last_day = True

# Set the path to the directory where the wallet and exchange information will
# be saved to avoid to query for it again and again
tezos_dir = "../data/tezos"

# Set the path to the directory where the figures will be saved
figures_dir = "../figures"

# Get the complete list of tezos wallets
tezos_wallets = get_tezos_wallets(tezos_dir, sleep_time=1)

# Get the wallets first activity
wallets = np.array(
    [wallet["address"].startswith("tz") for wallet in tezos_wallets.values()])
first_activity_timestamps = np.array(
    [wallet["firstActivityTime"] for wallet in tezos_wallets.values()])

# Plot the number of new tezos wallets created per day
plot_data_per_day(
    wallets, first_activity_timestamps,
    "New tezos wallets per day",
    "Days since 5th of August 2019", "New tezos wallets",
    exclude_last_day=exclude_last_day, first_year=2019, first_month=8,
    first_day=5)
save_figure(os.path.join(figures_dir, "new_wallets_per_day.png"))

plot_data_per_day(
    wallets, first_activity_timestamps,
    "New tezos wallets per day",
    "Days since first minted OBJKT (1st of March)", "New tezos wallets",
    exclude_last_day=exclude_last_day, first_month=3, first_day=1)
save_figure(os.path.join(figures_dir, "new_wallets_per_day_since_hen.png"))

# Get the tez exchange rates
exchange_timestamps, exchange_rates = get_tez_exchange_rates(
    "USD", start_date="2019-08-05T00:00:00Z")
exchange_timestamps, exchange_rates = get_tez_exchange_rates(
    "USD", start_date="2021-03-01T00:00:00Z")

plot_data_per_day(
    exchange_rates, exchange_timestamps,
    "New tezos wallets per day",
    "Days since 30th of June 2018", "New tezos wallets",
    exclude_last_day=exclude_last_day, first_year=2019, first_month=8,
    first_day=5)
save_figure(os.path.join(figures_dir, "new_wallets_per_day.png"))
