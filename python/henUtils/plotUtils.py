import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
from calendar import monthrange

from henUtils.queryUtils import split_timestamps, get_counts_per_day


def plot_histogram(data, title, x_label, y_label, bins=100, **kwargs):
    """Plots a histogram of the given data.

    Parameters
    ----------
    data: object
        A numpy array with the data to use for the histogram.
    title: str
        The histogram title.
    x_label: str
        The label for the x axis.
    y_label: str
        The label for the y axis.
    bins: int, optional
        The number of bins that the histogram should have. Default is 100.
    kwargs: plt.figure properties
        Any additional property that should be passed to the figure.

    """
    plt.figure(figsize=(7, 5), facecolor="white", tight_layout=True, **kwargs)
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.hist(data, bins=bins)
    plt.show(block=False)


def plot_operations_per_day(operations, title, x_label, y_label, exclude_last_day=False, **kwargs):
    """Plots the number of operation per day as a function of time.

    Parameters
    ----------
    operations: list
        A python list with the operations information.
    title: str
        The plot title.
    x_label: str
        The label for the x axis.
    y_label: str
        The label for the y axis.
    exclude_last_day: bool, optional
        If True the last day will be excluded from the plot. Default is False.
    kwargs: plt.figure properties
        Any additional property that should be passed to the figure.

    """
    # Get the operations per day
    timestamps = [operation["timestamp"] for operation in operations]
    operations_per_day = get_counts_per_day(timestamps)

    if exclude_last_day:
        operations_per_day = operations_per_day[:-1]

    # Create the figure
    plt.figure(figsize=(7, 5), facecolor="white", tight_layout=True, **kwargs)
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.plot(operations_per_day)
    plt.show(block=False)


def plot_new_users_per_day(users, title, x_label, y_label, exclude_last_day=False, **kwargs):
    """Plots the new users per day as a function of time.

    Parameters
    ----------
    users: dict
        A python dictionary with the users information.
    title: str
        The plot title.
    x_label: str
        The label for the x axis.
    y_label: str
        The label for the y axis.
    exclude_last_day: bool, optional
        If True the last day will be excluded from the plot. Default is False.
    kwargs: plt.figure properties
        Any additional property that should be passed to the figure.

    """
    # Get the users per day
    timestamps = [users[wallet_id]["first_interaction"]["timestamp"] for wallet_id in users]
    users_per_day = get_counts_per_day(timestamps)

    if exclude_last_day:
        users_per_day = users_per_day[:-1]

    # Create the figure
    plt.figure(figsize=(7, 5), facecolor="white", tight_layout=True, **kwargs)
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.plot(users_per_day)
    plt.show(block=False)


def plot_data_per_day(data, timestamps, title, x_label, y_label, exclude_last_day=False, **kwargs):
    """Plots some combined data per day as a function of time.

    Parameters
    ----------
    editions: object
        A numpy array with the data.
    timestamps: object
        A numpy array with the timestamps.
    title: str
        The plot title.
    x_label: str
        The label for the x axis.
    y_label: str
        The label for the y axis.
    exclude_last_day: bool, optional
        If True the last day will be excluded from the plot. Default is False.
    kwargs: plt.figure properties
        Any additional property that should be passed to the figure.

    """
    # Extract the years, months and days from the time stamps
    years, months, days = split_timestamps(timestamps)

    # Get the data per day
    data = np.array(data)
    data_per_day = []
    started = False
    finished = False
    now = datetime.now()

    for year in range(2021, np.max(years) + 1):
        for month in range(1, 13):
            for day in range(1, monthrange(year, month)[1] + 1):
                # Check if we passed the starting day: 2021-03-01
                if not started:
                    started = (year == 2021) and (month == 3) and (day == 1)

                # Check that we started and didn't finish yet
                if started and not finished:
                    # Add the combined data for the current day
                    data_per_day.append(np.sum(data[
                        (years == year) & (months == month) & (days == day)]))

                    # Check if we reached the current day
                    finished = (year == now.year) and (
                        month == now.month) and (day == now.day)

    if exclude_last_day:
        data_per_day = data_per_day[:-1]

    # Create the figure
    plt.figure(figsize=(7, 5), facecolor="white", tight_layout=True, **kwargs)
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.plot(data_per_day)
    plt.show(block=False)


def plot_price_distribution_per_day(money, timestamps, price_ranges, title, x_label, y_label, exclude_last_day=False, **kwargs):
    """Plots the price distribution in collect operations per day as a function
    of time.

    Parameters
    ----------
    money: object
        A numpy array with the money of each collect operation.
    timestamps: object
        A numpy array with the timestamps of each collect operation.
    price_ranges: list
        A python list with 4 elements indicating the price ranges to use. 
    title: str
        The plot title.
    x_label: str
        The label for the x axis.
    y_label: str
        The label for the y axis.
    exclude_last_day: bool, optional
        If True the last day will be excluded from the plot. Default is False.
    kwargs: plt.figure properties
        Any additional property that should be passed to the figure.

    """
    # Extract the years, months and days from the time stamps
    years, months, days = split_timestamps(timestamps)

    # Get the operation counts in the different price ranges per day
    counts_range_1 = []
    counts_range_2 = []
    counts_range_3 = []
    counts_range_4 = []
    started = False
    finished = False
    now = datetime.now()

    for year in range(2021, np.max(years) + 1):
        for month in range(1, 13):
            for day in range(1, monthrange(year, month)[1] + 1):
                # Check if we passed the starting day: 2021-03-01
                if not started:
                    started = (year == 2021) and (month == 3) and (day == 1)

                # Check that we started and didn't finish yet
                if started and not finished:
                    # Get the number of operations for the current day in each range
                    cond = (years == year) & (months == month) & (days == day)
                    counts_range_1.append(np.sum(
                        (money[cond] >= price_ranges[0]) & (money[cond] < price_ranges[1])))
                    counts_range_2.append(np.sum(
                        (money[cond] >= price_ranges[1]) & (money[cond] < price_ranges[2])))
                    counts_range_3.append(np.sum(
                        (money[cond] >= price_ranges[2]) & (money[cond] < price_ranges[3])))
                    counts_range_4.append(10 * np.sum(
                        (money[cond] >= price_ranges[3])))

                    # Check if we reached the last day
                    finished = (year == now.year) and (
                        month == now.month) and (day == now.day)

    if exclude_last_day:
        counts_range_1 = counts_range_1[:-1]
        counts_range_2 = counts_range_2[:-1]
        counts_range_3 = counts_range_3[:-1]
        counts_range_4 = counts_range_4[:-1]

    # Create the figure
    plt.figure(figsize=(7, 5), facecolor="white", tight_layout=True, **kwargs)
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.plot(counts_range_1, label="%.2f tez ≤ edition price < %.0f tez" % (
        price_ranges[0], price_ranges[1]))
    plt.plot(counts_range_2, label="%.0f tez ≤ edition price < %.0f tez" % (
        price_ranges[1], price_ranges[2]))
    plt.plot(counts_range_3, label="%.0f tez ≤ edition price < %.0f tez" % (
        price_ranges[2], price_ranges[3]))
    plt.plot(counts_range_4, label="edition price ≥ %.0f tez (x10)" % (
        price_ranges[3]))
    plt.legend()
    plt.show(block=False)


def plot_active_users_per_day(wallet_ids, timestamps, title, x_label, y_label, exclude_last_day=False, **kwargs):
    """Plots the active users per day as a function of time.

    Parameters
    ----------
    wallet_ids: object
        A numpy array with the wallet id of each operation.
    timestamps: object
        A numpy array with the timestamps of each operation.
    title: str
        The plot title.
    x_label: str
        The label for the x axis.
    y_label: str
        The label for the y axis.
    exclude_last_day: bool, optional
        If True the last day will be excluded from the plot. Default is False.
    kwargs: plt.figure properties
        Any additional property that should be passed to the figure.

    """
    # Extract the years, months and days from the time stamps
    years, months, days = split_timestamps(timestamps)

    # Get the active users per day
    active_users_per_day = []
    started = False
    finished = False
    now = datetime.now()

    for year in range(2021, np.max(years) + 1):
        for month in range(1, 13):
            for day in range(1, monthrange(year, month)[1] + 1):
                # Check if we passed the starting day: 2021-03-01
                if not started:
                    started = (year == 2021) and (month == 3) and (day == 1)

                # Check that we started and didn't finish yet
                if started and not finished:
                    # Get the number of unique users for the current day
                    active_users_per_day.append(len(np.unique(wallet_ids[
                        (years == year) & (months == month) & (days == day)])))

                    # Check if we reached the last day
                    finished = (year == now.year) and (
                        month == now.month) and (day == now.day)

    if exclude_last_day:
        active_users_per_day = active_users_per_day[:-1]

    # Create the figure
    plt.figure(figsize=(7, 5), facecolor="white", tight_layout=True, **kwargs)
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.plot(active_users_per_day)
    plt.show(block=False)


def plot_users_last_active_day(wallet_ids, timestamps, title, x_label, y_label, exclude_last_day=False, **kwargs):
    """Plots users last active day as a function of time.

    Parameters
    ----------
    wallet_ids: object
        A numpy array with the wallet id of each operation.
    timestamps: object
        A numpy array with the timestamps of each operation.
    title: str
        The plot title.
    x_label: str
        The label for the x axis.
    y_label: str
        The label for the y axis.
    exclude_last_day: bool, optional
        If True the last day will be excluded from the plot. Default is False.
    kwargs: plt.figure properties
        Any additional property that should be passed to the figure.

    """
    # Get the users last activity time stamp
    users_last_activity = {}

    for wallet_id, timestamp in zip(wallet_ids, timestamps):
        if wallet_id in users_last_activity:
            if users_last_activity[wallet_id] < timestamp:
                users_last_activity[wallet_id] = timestamp
        else:
            users_last_activity[wallet_id] = timestamp

    # Get the last activity time stamps
    timestamps = list(users_last_activity.values())

    # Get the users per day
    users_per_day = get_counts_per_day(timestamps)

    if exclude_last_day:
        users_per_day = users_per_day[:-1]

    # Create the figure
    plt.figure(figsize=(7, 5), facecolor="white", tight_layout=True, **kwargs)
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.plot(users_per_day)
    plt.show(block=False)


def save_figure(file_name, **kwargs):
    """Saves an image of the current figure.

    Parameters
    ----------
    file_name: object
        The complete path to the file where the figure should be saved.
    kwargs: figure.savefig properties
        Any additional property that should be passed to the savefig method.

    """
    plt.gcf().savefig(file_name, **kwargs)
