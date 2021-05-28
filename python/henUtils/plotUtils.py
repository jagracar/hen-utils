import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime


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
    # Get the new users per day counts
    datetime_format = "%Y-%m-%dT%H:%M:%SZ"
    dates = [datetime.strptime(
        users[wallet_id]["first_interaction"]["timestamp"], datetime_format) for wallet_id in users]
    years = np.array([date.year for date in dates])
    months = np.array([date.month for date in dates])
    days = np.array([date.day for date in dates])
    counts = None

    for year in np.unique(years):
        for month in np.unique(months[years == year]):
            month_counts = np.unique(
                days[np.logical_and(years == year, months == month)], return_counts=True)[1]

            if counts is None:
                counts = month_counts
            else:
                counts = np.hstack((counts, month_counts))

    if exclude_last_day:
        counts = counts[:-1]

    # Create the figure
    plt.figure(figsize=(7, 5), facecolor="white", tight_layout=True, **kwargs)
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.plot(counts)
    plt.show(block=False)


def plot_collected_money_per_day(money, timestamps, title, x_label, y_label, exclude_last_day=False, **kwargs):
    """Plots the money spent in collect operations per day as a function of
    time.

    Parameters
    ----------
    money: object
        A numpy array with the money of each collect operation.
    timestamps: object
        A numpy array with the timestamps of each collect operation.
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
    # Get the money spent per day
    datetime_format = "%Y-%m-%dT%H:%M:%SZ"
    dates = [datetime.strptime(
        timestamp, datetime_format) for timestamp in timestamps]
    years = np.array([date.year for date in dates])
    months = np.array([date.month for date in dates])
    days = np.array([date.day for date in dates])
    money_per_day = []

    for year in np.unique(years):
        months_in_year = np.unique(months[years == year])

        for month in months_in_year:
            days_in_month = np.unique(days[(years == year) & (months == month)])

            for day in days_in_month:
                cond = (years == year) & (months == month) & (days == day)
                money_per_day.append(np.sum(money[cond]))

    if exclude_last_day:
        money_per_day = money_per_day[:-1]

    # Create the figure
    plt.figure(figsize=(7, 5), facecolor="white", tight_layout=True, **kwargs)
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.plot(money_per_day)
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
    # Get the OBJKT distribution in different price ranges per day
    datetime_format = "%Y-%m-%dT%H:%M:%SZ"
    dates = [datetime.strptime(
        timestamp, datetime_format) for timestamp in timestamps]
    years = np.array([date.year for date in dates])
    months = np.array([date.month for date in dates])
    days = np.array([date.day for date in dates])
    range_1 = []
    range_2 = []
    range_3 = []
    range_4 = []

    for year in np.unique(years):
        months_in_year = np.unique(months[years == year])

        for month in months_in_year:
            days_in_month = np.unique(days[(years == year) & (months == month)])

            for day in days_in_month:
                cond = (years == year) & (months == month) & (days == day)
                range_1.append(np.sum((money[cond] >= price_ranges[0]) & (money[cond] < price_ranges[1])))
                range_2.append(np.sum((money[cond] >= price_ranges[1]) & (money[cond] < price_ranges[2])))
                range_3.append(np.sum((money[cond] >= price_ranges[2]) & (money[cond] < price_ranges[3])))
                range_4.append(10 * np.sum((money[cond] >= price_ranges[3])))

    if exclude_last_day:
        range_1 = range_1[:-1]
        range_2 = range_2[:-1]
        range_3 = range_3[:-1]
        range_4 = range_4[:-1]

    # Create the figure
    plt.figure(figsize=(7, 5), facecolor="white", tight_layout=True, **kwargs)
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.plot(range_1)
    plt.plot(range_2)
    plt.plot(range_3)
    plt.plot(range_4)
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
