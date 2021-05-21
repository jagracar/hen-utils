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
