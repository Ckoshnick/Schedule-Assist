# -*- coding: utf-8 -*-
"""
Created on Tue Jul 24 14:41:42 2018

@author: koshnick
"""

import mypy
import datetime

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib as mpl
import matplotlib.pyplot as plt

from math import floor
from pi_client import pi_client
from matplotlib.backends.backend_pdf import PdfPages

sns.set()


# =============================================================================
# ---Functions
# =============================================================================


def load_standard_schedules():
    """ Load in the gold standard of scheudles from excel file """

    fileName = 'AHU schedules.xlsx'

    standard = pd.read_excel(fileName,
                             index_col=0,
                             header=[0, 1],  # make mutli index
                             parse_dates=True)

    replacements = {'Off': 0,
                    'OFF': 0,
                    'off': 0}

    standard = standard.replace(replacements)

    # Drop un-needed 'Notes' column
    try:
        # standard = standard.drop('0', axis=0)
        standard = standard.drop("Notes", axis=1)
        standard = standard.drop("Concat", axis=0)
    except KeyError:
        pass

    standard = standard.T  # need transpose so timeseries is on index

    standard['value'] = standard.index.get_level_values(1)
    standard['value'] = standard['value'].replace({'Start': 1, 'End': 0})

    return standard


def inflate_schedules(standard):
    """ Load in standard schedule and expand to a 15 min time series axis """

    # Set up generic 1 week time index
    timeIndex = pd.DatetimeIndex(start='1990-01-01 00:00:00',
                                 end='1990-01-07 23:45:00',
                                 freq='15min')

    # Build empty dataframe with generic time index
    df = pd.DataFrame([], columns=standard.columns, index=timeIndex)

    # Iterate over 'standard' schedules and update the inflated df with 1 or 0
    for index, row in standard.iterrows():

        if index[0] == 'weekstart':  # do weekstart stuff
            for ahu, value in row.iteritems():
                df.at['1990-01-01T00:00:00', ahu] = value

        else:  # do normal day lookups
            value = row['value']  # value = 1 on value = 0 off
            for ahu, date in row.iteritems():

                if ahu == 'value':  # Ignore this column (not an AHU)
                    continue
                if date == 0:
                    continue
                if pd.isnull(date):  # Ignore days where AHU if off
                    print("Warning: Null value detected {} {} {}".format(
                            row, ahu, date))
                    continue

                df.at[str(date), ahu] = value  # set value in inflated df

                if len(df.index) > 672:  # dont allow new dates to be added
                    # Or do let new dates be added, but then just resample?
                    print('INFLATION ERROR DF EXPANDED BEYOND 672!')
                    return df

    # Fill in values forward in time
    # so start = 1 fills values of 1 until there is a stop (0 value)
    df = df.ffill(axis=0)

    # Build in time columns then make into multinindx for later meerge
    df = mypy.build_time_columns(df)
    df = df.drop(['sunday', 'saturday', 'weekday', 'dayofmonth', 'month',
                  'year', 'daytime', 'weekofyear', 'value'], axis=1)

    df = df.set_index(keys=['dayofweek', 'hour', 'minute'])

    return df


def pull_pi_status(piTags, start="2018-01-05", end="y"):
    """ Pulls the Run Status tag for each AHU listed in AHU_schedules.xlsx """

    newTags = []

    print('Pulling PI data...')
    print(f'Starting at {start} until {end}')

    for i, tag in enumerate(piTags):
        if tag in ['day', 'value']:
            continue
        else:
            newTags.append(tag + '.Run Status')

    pi = pi_client()

    # Based on behavior of PI tag, need to use interpolated

    piData = pi.get_stream_by_point(newTags,
                                    start=start,
                                    end=end,
                                    calculation='interpolated',
                                    interval='15m',
                                    _chunk_size=20,
                                    _web_id_source='get')

    print('PI data recieved!')

    return piData


def combine_standard_pi(piData, standard):
    """ Perform a merge such that the standard schedule is matched with PI """

#    piData = piData.drop('weekofyear',axis=1)
#    standard = standard.drop('weekofyear',axis=1)

    piData = mypy.build_time_columns(piData)

    piData = piData.drop(['sunday', 'saturday', 'weekday', 'dayofmonth',
                          'month', 'year', 'daytime', 'weekofyear'],
                         axis=1)

    # reset indecies to make merge happen
    piData = piData.reset_index()
    standard = standard.reset_index()

    merged = piData.merge(standard, how='left',
                          left_on=['dayofweek', 'hour', 'minute'],
                          right_on=['dayofweek', 'hour', 'minute'])

    # Restore index from piData
    merged = merged.set_index('Timestamp')

    merged = merged.drop(['dayofweek', 'hour', 'minute'], axis=1)

    return merged


def find_difference_in_columns(df, ahuCols, tag='.Run Status'):
    """ Find the mismatches (+ or -) in the schedule and Run Status points """

    newDf = pd.DataFrame(index=df.index)

    for ahu in ahuCols:

        if ahu in ['dayofweek', 'hour', 'minute', 'day']:
            continue
        else:
            try:
                newDf[ahu] = df[ahu + tag] - df[ahu]
            except KeyError:
                print('{} failed find difference ("df[ahu + tag] - df[ahu]")'
                      ''.format(ahu))

    return newDf


def type_aggreagte_mismatches(df, ahuCols, tag='.Run Status'):
    """ Takes the combined dataframe before 'diff' occurs and finds the 4 run
    categories """

#    newDf = pd.DataFrame()

    cols = list(df.columns)

    for ahu in ahuCols:

        statusTag = ahu + tag

        if statusTag in cols:
            pass
        else:
            continue

        df[ahu + ' result'] = np.nan

        df.loc[(df[ahu] == 1) & (df[statusTag] == 1), ahu + ' result'] = 'on'
        df.loc[(df[ahu] == 0) & (df[statusTag] == 0), ahu + ' result'] = 'off'
        df.loc[(df[ahu] == 0) & (df[statusTag] == 1), ahu + ' result'] = 'pos'
        df.loc[(df[ahu] == 1) & (df[statusTag] == 0), ahu + ' result'] = 'neg'

    filteredCols = df.columns[df.columns.str.contains(' result')]
    df = df[filteredCols]

    df.columns = list(df.columns.str.replace(' result', ''))

    return df


def aggregate_results(resultsDf, divisor=4):

    pos = resultsDf[resultsDf == 'pos']
    on = resultsDf[resultsDf == 'on']
    off = resultsDf[resultsDf == 'off']
    neg = resultsDf[resultsDf == 'neg']

    pSum = pos.count(axis=0) / divisor
    onSum = on.count(axis=0) / divisor
    nSum = neg.count(axis=0) / divisor
    offSum = off.count(axis=0) / divisor

    sums = pd.concat([pSum, onSum, offSum, nSum], axis=1)

    sums.columns = ['Over Hours', 'On Hours', 'Off Hours', 'Under Hours']

    sums = sums.sort_values('Over Hours', ascending=False)

    return sums


def update_weekly_archive(archiveName, diff, divisor=4):
    """ Load archive, combine with aggregate """

    # This division is because the sampling in 15min and we want hourly
    diff = diff[diff > 0]

    diff = mypy.build_time_columns(diff, interpTime='all')
    df = mypy.remove_time_cols(diff.groupby(by='weekofyear').sum()) / divisor
    df.index = pd.to_datetime(df.index)  # convert from categorical to datetime

    archive = pd.read_excel(archiveName, index_col=0, headers=0,
                            parse_dates=True)
    # The assumption is that df.index will have a 'longer' index, and to use
    # update the 'left' index must be the full index
    archive = archive.reindex(index=archive.index.union(df.index))
    # Allow for new AHUs to populate the list - back filling is limited
    archive = archive.reindex(columns=df.columns)

    archive.update(df, overwrite=True)
    archive.to_excel(archiveName)

    return archive


def generate_history(diff, divisor=4):
    """ Load archive, combine with aggregate """

    # This division is because the sampling in 15min and we want hourly
    diff = diff[diff > 0]

    diff = mypy.build_time_columns(diff, interpTime='all')
    df = mypy.remove_time_cols(diff.groupby(by='weekofyear').sum()) / divisor
    df.index = pd.to_datetime(df.index)  # convert from categorical to datetime

    return df


def _get_last_monday(date):
    """ Get the most recent monday before the input date """

    delta = pd.Timedelta(-1, 'd')
    while True:
        day = date.day_name()

        if day == 'Monday':
            break
        else:
            date += delta

    return date


# =============================================================================
# --- Plotting
# =============================================================================

def plot_new_aggregate(agg, startDate, endDate, ax):

    """ Plot 10 worst AHUs or any over cutoff hours """

#    agg['Standard'] = standard.sum(axis=0)

    # Plot stacked and overlapped bars
    if ax is None:
        _, ax = plt.subplots(figsize=(10, 5))
    else:
        pass

    plotGenre = 2

    if plotGenre == 2:
        # All bars stacked on the y=0 line
        agg[['Over Hours', 'On Hours', 'Off Hours', 'Under Hours']].plot(
                kind='bar', width=0.6, fontsize=11, align='center',
                ax=ax, color=['#f7022a', 'g', 'k', '#01a1a2'], stacked=True)

    if plotGenre == 4:
        # Bars spanning the y=0 line
        agg[['Over Hours', 'On Hours']].plot(
                kind='bar', width=0.6, fontsize=11, align='center',
                ax=ax, color=['#f7022a', 'g'], stacked=True)

        (-agg[['Under Hours', 'Off Hours']]).plot(
                kind='bar', width=0.6, align='center', stacked=True,
                ax=ax, color=['#01a1a2', 'k'])

    ax.legend(bbox_to_anchor=(1.01, 0.8), fontsize=11)
    ax.set_ylabel('Total Hours', size=14)
    titleStr = 'Stacked run hours\n{} - {}'.format(startDate, endDate)
    ax.set_title(titleStr, size=14)


def plot_time_aggregate(df, ax=None):
    """ Group the mismatches using pivot and plot to show the problem
    days/times """

    cmap = mpl.colors.ListedColormap([
        'xkcd:magenta',
        'xkcd:dark lavender',
        'xkcd:light forest green',
        'xkcd:tangerine',
        'xkcd:orangered',
        'xkcd:true blue',
        'xkcd:cerulean blue'])

    mypy.pivot_and_plot(df,
                        ax=ax,
                        split=[],
                        stack=['dayofweek'],
                        xaxis=['hour'],
                        aggFun='mean', splitSensors=True,
                        figSize=figureSize, ylims=(-1.2, 1.2),
                        customColors=cmap, lineStagger=True,
                        plotType='line', interpTime='all',
                        ylabel='Hour Mismatch',
                        saveFigs=False)


def pdf_reporter(diff, result, archive, startDate, endDate):
    """ Can this even work?
    plot 10 aggregates, then plot the 10 breakdowns
    or maybe I should do 12, and 2 x 3 then 2 x 3

    for 0:12n:
        group = 0:12n
        subplot 2x3
        page 1 then page 2
        for g in group:
            plot agg 1-6, then 6 to 12

    """

    # -----
    # Setup
    # -----
    pdfWriter = PdfPages("../monitor output/Run Status Report "
                         "{}.pdf".format(endDate))

#    agg = aggregate_mismatches(diff)

    diff = diff[result.T.columns]  # reorder the diff df based on .sum()

    chunkSize = 12
    plotsOnPage = 3

    totalLength = len(diff.columns)
    pages = round(chunkSize / plotsOnPage)

    if totalLength % chunkSize == 0:
        repeatLength = floor(totalLength / chunkSize)
    else:
        repeatLength = floor(totalLength / chunkSize) + 1

    print(f'Expect {repeatLength} repetitions to fit {totalLength} AHUs with '
          f'{chunkSize} AHU per group and {pages} intermediate pages with '
          f'{plotsOnPage} plots per page')

    # --------
    # Plotting
    # --------

    for i in range(repeatLength):
        # make subplot for plot_aggregate
        ax1 = plt.subplot(111)

        # Split data apprpriately
        try:
            plotDf = diff[diff.columns[i*chunkSize:(i+1)*chunkSize]]
        except IndexError:
            # Catch error where [i*n:(i+1)*n] is larger than N
            plotDf = diff[diff.columns[i*chunkSize:totalLength]]

        # Save plot to PDF
        plot_new_aggregate(result.loc[plotDf.columns], startDate, endDate,
                           ax=ax1)

        plt.savefig(pdfWriter, format='pdf', bbox_inches='tight')
        plt.close()  # should this be here?

        # Populate Subplots on following pages
        columnCounter = -1
        for _j in range(pages):
            # Make subplot of rows:plotsOnPage x cols:2
            f, axes = plt.subplots(
                    plotsOnPage, 2, figsize=(figureSize[0],
                                             figureSize[1]*plotsOnPage)
                    )

            # populate subplot
            rowCounter = -1  # counts the number of plots to index the axis
            for _k in range(plotsOnPage):
                columnCounter += 1
                rowCounter += 1
                col = plotDf.columns[columnCounter]

                print('row in page', rowCounter, '; col in df', columnCounter)

                # Draw weekly subplot on col 0
                mypy.pivot_and_plot(
                    plotDf[col].to_frame(),
                    ax=axes[rowCounter, 0],
                    split=[],
                    stack=['dayofweek'],
                    xaxis=['hour'],
                    aggFun='mean', splitSensors=True,
                    figSize=figureSize, ylims=(-1.2, 1.2),
                    customColors=cmap, lineStagger=True,
                    plotType='line', interpTime='all',
                    ylabel='Hour Mismatch',
                    saveFigs=False,
                    showPlots=False)

                # Draw historical subplot on col 1
                archive[col].plot(
                    title='Historic Over Hours\n{}'.format(col),
                    figsize=figureSize, ax=axes[rowCounter, 1])
                axes[rowCounter, 1].set_ylabel('Over Hours')

            # Save page to PDF writer
            plt.savefig(pdfWriter, format='pdf', bbox_inches='tight')
            plt.close()

    # Close PDF
    pdfWriter.close()

# =============================================================================
# --- Constants
# =============================================================================


xLim = (35, 99)  # OAT x-axis limits (drop 100 for cleaner x axis)
figureSize = (10, 8)  # size of individial plot, larger plot is 2x bigger
textSize = 14  # Text size for tick marks etc
headingSize = 16  # text size for plot title and axes titles
dateFmt = '%Y-%m-%d'

cmap = mpl.colors.ListedColormap([
    'xkcd:magenta',
    'xkcd:dark lavender',
    'xkcd:light forest green',
    'xkcd:tangerine',
    'xkcd:orangered',
    'xkcd:true blue',
    'xkcd:cerulean blue'])

# =============================================================================
# ---Main Call
# =============================================================================

# if __name__ == "__main__":


def generate_report(numWeeksAgo=12):
    todaysDate = datetime.datetime.now()

    # TODO do we have this date range automatically update?
    # Normal Operation
    endDate = _get_last_monday(pd.to_datetime(todaysDate))  # last Monday
    startDate = endDate + pd.Timedelta(-7 * numWeeksAgo, 'd')  # x weeks ago
    weekAgo = endDate + pd.Timedelta(-7, 'd')  # 7 days before last mon

    # Date formatting
    endDate = endDate.strftime(dateFmt)
    startDate = startDate.strftime(dateFmt)
    weekAgo = weekAgo.strftime(dateFmt)

    # Load standard
    standard = load_standard_schedules()
    standard = inflate_schedules(standard)

    # PI data section
    tags = list(standard.columns)
    piData = pull_pi_status(tags, start=startDate, end=endDate)

    # Old Aggregation
    combined = combine_standard_pi(piData, standard)
    diff = find_difference_in_columns(combined, standard.columns)

    shortCombined = combined[weekAgo:endDate]
    # New Aggregation
    typeAgg = type_aggreagte_mismatches(shortCombined, standard.columns)
    results = aggregate_results(typeAgg, divisor=4)  # Divisor = 4 for 15min

    history = generate_history(diff)

    pdf_reporter(diff, results, history, weekAgo, endDate)

    print('Complete !')
    return diff


if __name__ == "__main__":
    A = generate_report()
