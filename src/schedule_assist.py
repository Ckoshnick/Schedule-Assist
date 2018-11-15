# -*- coding: utf-8 -*-
"""
Created on Mon Jul  9 09:01:44 2018

@author: koshnick

Inital outline
# Done - Open kalidah report
# Done - parse kalidah report
# Done - Open AHU inventory
# Done - Open Siemens schedule report (find a way to get this in easy)
# Done - Parse siemens schedule report
# Done - compare schedule report to kalidah to see which AHUs need changing
# Done - Remove items from parsed kalidah report based on main doc


V 1.0 (updated 7/10/2018)
-------------------------
Features
Reads data from a Kalidah report, Siemens Schedule report, and AHU inventory
to generate a list of Siemens Schedules that need modifying

Generates a cleaned version of the Kalidah report.

known issues:
    - none

Todo:
    - Create a function which captures all of the events of a specific day and
    appends them to the final report
    - Add the raw Kalidah report to a new sheet in the final output

V 1.1 (updated 08/09/2018)
-------------------------
Additions
- Final report is dropped into its own 'report' folder
- Siemenes reports are used to capture the date ranges for the report
-- (no more manual input)
- Grabs Kalidah AC report from web using date ranges
- Option to have the siemens report moved to a storage folder so that we can
-- easily keep all old files, and not have to manage moving them
- Changed raw Report to show mising data elements by changing merge from inner
-- to outer in merge_kalidah_inventory
- removed main() functions and replaced it with generate_report() to be more
-- descriptive

V 1.2 (updated 08/16/2018)
-------------------------
Additions
- Moved code to GitHub -- Check there for changes
https://github.com/Ckoshnick/Schedule-Assist

"""

import os
from os import path
import pandas as pd
import datetime as dt
import requests

# =============================================================================
# --- Global Constants
# =============================================================================

timeFmt = "%H:%M"
dateFmt = "%Y-%m-%d"
reportPath = "report output"
dataSrc = 'siemens schedule input'

# =============================================================================
# --- Functions
# =============================================================================


def get_web_report(startDate='2018-04-28',
                   endDate='2018-04-29',
                   fileType='Excel'):

    """ Request the Kalidah AC report from the Campus Event webpage """

    splitStart = startDate.split('-')
    splitEnd = endDate.split('-')

    data = {'StartYear': splitStart[0],
            'StartMonth': splitStart[1],
            'StartDate': splitStart[2],

            'endYear': splitEnd[0],
            'endMonth': splitEnd[1],
            'endDate': splitEnd[2],

            'FileType': fileType,
            'RepType': 'Rep',
            'Submit': 'Submit'}

    url = ("http://kalidah.ucdavis.edu/Reports/ACReport/"
           "ACReport.cfm?RequestTimeout=400")

    r = requests.post(url, allow_redirects=True, data=data)
    html = r.content

    return html


def parse_kalidah(html):
    """
    Parses kalidah report from its standard (read:ugly) html format into a
    series of nested dictionaries that can be converted in to a pd.DataFrame
    """

    lines = html.decode('utf-8').split('\r')

    # Setup Variables - containers
    bigDict, dateIndex, dates = {}, [], []
    # - counters
    uniqueId = 0

    # Initial Parse to find date sections
    for i, line in enumerate(lines):

        # Remove tabs and newline charachters
        lines[i] = line.strip('\t\n\\ ')

        # Ignore the heading section, hard coded later
        if i < 12:
            lines[i] = ""
            continue

        # Each unique date heading starts with "<th>"
        if line.find('<th>') > -1:
            nextDate = line.replace("<th>", "").replace("</th>", "")
            dateIndex.append(i)
            dates.append(nextDate)

    # parse each date section, by finding all row sections
    for d, dIndex in enumerate(dateIndex):
        # Allow to run to end of list, and not drop last section
        try:
            dateSection = lines[dIndex:dateIndex[d+1]]
        except IndexError:
            dateSection = lines[dIndex:len(lines)]

        # Remove tabs and newline charachters
        date = dates[d].strip(' \t')

        # Find all row sections inside of this date section
        rowIndex = []
        for j, row in enumerate(dateSection):
            # all sections are split by a "<tr>" field
            if row == "<tr>":
                rowIndex.append(j)

        # Parse through row sections add to Dictionary
        for r, rIndex in enumerate(rowIndex):
            # Allow to run to end of list, and not drop last section
            try:
                section = dateSection[rIndex:rowIndex[r+1]]
            except IndexError:
                section = dateSection[rIndex:len(dateSection)]

            counter = 0  # counter allows the columns to follow the same name
            uniqueId += 1  # makes sure no collisions happen when df is made
            sectionDict = {'Date': date}  # start each section with the date

            # Parse through the section (~6 elements, and add to {section})
            for item in section:
                # add each row to Dict
                if item.find("<td>") > -1:
                    addRow = item.replace("<td>", "").replace("</td>", "")
                    counter += 1
                    sectionDict[counter] = addRow
                else:
                    pass

            # Collect all sections into larger dictionary
            bigDict[uniqueId] = sectionDict

    # Make df
    # Columns from the Kalidah report
    columns = ['Date', 'Facility', 'Building', 'Room Number', 'New Start',
               'New End', 'Name of Reservation']
    df = pd.DataFrame(bigDict).T
    df.columns = columns
    df.set_index('Date', inplace=True)
    df.index = pd.to_datetime(df.index)

    df['New Start'] = pd.to_datetime(df['New Start'])
    df['New End'] = pd.to_datetime(df['New End'])

    return df


def grab_siemens_report():
    """ Open the siemens schedule report input folder and find the first
    file's path
    """

    files = os.listdir(path.join("..", dataSrc))
    files = [x for x in files if x.find('.csv') >= 0]

    if len(files) == 0:
        raise ValueError("siemens schedule input contains no .csv files!")
    elif len(files) > 1:
        print("Warning: Found multiple files, choosing {}!".format(files[0]))
        fileName = files[0]
    else:
        print("Loading in file {}".format(files[0]))
        fileName = files[0]

    relativePath = path.join("..", dataSrc, fileName)

    return relativePath


def parse_siemens_schedule(fileName):
    """ Open the siemens schedule report and parse the data into a pd.DataFrame
    """

    with open(fileName, 'r') as f:
        lines = f.readlines()

    # Setup Variables - containers
    bigDict, dateIndex, dates = {}, [], []
    # - counters
    uniqueId = 0

    # Initial Parse to find date sections
    for i, line in enumerate(lines):

        # Remove tabs and newline charachters
        lines[i] = (
            line.strip('\n')
            .replace('"', "")
            .replace('<<', '00:00')
            .replace('>>', '23:59'))

    # Ignore the heading section, hard coded later
    splitLines = []
    for line in lines:
        splitLines.append(line.split(','))

    dateList = ['Monday', 'Tuesday', 'Wednesday', 'Thursday',
                'Friday', 'Saturday', 'Sunday']

    dateIndex = []
    dates = []

    for i, line in enumerate(splitLines):
        if line[0] in dateList:
            newDate = ''.join(line[0:3]).replace(':', '')
            print(newDate)

            dateIndex.append(i)
            dates.append(newDate)

    for d, dIndex in enumerate(dateIndex):
        # Allow to run to end of list, and not drop last section
        try:
            dateSection = splitLines[dIndex:dateIndex[d+1]]
        except IndexError:
            dateSection = splitLines[dIndex:len(splitLines)]

        # Remove tabs and newline charachters
        date = dates[d]  # strip stuff?

        # Find all row sections inside of this date section
        for j, row in enumerate(dateSection):
            counter = 0
            uniqueId += 1
            section = {'Date': date}
            # all sections are split by a "<tr>" field
            if "(OVR)" in row:
                # fix OVR
                row.remove("(OVR)")
            else:
                pass

            for item in row:
                counter += 1
                section[counter] = item

            # Collect all sections into larger dictionary
            bigDict[uniqueId] = section

    # Turn into df and clean up
    columns = ['Date', 'Type', 'Siemens Schedule', 'blank',
               'enabled', 'Current Start', 'Current End']
    df = pd.DataFrame(bigDict).T
    df.columns = columns
    df = df[['Date', 'Siemens Schedule', 'enabled',
             'Current Start', 'Current End']]
    df.set_index('Date', inplace=True)
    df.index = pd.to_datetime(df.index)
    # Format time numbers properly (warning: converts to strings)
#    df['Current Start'] = pd.to_datetime(
#            df['Current Start'].str.strip(' ')).dt.strftime(timeFmt)
#    df['Current End'] = pd.to_datetime(
#            df['Current End'].str.strip(' ')).dt.strftime(timeFmt)

    df = df[df['enabled'] == 'Enabled']

    df['Current Start'] = pd.to_datetime(df['Current Start'].str.strip(' '))
    df['Current End'] = pd.to_datetime(df['Current End'].str.strip(' '))

    df = remove_short_schedules(df)

    df['Current Start'] = df['Current Start'].dt.strftime(timeFmt)
    df['Current End'] = df['Current End'].dt.strftime(timeFmt)

    return df


def remove_short_schedules(df, timeDiff=1800):
    """ if a schedule in siemens is less than timeDiff seconds, then remove it
    from the report before it is processed """

    df['difftime'] = df['Current End'] - df['Current Start']
    df['difftime'] = df['difftime'].apply(pd.Timedelta.total_seconds).abs()

    df = df[df['difftime'] > timeDiff]

    df = df.drop('difftime', axis=1)
    return df


def adjust_Kalidah_start(kalidah, deltaT='-1h'):
    """ Pulls the start of the kalidah events back by deltaT, this makes it so
    the user does not have to subtract time in their head to allow for warmup
    """

    df = kalidah.copy()

    df.loc[df['New Start'].dt.hour > 0, 'New Start'] += pd.Timedelta(deltaT)

    return df


def load_exceptions(kalidah, filename):

    exceptions = pd.read_excel(filename, index_col=0, parse_dates=True)

    def strf(t):
        return dt.time.strftime(t, timeFmt)

    exceptions['New Start'] = exceptions['New Start'].apply(strf)
    exceptions['New End'] = exceptions['New End'].apply(strf)
##                               infer_datetime_format=False)
#    return exceptions

    exceptions['New Start'] = pd.to_datetime(exceptions['New Start']).apply(pd.Timestamp)
    exceptions['New End'] = pd.to_datetime(exceptions['New End']).apply(pd.Timestamp)

    exceptions = adjust_Kalidah_start(exceptions)

    combined = pd.concat([kalidah, exceptions], axis=0, sort=False).sort_index()

    return combined


def merge_kalidah_inventory(kalidah, inventory):
    """
    Matches Facilities names in kalidah report with Siemens Schedule names that
    are stored in the AHU inventory file (manually generated)

    """
    merged = kalidah.reset_index().merge(inventory, how="outer",
                                         on='Facility').set_index('Date')

    # Check for facilities that are missing in AHU_inventory
    missing = merged[(merged['Siemens Schedule'].isnull())
                     & ~(merged['Facility'].isnull())]

    if not missing.empty:
        print("There is a missing Facility in AHU inventory!!")
        print(missing)

    return merged, missing


def expand_kalidah_groups(merged, col, splitString=','):
    """ Takes in a mergred inventory kalidah df and expands any items that
    has multiple siemens schedules listed. The different schedules must be
    comma delimited """

    merged = merged.loc[pd.notnull(merged.index)]

    for index, row in merged.iterrows():

        if isinstance(row[col], str):
            pass
        else:
            continue

        if row[col].find(splitString) > 0:

            for item in row[col].split(splitString):

                newRow = row
                newRow[col] = item.strip()

                merged = merged.append(newRow)

    merged = merged[merged['Siemens Schedule'].str.contains(",") == False]

    return merged


def multi_merge(left, right, keys):

    """
    Taken form the following link
    http://pandas-docs.github.io/pandas-docs-travis/merging.html#merging-join-on-mi

    which results from the following discussion
    https://github.com/pandas-dev/pandas/issues/3662
    """

    # Return multi Index on dates
#    result = pd.merge(left.reset_index(),
#                      right.reset_index(),
#                      on=keys,
#                      how='outer').set_index(keys)

    # DO NOT Return multi Index on dates
    result = pd.merge(left.reset_index(),
                      right.reset_index(),
                      on=keys,
                      how='outer')

    return result


def reduce_report(df):
    """
    Removes extraneous columns by leaving them out of the groupby by=() arg
    Also makes sure to aggregate the times as mins and maxs depending on if it
    is an start time(min) or end time(max)
    """

    if 'Current Start' in df.columns:

        f = {'New Start': min,
             'New End': max,
             'Current Start': min,
             'Current End': max,
             'Name of Reservation': lambda x: ', '.join(x),
             'Facility': lambda y: ', '.join(y)
             }

    else:

        f = {'New Start': min,
             'New End': max,
             # 'Current Start': min,
             # 'Current End': max,
             'Name of Reservation': lambda x: ', '.join(x)
             }

    df = df.groupby(by=['Date', 'Building', 'Siemens Schedule']).agg(f)

    return df


def compare_times(df):
    """
    Create two new columns that signal whether the start times are equal and
    whether the end times are equal
    """
    df['Change Start'] = df['New Start'] != df['Current Start']
    df['Change End'] = df['New End'] != df['Current End']

    return df


def extend_only_logic(df):
    """
    Create two new columns that signal whether the special event start times
    are outside of the normal schedule bounds. This method saves the LEAST
    amount of energy
    """

    df['Change Start'] = df['New Start'] < df['Current Start']
    df['Change End'] = df['New End'] > df['Current End']

    return df


def color_changer(date):

    month = date.month

    # Change color :: New Color
    win = ('#B2E3E8', "#0077ff")
    spr = ('#B2E3E8', '#DBE6AF')
    summ = ('#E87F60', '#C2C290')
    fall = ('#FADDAF', '#EB712F')

    colorDict = {1: win, 2: win,
                 3: spr, 4: spr, 5: spr,
                 6: summ, 7: summ, 8: summ,
                 9: fall, 10: fall, 11: fall,
                 12: win}

    return colorDict[month]


def save_function(df, kalidah, missing):
    """
    Package the final df output into a nice excel file
    """

    # Setup Name vars
    currentTime = dt.datetime.now().strftime("%Y-%m-%d %H_%M")
    outputFilename = "SA_output {}.xlsx".format(currentTime)

    outputFilename = path.join("..", reportPath, outputFilename)

    # Initate write to save df
    writer = pd.ExcelWriter(outputFilename,  datetime_format='yyyy-MM-dd')
    workbook = writer.book

    color1, color2 = color_changer(dt.datetime.now())

    format1 = workbook.add_format({"bg_color": color1})
    format2 = workbook.add_format({"bg_color": color2})

    # Filter page 1 to only times that need changing

    searchFor = ['Can not schedule', 'This space is 24/7', ',']

    noScheduleMask = (
            (~df['New Start'].isna())
            & (df['Current Start'].isna())
            & ~df.index.get_level_values('Siemens Schedule').str.contains(
                    '|'.join(searchFor))
            )

    filtered = df[(df['Change Start']) | (df['Change End'] | noScheduleMask)]

    if filtered.empty:
        print("Warning: filtered dataframe is empty! No changes needed!")
        filtered.loc['empty', df.columns] = 0

    # Sort Date - Siemens Schedule
    filtered = filtered.sort_index(level=[0, 2], sort_remaining=False)

    filtered.to_excel(writer, 'changes')

    # Grab worksheet to make formatting changes
    worksheet = writer.sheets["changes"]  # pull worksheet object
    # Set column widths

    worksheet.conditional_format("D2:E1000",
                                 {"type": "formula",
                                  "criteria": '=(J2=TRUE)',
                                  "format": format1
                                  }
                                 )

    worksheet.conditional_format("D2:E1000",
                                 {"type": "formula",
                                  "criteria": '=AND(NOT(ISBLANK(D2)), ISBLANK(F2))',
                                  "format": format2
                                  }
                                 )
#    worksheet.conditional_format("E2:E1000",
#                             {"type": "formula",
#                              "criteria": '=($J2=TRUE)',
#                              "format": format1
#                             }
#                             )

    # Grab worksheet to make formatting changes
    # Set column widths
    worksheet.set_column(0, 0, 15)  # set date column width
    worksheet.set_column(2, 2, 28)  # set schedule column width
    worksheet.set_column(3, 7, 11)  # set remaining column widths
    worksheet.set_column(7, 7, 20)  # set Name of reservation
    worksheet.set_column(8, 8, 13)  # set change start
    worksheet.set_column(9, 9, 13)  # set change end

    # save raw dataframe to raw tab
    df.to_excel(writer, 'raw')

    worksheet2 = writer.sheets["raw"]  # pull worksheet object
    worksheet2.set_column(0, 0, 15)  # set date column width
    worksheet2.set_column(2, 2, 28)  # set schedule column width
    worksheet2.set_column(3, 7, 11)  # set remaining column widths
    worksheet2.set_column(7, 7, 20)  # set Name of reservation
    worksheet2.set_column(8, 8, 13)  # set change start
    worksheet2.set_column(9, 9, 13)  # set change end

    kalidah.to_excel(writer, 'Kalidah')
    worksheet3 = writer.sheets["Kalidah"]  # pull worksheet object
    worksheet3.set_column(0, 0, 15)  # set date column width
    worksheet3.set_column(1, 1, 28)  # set Facility width
    worksheet3.set_column(2, 5, 11)  # set remaining column widths
    worksheet3.set_column(6, 6, 28)  # set Name of reservation

    missing.to_excel(writer, 'missing')
    worksheet3 = writer.sheets["missing"]  # pull worksheet object
#    worksheet3.set_column(0, 0, 15)  # set date column width
#    worksheet3.set_column(1, 1, 28)  # set Facility width
#    worksheet3.set_column(2, 5, 11)  # set remaining column widths
#    worksheet3.set_column(6, 6, 28)  # set Name of reservation

    # Clean up
    writer.save()
    print('Saving Assistant report as {}'.format(outputFilename))
    writer.close()


def move_siemens_report(reportPath):
    """ Move the file that was loaded from input folder to output folder """
    newPath = reportPath.replace("input", "output")
    print("Moving {} to {}".format(reportPath, newPath))
    os.rename(reportPath, newPath)

# =============================================================================
# --- TEST FUNCTIONS
# =============================================================================

# =============================================================================
# --- MAIN
# =============================================================================


def generate_report(moveSiemens=False, exception_file=None):
    # Grab siemens report
    siemensPath = grab_siemens_report()
    siemens = parse_siemens_schedule(siemensPath)

#    return siemens

    # find dates
    startDate = siemens.index[0].strftime(dateFmt)
    endDate = siemens.index[-1].strftime(dateFmt)

    # pull kalidah report with dates
    print("\nWorking...\n")
    html = get_web_report(startDate=startDate,
                          endDate=endDate)

    kalidah = parse_kalidah(html)
    kalidah = adjust_Kalidah_start(kalidah)

    if exception_file:
        kalidah = load_exceptions(kalidah, exception_file)

    kalidah['New Start'] = kalidah['New Start'].dt.strftime(timeFmt)
    kalidah['New End'] = kalidah['New End'].dt.strftime(timeFmt)
#
#    siemens['Current Start'] = siemens['Current Start'].dt.strftime(timeFmt)
#    siemens['Current End'] = siemens['Current End'].dt.strftime(timeFmt)

    # load inventory
    inventory = pd.read_excel('AHU inventory.xlsx')
    inventory = inventory.drop(['Building', 'Can Schedule',
                                'Single Unit', '24/7 space', 'Notes'], axis=1)

    # Compare reports
    df, missing = merge_kalidah_inventory(kalidah, inventory)
    df = expand_kalidah_groups(df, 'Siemens Schedule')
    df = multi_merge(df, siemens, ['Date', 'Siemens Schedule'])
    df = reduce_report(df)
    df = extend_only_logic(df)

    # Make excel - Save
    df = save_function(df, kalidah, missing)

#    return df

    if moveSiemens:
        move_siemens_report(siemensPath)

    return kalidah


if __name__ == "__main__":
    A = generate_report(moveSiemens=False, exception_file='../exceptions/vet2018.xlsx')
#    B = remove_short_schedules(A)
    pass
