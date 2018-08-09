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

known issues:
    - none

Todo:
    - none
"""


import os
from os import path
import pandas as pd
import datetime as dt
import requests


### Global Constants
timeFmt = "%H:%M"
dateFmt = "%Y-%m-%d"
reportPath = "report output"
dataSrc = 'siemens schedule input'


# =============================================================================
### Functions
# =============================================================================


def get_web_report(startDate='2018-04-28',
                   endDate='2018-04-29',
                   fileType='Excel'):

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
            sectionDict = {'Date': date} #start each section with the date

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

    df['New Start'] = pd.to_datetime(df['New Start']).dt.strftime(timeFmt)
    df['New End'] = pd.to_datetime(df['New End']).dt.strftime(timeFmt)

    return df


def grab_siemens_report():
    # Look in siemens folder
    # Grab first file you see
    # print name
    # parse file
    # Move file to used folder
    # Return dataframe

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

    with open(fileName, 'r') as f:
        lines = f.readlines()

    # Setup Variables - containers
    bigDict, dateIndex, dates = {}, [], []
    # - counters
    uniqueId = 0

    # Initial Parse to find date sections
    for i, line in enumerate(lines):

        # Remove tabs and newline charachters
        lines[i] = line.strip('\n').replace('"', "").replace('<<', '00:00').replace('>>', '23:59')

    # Ignore the heading section, hard coded later
    splitLines = []
    for line in lines:
        splitLines.append(line.split(','))

    dateList = ['Monday', 'Tuesday', 'Wednesday', 'Thurdsday',
                'Friday', 'Saturday', 'Sunday']

    dateIndex = []
    dates = []

    for i, line in enumerate(splitLines):
        if line[0] in dateList:
            newDate = ''.join(line).replace(':', '')

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
    df = df[['Date', 'Siemens Schedule', 'Current Start', 'Current End']]
    df.set_index('Date', inplace=True)
    df.index = pd.to_datetime(df.index)
    # Format time numbers properly (warning: converts to strings)
    df['Current Start'] = pd.to_datetime(df['Current Start'].str.strip(' ')).dt.strftime(timeFmt)
    df['Current End'] = pd.to_datetime(df['Current End'].str.strip(' ')).dt.strftime(timeFmt)

    return df


def merge_kalidah_inventory(kalidah, inventory):
    """
    Matches Facilities names in kalidah report with Siemens Schedule names that
    are stored in the AHU inventory file (manually generated)

    """
    merged = kalidah.reset_index().merge(inventory, how="outer",
                                         on='Facility').set_index('Date')

    return merged


def multi_merge(left, right, keys):

    """
    Taken form the following link
    http://pandas-docs.github.io/pandas-docs-travis/merging.html#merging-join-on-mi

    which results from the following discussion
    https://github.com/pandas-dev/pandas/issues/3662
    """

    result = pd.merge(left.reset_index(),
                      right.reset_index(),
                      on=keys,
                      how='outer').set_index(keys)

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
             'Name of Reservation': lambda x: ', '.join(x)
             }

    else:

        f = {'New Start': min,
             'New End': max,
             # 'Current Start': min,
             # 'Current End': max,
             'Name of Reservation': lambda x: ', '.join(x)
             }

    df = df.groupby(by=('Date', 'Building', 'Siemens Schedule')).agg(f)

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


def save_function(df, kalidah):
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
    format1 = workbook.add_format({"bg_color": "#e36bff"})

    # Filter page 1 to only times that need changing
    filtered = df[(df['Change Start'] == True) | (df['Change End'] == True)]

    if filtered.empty:
        print("Warning: filtered dataframe is empty! No changes needed!")
        filtered.loc['empty', df.columns] = 0
    filtered.to_excel(writer, 'changes')

    # Grab worksheet to make formatting changes
    worksheet = writer.sheets["changes"]  # pull worksheet object
    # Set column widths

    worksheet.conditional_format("D2:E1000",
                                 {"type": "formula",
                                  "criteria": '=(I2=TRUE)',
                                  "format": format1
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
### TEST FUNCTIONS
# =============================================================================


# =============================================================================
### RETIRED FUNCTIONS
# =============================================================================

def ensure_date(dateString):

    splitString = dateString.split('-')

    try:
        assert(len(splitString)) == 3
        assert(len(splitString[0])) == 4
        assert(len(splitString[1])) == 2
        assert(len(splitString[2])) == 2

        int(splitString[0])
        int(splitString[0])
        int(splitString[0])

    except AssertionError:
        print('Date format failure in ensure_date')
        return False

    try:
        assert(0 < int(splitString[1]) < 13)
        assert(0 < int(splitString[2]) < 32)
    except AssertionError:
        print('Date values out of range in ensure_date.'
              '0 < MM < 13 ; 0 < DD < 32')
    else:
        return True


def aggregate_reservations(kalidah):
    """
    Once kalidah is parsed, collect all of the reservations for a particular
    date and facility, then save them to append to the main output
    """

    reservations = kalidah.groupby(by=('Date','Facility')).apply(lambda x: x["Name of Reservation"])
    return reservations


def auto_load_data():
    """
    Pull data in the 'standard' way from the "data" directory
    Return kalidah, inventory and siemens documents to be processed in "main()"
    """
    """
    kalidahFile = find_files(extension='.xls', filePath=path.join(os.getcwd(),'data'))
    if len(kalidahFile) > 1:
        raise ValueError("There should only be one kalidah report in 'data' folder")

    siemensFile = find_files(extension='.csv', filePath=path.join(os.getcwd(),'data'))
    if len(siemensFile) > 1:
        raise ValueError("There should onl be one Siemens Report in 'data' folder")

    """
    print(kalidahFile, siemensFile)

    kalidah = parse_kalidah(path.join(path_prefix, 'data', kalidahFile[0]))
    inventory = pd.read_excel('AHU inventory.xlsx')
    inventory = inventory.drop('Building',axis=1)
    siemens = parse_siemens_schedule(path.join(path_prefix, 'data', siemensFile[0]))

    return kalidah, inventory, siemens

# =============================================================================
### MAIN
# =============================================================================


def generate_report(moveSiemens=False):
    # Grab siemens report
    siemensPath = grab_siemens_report()
    siemens = parse_siemens_schedule(siemensPath)
    # find dates
    startDate = siemens.index[0].strftime(dateFmt)
    endDate = siemens.index[-1].strftime(dateFmt)
    # pull kalidah report with dates
    print("\nWorking...\n")
    html = get_web_report(startDate=startDate,
                          endDate=endDate)
    # parse Kalidah
    kalidah = parse_kalidah(html)

    # load inventory
    inventory = pd.read_excel('AHU inventory.xlsx')
    inventory = inventory.drop('Building', axis=1)
    # Compare reports
    df = merge_kalidah_inventory(kalidah, inventory)
    df = multi_merge(df, siemens, ['Date', 'Siemens Schedule'])
    df = reduce_report(df)
    df = extend_only_logic(df)

    save_function(df, kalidah)

    if moveSiemens:
        move_siemens_report(siemensPath)
    # Make excel
    # save
    return kalidah


if __name__ == "__main__":

    A = generate_report(moveSiemens=False)
    pass
