import glob
import os
import time
import pandas as pd
from multiprocessing import Pool

class FrameFunctions:
    """This class houses functions for dataframe manipulation and analysis.
    These functions provide additional varaibles that will aid in the 
    analysis of driving and working related metrics."""

    @staticmethod
    def time_stamp(df):
        """Convert the date related variables to datetime type."""

        df['Time'] = pd.to_datetime(df['Time'])

        df['Date'] = df['Time'].dt.date

        df.drop(['Time'], axis=1, inplace=True)

        return df

    @staticmethod
    def key_columns(df):
        """Create the path (primary key field) and number of records field."""

        df = df.reset_index()

        df['Path_ID'] = df.index + 1

        df.drop(['index'], axis=1, inplace=True)

        return df

    @staticmethod
    def zone(df):
        """Sub census zone. Used for proximity based evaluations."""

        # Rounding the latitude and longitude is used to create the "zone"
        df['lat_rnd'] = df['Lat'].round(decimals=2)

        df['lon_rnd'] = df['Lon'].round(decimals=2)

        # Create the zone variable
        df['zone'] = df['lat_rnd'].apply(str) + '-' + df['lon_rnd'].apply(str)

        return df

    @staticmethod
    def crossover(df):
        """Calculates how many times each sub census zone was entered by a vehicle"""

        # Create the Number of Records Column (used for pivot table)
        # This column is a vector of 1's
        df['Number_of_Records'] = 1

        # Calculate how many times a zone is driven through per month
        piv = pd.pivot_table(df, index=['zone'], values='Number_of_Records', aggfunc='sum')

        # Convert the pivot table to a dataframe
        piv = pd.DataFrame(piv.to_records())

        # Rename the pivoted column to crossover (the number of times a sub census zone has been traveled)
        piv.rename(columns={'Number_of_Records': 'crossover'}, inplace=True)

        # Join this back to the original dataframe
        merged_df = pd.merge(piv, df, how='outer', on=['zone'])

        # Drop the number of records (vector of ones)
        merged_df.drop(['Number_of_Records'], axis=1, inplace=True)

        return merged_df


class Mapper:
    """Custom multiprocessing map reduce. 
    
    The use of this class for dataframe processing and manimulation greatly
    speeds up the run time."""

    @staticmethod
    def parse_date(df):
        """Convert series to datetime (map function)"""

        # df['Time'] = df['Time'].apply(pd.Timestamp)  # will handle parsing
        df['date_time'] = pd.to_datetime(df['Time'])

        # Create a simple date column
        df['date'] = df.date_time.map(lambda x: x.strftime('%Y-%m-%d'))

        return df

    @staticmethod
    def list_to_df(list_object):
        """Read excel file for each path in list item (map function)"""

        df = pd.read_excel(list_object, index_col=None, usecols="A:F,H,I,K")
        df['Duration'] = df['Duration'].astype(str)

        return df

    @staticmethod
    def parse_duration(df):
        """Convert to seconds (map function)"""

        for index, row in df.iterrows():

            try:

                hour, _min, sec = row['Duration'].split(":")
                # total_seconds = ((int(hour)* 60 + int(_min)) * 60 + int(sec))

                total_hours = int(hour) + int(_min) / 60

                df.at[index, 'Duration'] = total_hours

            except ValueError:

                df.drop(index, inplace=True)

        return df

    @staticmethod
    def mult_map(func,list_object):
        """Process a map function in parallel"""

        pool = Pool()
        mapped_list = list(pool.map(func,list_object))

        pool.close()
        pool.join()

        return mapped_list


def main():
    # Logging to keep track of time
    import logging
    logging.basicConfig(filename='app1.log', filemode='w', level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    # Program start time
    start_time = time.time()
    last_time = start_time

    # Log
    logging.info('Program start time: {} '.format(start_time))

    # File path extension
    ext = "*.xlsx"

    # Create a list of file paths for the map function
    file_list1 = glob.glob(os.path.join(r'/Data/Regina_Data', ext))
    file_list2 = glob.glob(os.path.join(r'/Saskatoon_Data', ext))

    # Log
    logging.info(
        'List of file paths created. Lap: {} Elapsed: {} '.format(time.time() - last_time, time.time() - start_time))
    last_time = time.time()

    # Convert file paths to a list of dataframes
    df_list1 = Mapper.mult_map(Mapper.list_to_df, file_list1)
    df_list2 = Mapper.mult_map(Mapper.list_to_df, file_list2)

    # Log
    logging.info(
        'List of data frames created. Lap: {} Elapsed: {} '.format(time.time() - last_time, time.time() - start_time))
    last_time = time.time()

    df_list1 = Mapper.mult_map(Mapper.parse_duration, df_list1)
    df_list2 = Mapper.mult_map(Mapper.parse_duration, df_list2)

    # Log
    logging.info(
        'Duration converted to seconds. Lap: {} Elapsed: {}'.format(time.time() - last_time, time.time() - start_time))
    last_time = time.time()

    # Parse the date series in parallel
    df_list1 = Mapper.mult_map(Mapper.parse_date, df_list1)
    df_list2 = Mapper.mult_map(Mapper.parse_date, df_list2)

    # Log
    logging.info('Dates parsed. Lap: {} Elapsed: {} '.format(time.time() - last_time, time.time() - start_time))
    last_time = time.time()

    # Concatenate the dataframe lists into one dataframe per city
    df1 = pd.concat(df_list1)
    df2 = pd.concat(df_list2)

    # Log
    logging.info(
        'Dataframes concatenated. Lap: {} Elapsed: {}'.format(time.time() - last_time, time.time() - start_time))
    last_time = time.time()

    # Keep track of the city
    df1['City'] = 'Regina'
    df2['City'] = 'Saskatoon'

    # Create one dataframe for analysis
    df = pd.concat([df1, df2], axis=0, ignore_index=True, sort=False)

    # Add the calculated fields

    # Log
    logging.info(
        'Adding calculated fields. Lap: {} Elapsed: {}'.format(time.time() - last_time, time.time() - start_time))
    last_time = time.time()

    df = FrameFunctions.key_columns(df)

    # Log
    logging.info('PK added. Lap: {} Elapsed: {}'.format(time.time() - last_time, time.time() - start_time))
    last_time = time.time()

    df = FrameFunctions.zone(df)

    # Log
    logging.info('Sub census zone added. Lap: {} Elapsed: {}'.format(time.time() - last_time, time.time() - start_time))
    last_time = time.time()

    df = FrameFunctions.crossover(df)

    # Log
    logging.info('Crossover added. Lap: {} Elapsed: {}'.format(time.time() - last_time, time.time() - start_time))
    last_time = time.time()

    # Log
    logging.info(
        'Calculated fields added. Lap: {} Elapsed: {}'.format(time.time() - last_time, time.time() - start_time))
    last_time = time.time()

    # Concatenate additional excel files to a dataframe
    def concat_additional_df(df, file_path):
        """Concatenate an additional dataframe to the one passed to this function"""

        df1 = pd.read_excel(file_path, index_col=None)

        frame = pd.concat([df, df1], axis=0, ignore_index=True, sort=False)

        return frame

    # Add the box and office location data (via excel files)
    df = concat_additional_df(df,
                              file_path=r'/Data/Other_Input_Data/box_address.xlsx')
    df = concat_additional_df(df,
                              file_path=r'/Data/Other_Input_Data/sask_branches.xlsx')

    # Write output
    write_path = r'/Facility_Location/Data'
    df.to_csv(os.path.join(write_path, r'Sask.csv'), index=False)

    # Log
    logging.info('Output written. Lap: {} Elapsed: {}'.format(time.time() - last_time, time.time() - start_time))

    # Testing
    print(df.head(100))
    print(df.columns)

    return None


if __name__ == '__main__':
    print(__name__)
    main()
