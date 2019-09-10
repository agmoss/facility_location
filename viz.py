import folium
from folium import plugins
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import sklearn
import tkinter


class LocationMap:
    """This class contains functionality 
    for adding data elements to a folium map"""

    def __init__(self, name, fmap):
        self.name = name
        self.fmap = fmap

    @staticmethod
    def html_popup(code1, code2, code3, code4):
        """Add custom HTML markers to the folium popup boxes."""

        html1 = """\
        <html>
        <head></head>
        <body>
            <h3>{code1}</h3>
            <p><br>
            <h4>{code2}</h4>
            <h4>{code3}</h4>
            <h4>{code4}</h4>
            </p>
        </body>
        </html>
        """.format(code1=code1, code2=code2, code3=code3, code4=code4)

        return html1

    def add_box_marker(self, df):
        """Add the markers to the map."""

        # mark each box as a point
        for index, row in df.iterrows():
            popup = LocationMap.html_popup(code1=row['Status'], code2=row['Lat'], code3=row['Lon'], code4=row['City'])

            folium.Marker([row['Lat'], row['Lon']]
                          , icon=folium.Icon(color='black')
                          , popup=popup

                          ).add_to(self.fmap)

        return self.fmap

    def add_branch(self, df):
        """Add the parts box markers to the map."""

        # mark each box as a point
        for index, row in df.iterrows():
            popup = self.html_popup(code1=row['Status'], code2=row['Lat'], code3=row['Lon'], code4=row['City'])

            folium.Marker([row['Lat'], row['Lon']]
                          , icon=folium.Icon(color='darkred')
                          , popup=popup
                          ).add_to(self.fmap)

        return self.fmap

    @staticmethod
    def color_producer(index):
        """Custom shader"""
        if index < 0.25:
            return 'green'
        elif 0.25 <= index < 0.75:
            return 'orange'
        else:
            return 'red'

    def status_marks(self, df):
        """Apply driving and/or working marks to the map."""

        for index, row in df.iterrows():
            # The radius of the circle grows as crossover increases
            radius = row['crossover'] / 1000

            folium.CircleMarker([row['Lat'], row['Lon']]
                                , radius=radius
                                , fill=True
                                , color=self.color_producer(row['Cross_Normal'])
                                , opacity=row['Cross_Normal']

                                , fill_color=self.color_producer(row['Cross_Normal'])
                                , fill_opacity=row['Cross_Normal']

                                ).add_to(self.fmap)

        return self.fmap

    def add_heat(self, df):
        """Add standard folium heatmap to the map object."""

        # convert to (n, 2) nd-array format for heat map
        fleet_arr = df[['Lat', 'Lon']].values

        # plot heat map
        self.fmap.add_child(plugins.HeatMap(fleet_arr, radius=10))

        return self.fmap


class DataManipulator:
    """Functions for additional data manipulation 
    and transformation. 
    rm_outlier is used for removing unnecessary high crossovers in the original data.
    normalizer creates a [0-1] continuous vector for use as a shader.
    """

    @staticmethod
    def rm_outlier(df):
        """Remove outliers from the crossover variable."""

        # keep rows that are within +3 to -3 standard deviations in the column 'Crossover'
        # df =  df[np.abs(df.Crossover-df.Crossover.mean()) <= (0.3 * df.Crossover.std())]

        # Keep rows less than x quantile
        df = df[df.crossover < df.crossover.quantile(.80)]

        return df

    @staticmethod
    def normalizer(df):
        """Normalize the crossover variable for use as a shading/sizing vector."""

        from sklearn import preprocessing

        # Create x, where x the 'scores' column's values as floats
        x = df[['crossover']]

        # Create a minimum and maximum processor object
        min_max_scaler = preprocessing.MinMaxScaler()

        # Create an object to transform the data to fit minmax processor
        x_scaled = min_max_scaler.fit_transform(x)

        # Run the normalizer on the dataframe
        df_normalized = pd.DataFrame(x_scaled)

        df_normalized.columns = ['Cross_Normal']

        return df_normalized

    @staticmethod
    def joiner(df1, df2):
        """Join two dataframes"""

        df = pd.merge(df1, df2, left_index=True, right_index=True)

        return df


# Main Methods

def custom_main():
    """Create a custom circle object map. 
    The size and color of each circle is a function of the crossover
    
    FYI This map is graphically intensive. To reconcile for this, only a small protion 
    of the data is used for plotting."""

    # Create a map object
    mp = folium.Map([52, -113], zoom_start=6)

    # Read in the data created by verizon_data.py
    df = pd.read_csv(r'Data/Sask.csv')

    # Slice data based on status
    box_df = df.loc[df['Status'] == 'box']
    branch_df = df.loc[df['Status'] == 'Branch']
    driving_df = df.loc[df['Status'] == 'Driving']

    # Remove Outliers
    driving_df = DataManipulator.rm_outlier(driving_df)

    # Take random sample of df
    small_df = driving_df.sample(frac=0.1)

    # Normalize Crossover column and join back to original df
    small_df.reset_index(inplace=True)

    norm = DataManipulator.normalizer(small_df)

    pass_df = DataManipulator.joiner(norm, small_df)

    # Create a mapping object
    data_map = LocationMap('Custom Map', mp)

    # Add the data
    tab = data_map.add_box_marker(box_df)
    tab = data_map.add_branch(branch_df)
    tab = data_map.status_marks(pass_df)

    # Save the map
    tab.save('DrivingMapSask.html')


def folium_heatmap_main():
    """Create a heatmap with foliums built in heat map capabilities"""

    # Create a map object
    mp = folium.Map([52, -113], zoom_start=6)

    # Read in the data created by verizon_data.py
    df = pd.read_csv(r'/Sask.csv')

    # Slice data based on status
    box_df = df.loc[df['Status'] == 'box']
    branch_df = df.loc[df['Status'] == 'Branch']
    driving_df = df.loc[df['Status'] == 'Driving']

    # Create mapping object
    heat_map = LocationMap('Folium Heat Map', mp)

    # Add the data to the map
    tab = heat_map.add_box_marker(box_df)
    tab = heat_map.add_branch(branch_df)
    tab = heat_map.add_heat(driving_df)

    tab.save('SaskDrivingHeatMap.html')


if __name__ == "__main__":
    print(__name__)
    folium_heatmap_main()
    custom_main()
