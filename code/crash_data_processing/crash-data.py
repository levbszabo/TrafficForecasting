#extracting relevant Motor Vehicle Crashes 
from datetime import datetime
#datetime_object = datetime.strptime('01/01/2019', '%b/%d/%Y')
crash_df = pd.read_csv("Motor_Vehicle_Collisions_-_Crashes.csv")
crash_df = crash_df[[int(c.split("/")[-1])>= 2019 for c in crash_df["CRASH DATE"].values]]
crash_data = crash_df.groupby("CRASH DATE").size()
crash_data = pd.DataFrame(crash_data)
crash_data.columns = ["crashes"]
dates = [datetime.strptime(date, "%m/%d/%Y") for date in crash_data.index.values]
crash_data.insert(0,"ds",dates)
crash_data = crash_data.set_index(pd.DatetimeIndex(crash_data["ds"]))
crash_data = crash_data.sort_index()
#crash_data = crash_data[crash_data.index<"04/01/2020"]
#crash_data.to_csv("crash_data.csv")
crash_data["crashes"].plot()