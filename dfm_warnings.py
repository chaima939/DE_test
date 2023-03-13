import json
import pandas as pd

# load the parquet file into a pandas dataframe
f ='C:/2023 DE_case_dataset.gz.parquet'
df = pd.read_parquet(f, engine='pyarrow')

# load the json strings into dictionaries
def load_json(x):
    if pd.isnull(x):
        return None
    else:
        return json.loads(x)

# apply the load_json function to column holes to convert data from string to list of dictionaries
df['holes'] = df['holes'].apply(load_json)

# extract the metrics (length/radius) values from a dictionary and store in a list
def get_metrics(x, column_name):
    if x is not None:
        metrics_list = []
        for item in x:
            c = item.get(column_name)
            if c is not None:
                metrics_list.append(c)
        if len(metrics_list) > 0:
            return metrics_list
        else:
            return None
    else:
        return None

# apply the get_length function to length and radius
hole_length = df['holes'].apply(get_metrics, column_name='length')
hole_radius = df['holes'].apply(get_metrics, column_name='radius')

# add new columns: iterate through hole list and check if one of the holes has unreachable warning. If yes, the whole row will have
# has_unreacable_hole_warning = true. Same logic with hole error. We assume that for parts having multiple holes, if one of the holes is unreachable then the part will have unreachable error/warning 
df['has_unreachable_hole_warning'] = ''
df['has_unreacheable_hole_error'] = ''
for i in range(len(hole_length)):
    if hole_length[i] is None and hole_radius[i] is None:
        df.loc[i,'has_unreachable_hole_warning'] = 'None'
        df.loc[i,'has_unreacheable_hole_error']= 'None'
    else:
        w = []
        e = []
        for j in range(len(hole_length[i])):
            if hole_length[i][j] > (hole_radius[i][j] * 2 * 10):
                w.append('True')    
            else:
                w.append('False')
            if hole_length[i][j] > (hole_radius[i][j] * 2 * 10):
                e.append('True')  
            else:
                e.append('False')
        if 'True' in w:
            df.loc[i,'has_unreachable_hole_warning'] = 'True'
        else:
            df.loc[i, 'has_unreachable_hole_warning'] = 'False'
        if 'True' in e:
            df.loc[i,'has_unreacheable_hole_error'] = 'True'
        else:
            df.loc[i,'has_unreacheable_hole_error'] = 'False'

# save results in a csv file
file_name = 'results.csv'
df.to_csv(file_name, encoding='utf-8', index=False)

# basic  insights on the size, structure and volume of data
print(f"The data has {df.shape[0]} rows and {df.shape[1]} columns.")
print(f"The data types are:\n{df.dtypes}")
print(f"The number of missing values in each column are:\n{df.isna().sum()}")
