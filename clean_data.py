import os
import pandas as pd
from typing import List

def current_dir(folder_name: str) -> str:
  return os.path.abspath(folder_name)

def concat_path(folder_path: str, file_name: str) -> str:
  return os.path.join(folder_path, file_name)

def rename_column(dataframe: pd.DataFrame, old_name: str, new_name: str) -> None:
  dataframe.rename(columns={old_name: new_name}, inplace=True)

def drop_column(dataframe: pd.DataFrame, column_name: str) -> None:
  dataframe.drop(columns=[column_name], inplace=True, errors='ignore')

def sec_to_min(seconds: float) -> str:
  minutes = seconds // 60
  remaining_seconds = seconds % 60
  return f"{int(minutes)}:{int(remaining_seconds):02}"

def convert_activity(activity: str) -> str:
  return activity.lower()

def edit_date_format(date: str):
  date_split = date.split(' ')
  del date_split[3]
  new_date_format = ' '.join(date_split)

  return new_date_format.strip()

def get_csv_files(folder_name: str) -> List[str]:
  running_folder_path = current_dir(folder_name)
  files_in_running_folder = os.listdir(running_folder_path)
  csv_files = sorted([concat_path(running_folder_path, file) for file in files_in_running_folder if file.endswith('.csv')])
  return csv_files

running_folder = 'running'
output_folder = 'clean-data'

if not os.path.exists(output_folder):
  os.makedirs(output_folder)

running_csv_files = get_csv_files(running_folder)

for csv_file in running_csv_files:
  df = pd.read_csv(csv_file)

  df = df[df['Distance(km)'] != 0]

  rename_column(df, 'Active energy burned(kcal)', 'Energy (kcal)')
  rename_column(df, 'Heart rate: Average(count/min)', 'Heart rate: Average(min)')
  rename_column(df, 'Heart rate: Maximum(count/min)', 'Heart rate: Maximum(min)')

  drop_columns = [
    'Heart rate zone: A Easy (<115bpm)(%)',
    'Heart rate zone: B Fat Burn (115-135bpm)(%)',
    'Heart rate zone: C Moderate Training (135-155bpm)(%)',
    'Heart rate zone: D Hard Training (155-175bpm)(%)',
    'Heart rate zone: E Extreme Training (>175bpm)(%)',
    'Elevation: Ascended(m)',
    'Elevation: Maximum(m)',
    'Elevation: Minimum(m)',
    'METs Average(kcal/hr·kg)',
    'Weather: Humidity(%)',
    'Weather: Temperature(degC)',
  ]
  for column in drop_columns:
    drop_column(df, column)

  df['Pace(sec)'] = df['Duration(s)'] / df['Distance(km)']
  df['Pace(min)'] = df['Pace(sec)'].apply(sec_to_min)
  df['Activity'] = df['Activity'].apply(convert_activity)
  df['Date'] = df['Date'].apply(edit_date_format)
  df['Duration(min)'] = df['Duration(s)'].apply(sec_to_min)

  column_order = [
    'Date', 
    'Energy (kcal)', 
    'Activity', 
    'Distance(km)', 
    'Duration(min)', 
    'Pace(min)', 
    'Heart rate: Average(min)', 
    'Heart rate: Maximum(min)'
  ]
  df = df[column_order]

  output_file = concat_path(output_folder, os.path.basename(csv_file))
  df.to_csv(output_file, index=False)

print("New CSV files have been created in the 'clean-data' folder.")
