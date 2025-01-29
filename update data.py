import shutil
import os

previous_data_file_path = "C:\\Users\\admin\\OneDrive - TestPerform\\Desktop\\pyspark_cicd\\code\\data\\prev_data.csv"
current_data_file_path = "C:\\Users\\admin\\OneDrive - TestPerform\\Desktop\\pyspark_cicd\\code\\data\\curr_data.csv"



# If prev_file exists, remove it
if os.path.exists(previous_data_file_path):
    os.remove(previous_data_file_path)

# Overwrite prev_file with curr_file
shutil.copy(current_data_file_path, previous_data_file_path)

print(f"Successfully overwritten {previous_data_file_path} with {current_data_file_path}")
