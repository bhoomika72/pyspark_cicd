import shutil
import os

current_data_file_path = "C:\\Users\\admin\\OneDrive - TestPerform\\Desktop\\cicd\\testpyspark\\curr_data.csv"
previous_data_file_path = "C:\\Users\\admin\\OneDrive - TestPerform\\Desktop\\cicd\\testpyspark\\prev_data.csv"



# If prev_file exists, remove it
if os.path.exists(previous_data_file_path):
    os.remove(previous_data_file_path)

# Overwrite prev_file with curr_file
shutil.copy(current_data_file_path, previous_data_file_path)

print(f"Successfully overwritten {previous_data_file_path} with {current_data_file_path}")
