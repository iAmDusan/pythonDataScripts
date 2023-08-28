import os

directory = os.getcwd()  # Get the current directory

for filename in os.listdir(directory):
    if filename.endswith(".csv"):
        new_filename = filename.split(" ")[0] + ".csv"
        os.rename(os.path.join(directory, filename), os.path.join(directory, new_filename))
