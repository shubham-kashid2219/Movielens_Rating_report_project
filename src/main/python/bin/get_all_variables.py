import os

# Set environment variable
os.environ['envn'] = "TEST"
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'
# Get environment variable
envn = os.environ['envn']
header = os.environ['header']
inferSchema = os.environ['inferSchema']
# Set other variables
appName = "MovieLens Research Project"
current_path = os.getcwd()
staging_dimension = current_path + '\..\staging\dimension'
staging_fact = current_path + '\..\staging\\fact'

output_path = current_path + '\..\output\\pycharm_report'