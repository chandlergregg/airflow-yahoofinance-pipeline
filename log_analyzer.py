import glob
import sys

def parse_file(filename):
    """
    Parses file for error messages in logs

    Args: 
        filename
    Returns:
        error_count: count of error messages in file 
        error_msgs: list of error messages
    """

    # Initialize return vals
    error_count = 0
    error_msgs = []
    
    with open(filename, 'r') as file:
        for line in file:
    
            # Try to find error message and locate index in string
            str_to_find = 'error -'
            str_idx = line.lower().find(str_to_find)
            
            # If error is found, extract and increment count
            if str_idx != -1:
                
                error_count += 1
                str_start = str_idx + len(str_to_find) + 1
                error_msg = line[str_start:].strip()
                error_msgs.append(error_msg)
    
    return error_count, error_msgs

# Get directory from command line to get list of files
directory = sys.argv[1]
glob_str = f'{directory}/**/*.log'
files = [ name for name in glob.glob(glob_str, recursive=True) ]

# Go through files and parse each one; print results
for file in files:
    cnt, errors = parse_file(file)
    print(f'File: {file} | Error count: {cnt}')
    if cnt > 0:
        print('Errors: --------------------------------------------------')
        print(*errors)
