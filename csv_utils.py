import csv
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv('config.env')

# Get output directory from environment variable
OUTPUT_DIRECTORY = os.getenv('CSV_OUTPUT_DIR', "/Users/lakshay.nailwal/Desktop/CSV_FILES")

def save_to_csv(filename, headers, data, output_dir=None):
    """
    Save data to CSV file. Supports both list of dicts and list of lists/tuples.
    """
    if output_dir is None:
        output_dir = OUTPUT_DIRECTORY
    
    os.makedirs(output_dir, exist_ok=True)
    full_path = os.path.join(output_dir, filename)

    try:
        with open(full_path, 'w', newline='', encoding='utf-8') as csvfile:
            if isinstance(data, list) and data and isinstance(data[0], dict):
                # Clean empty keys (like '') if present
                sanitized_data = [
                    {k: v for k, v in row.items() if k.strip() != ''}
                    for row in data
                ]
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()
                writer.writerows(sanitized_data)
            else:
                writer = csv.writer(csvfile)
                writer.writerow(headers)
                writer.writerows(data)
        
        print(f"CSV file saved successfully: {full_path}")
        print(f"Total rows written: {len(data)}")
        return full_path

    except Exception as e:
        print(f"Error saving CSV file: {e}")
        raise


def save_to_csv_with_timestamp(filename, headers, data, output_dir=None):
    """
    Save data to CSV file with timestamp in filename.
    
    Args:
        filename (str): Base name of the CSV file (without path and extension)
        headers (list): List of column headers
        data (list): List of rows (each row is a list/tuple)
        output_dir (str, optional): Custom output directory. Defaults to OUTPUT_DIRECTORY.
    
    Returns:
        str: Full path of the saved file
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    timestamped_filename = f"{filename}_{timestamp}.csv"
    
    return save_to_csv(timestamped_filename, headers, data, output_dir)

def append_to_csv(filename, data, output_dir=None):
    """
    Append data to existing CSV file.
    
    Args:
        filename (str): Name of the CSV file (without path)
        data (list): List of rows (each row is a list/tuple)
        output_dir (str, optional): Custom output directory. Defaults to OUTPUT_DIRECTORY.
    
    Returns:
        str: Full path of the file
    """
    if output_dir is None:
        output_dir = OUTPUT_DIRECTORY
    
    full_path = os.path.join(output_dir, filename)
    
    try:
        with open(full_path, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(data)
        
        print(f"Data appended to CSV file: {full_path}")
        print(f"Rows appended: {len(data)}")
        return full_path
        
    except Exception as e:
        print(f"Error appending to CSV file: {e}")
        raise

# Example usage:
# if __name__ == "__main__":
    # Test data
    # test_headers = ["Name", "Age", "City"]
    # test_data = [
    #     ["John", 25, "New York"],
    #     ["Jane", 30, "London"],
    #     ["Bob", 35, "Paris"]
    # ]
    
    # # Test the function
    # save_to_csv("test_data.csv", test_headers, test_data)