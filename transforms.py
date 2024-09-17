import pandas as pd


def results_to_dataframe(data):
    """Converts the dictionary results into a pandas DataFrame for each API response, handling nested lists.
       Prep for db insert
    """

    data_frames = {}

    for key, result in data.items():
        if isinstance(result, dict):
            # Check if any value in the dictionary is a list of dictionaries
            flattened_data = {}
            for k, v in result.items():
                if isinstance(v, list) and all(isinstance(item, dict) for item in v):
                    # If it's a list of dictionaries, create a DataFrame
                    df = pd.DataFrame(v)
                    flattened_data[k] = df
                else:
                    # Handle single values or other types
                    flattened_data[k] = v

            # If multiple DataFrames exist, concatenate them
            if flattened_data:
                # If have a DataFrame in flattened_data
                df = pd.concat(
                    [flattened_data[k] for k in flattened_data if isinstance(flattened_data[k], pd.DataFrame)], axis=1)
            else:
                df = pd.DataFrame([result])

        elif isinstance(result, list):
            # Convert list of dictionaries to DataFrame (if the list contains dictionaries)
            if all(isinstance(item, dict) for item in result):
                df = pd.DataFrame(result)
            else:
                # If it's just a list of values, create a single-column DataFrame
                df = pd.DataFrame(result, columns=[key])

        else:
            # Handle single non-dictionary values
            if result:
                df = pd.DataFrame([[result]], columns=[key])
            else:
                df = pd.DataFrame()  # Empty DataFrame if the result is empty

        # Exclude empty DataFrames
        if not df.empty:
            data_frames[key] = df

    return data_frames
