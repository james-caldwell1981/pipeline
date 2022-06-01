from pathlib import Path
import pandas as pd


def get_raw_document_head(file_path, rows=20):
    """
    Gets the unaltered contents of the file located at the given file_path truncated by the number of rows,
    defaulted to 20.

    :param file_path: String representation of the path to file relative the current context.
    :param rows: Integer denoting the maximum number of rows to return from the file.
    :return: List containing each row in the order it was read from the file.
    """

    file_path = Path(file_path)
    head = []
    with file_path.open() as f:
        for i, line, in enumerate(f):
            if i == rows:
                break
            head.append(line)

    return head


def read_contents_to_dataframe(file_path, index_col=0, skip_rows=0):
    """
    Reads the file contents to a pandas dataframe. Parameters mirroring pandas are used for the same functionality.

    :param file_path: String representation of the path to file relative the current context.
    :param skip_rows: The number of rows to skip prior to constructing the dataframe. EX: a file has a description
                      spanning 3 rows with one row of whitespace before the index row. It would make sense to skip
                      the first 4 rows in most applications.
    :return: Pandas dataframe containing file contents.
    """
    file_path = Path(file_path)
    df = pd.read_csv(file_path, index_col=index_col, skiprows=skip_rows)
    print(f'File successfully read from {file_path}')

    return df


def save_dataframe_to_csv(df, file_path):
    """
    Saves a pandas dataframe as a csv file in the file_path location. Returns successful message with the file_path for
    confirmation or raises an error if the file could not be save properly.

    :param df: Pandas dataframe.
    :param file_path: String representation of the path to file relative the current context.
    :return: Success message on save, raises error on failure.
    """
    file_path = Path(file_path)
    df.to_csv(file_path)

    if file_path.is_file() is True:
        return f'Saved successfully to {file_path}'
    else:
        raise FileExistsError('There was an issue with saving the file.')
