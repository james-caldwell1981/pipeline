"""
This file contains functions to handle comma separated values documents. This includes getting the raw document contents
to utilizing 3rd party libraries for modeling and manipulation before saving the data.
"""

from pathlib import Path
import pandas as pd


def get_raw_document_head(file_path: str, rows: int = 20) -> list:
    """
    Gets the unaltered contents of the file located at the given file_path truncated by the number of rows,
    defaulted to 20.

    Examples:
          >>>get_raw_document_head(file_path='testing/data.csv', rows=3)
          ['row1', 'row2', 'row3']

    Args:
        file_path (str): String representation of the path to file relative the current context.
        rows (int): Integer denoting the maximum number of rows to return from the file.

    Returns:
        list: List containing each row in the order it was read from the file.
    """

    file_path = Path(file_path)
    head = []
    with file_path.open() as f:
        for i, line, in enumerate(f):
            if i == rows:
                break
            head.append(line)

    return head


def read_contents_to_dataframe(
        file_path: str,
        index_col: int = None,
        skip_rows: int = 0,
        header: bool = True) -> pd.core.frame.DataFrame:
    """
    Reads the file contents to a pandas dataframe. Column names taken from first non-skipped row by defualt.

    Examples:
    >>> read_contents_to_dataframe(file_path='testing/data.csv', index_col=0, skip_rows=0, header=True)
            val1,val2
        200,more values
        120,other values
        302,data values
        404,missing values
        69,reversed values

    Args:
        file_path (str): String representation of the path to file relative the current context.
        index_col (str): Corresponding column within the csv file which pandas should use for the index of the dataframe.
                         If one is not supplied, pandas will generate an index automatically beginning at 0.
        skip_rows (int): The number of rows to skip prior to constructing the dataframe. EX: a file has a description
                          spanning 3 rows with one row of whitespace before the index row. It would make sense to skip
                          the first 4 rows in most applications.
        header (bool): Boolean indicating whether or not column names are present on the first line read during import.

    Returns:
        pd.core.frame.DataFrame: Pandas dataframe containing file contents indexed as indicated.
    """

    file_path = Path(file_path)
    df = pd.read_csv(file_path, index_col=index_col, skiprows=skip_rows, header=header)
    print(f'File successfully read from {file_path}')

    return df


def save_dataframe_to_csv(df: pd.core.frame.DataFrame, file_path: str) -> str:
    """
    Saves a pandas dataframe as a csv file in the file_path location.

    Examples:
        >>>save_dataframe_to_csv(df=df, file_path='testing/data_test_save.csv')
        Saved successfully to testing/data_test_save.csv

    Args:
        df (pd.core.frame.DataFrame): Pandas dataframe.
        file_path (str): String representation of the path to file relative the current context.

    Returns:
        Success message containig file location.
    """

    file_path = Path(file_path)
    df.to_csv(file_path)

    if file_path.is_file() is True:
        return f'Saved successfully to {file_path}'
    else:
        raise FileExistsError('There was an issue with saving the file.')
