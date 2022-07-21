"""
This file parameterizes queries to a PostgreSQL database.
"""

from typing import Union
from psycopg2 import sql
from pandas.core.indexes.base import Index as pd_Index
from pandas.core.series import Series


def auto_create_table(table: str, columns: tuple, types: Union[tuple, dict]) -> sql.Composed:
    """
    Using the supplied table name, column names, and column types, constructs a query which then creates a table.

    Examples:
        >>>auto_create_table(table='table_name', columns=('col1', 'col2'), types=(str, int64))
        CREATE TABLE IF NOT EXISTS table_name (col1 VARCHAR, col2 INT)

        >>>auto_create_table(table='table_name', columns=('col1', 'col2'), types={'VARCHAR':str, 'INT':(int, int32)})
        CREATE TABLE IF NOT EXISTS table_name (col1 VARCHAR, col2 INT)

    Args:
        table (str): Name of the table as string.
        columns (tuple): Names of the columns to be used as a pandas Index.
            This is obtained through the dataframe method .columns
        types (Union[tuple, dict]): Types for each of the columns as a pandas Series.
            This is obtained through the dataframe method .dtypes

    Returns:
        psycopg2.sql.Composed: The fully constructed query as a composed psycopg2 object

    TODO: Rewrite to utilize tuple for supplied columns instead of list
    TODO: Rewrite to use dictionary of supplied types instead of embedding pandas types
    """

    if not isinstance(table, str) or not isinstance(columns, pd_Index) or not isinstance(types, Series):
        raise TypeError(f'Required types: table => python string; columns => pandas Index; types => pandas Series\n\t\
        Received table: {type(table)} columns: {type(columns)} types: {type(types)}.')

    pandas_types = ('object', 'int64', 'float64', 'bool', 'datetime64', 'timedelta[ns]', 'category')
    postgres_types = ('VARCHAR', 'INT', 'FLOAT', 'BOOL', 'TIMESTAMP', 'INTERVAL', 'ARRAY')

    cols_types = []

    for i in range(0, len(types)):
        if types[i] in pandas_types:
            col_type = postgres_types[pandas_types.index(types[i])]
            cols_types.append(
                sql.Composed([sql.Identifier(columns[i]),
                              sql.SQL(' '), sql.SQL(col_type)]))
        else:
            raise TypeError(f'{types[i]} is not a supported pandas type.')

    query = sql.SQL("CREATE TABLE IF NOT EXISTS {table} ({columns})").format(
        table=sql.Identifier(table),
        columns=sql.SQL(', ').join(cols_types)
    )

    return query


def query_insert_data(table: str, columns: Union[tuple, list]) -> sql.Composed:
    """
    Inserts data into database. By default, this inserts data at the end of the table.
    Values are passed directly through the execute method of the cursor via placeholders
    generated within this function. This allows for inserting a single row or many rows
    without explicitly indicating intent.

    Examples:
        >>> # The below is multi-line for readability. Notice the value Placeholder objects mapped to each column.\
        query_insert_data(table='table_name', columns=('col1', 'col2'))
        Composed([
        SQL('INSERT INTO '),
            Identifier('table_name'),
            SQL(' ('),
            Composed([Identifier('col1'),
            SQL(', '),
            Identifier('col2')]),
            SQL(' VALUES '),
            Composed([Placeholder('col1'),
            SQL(', '),
            Placeholder('col2')]),
            SQL(')')
        ])

    Args:
    table (str): String representation of the table name.
    columns (Union[tuple, list]): List containing all columns for which there is data.

    Returns:
    psycopg2.sql.Composed: Composed psycopg2 object containing Placeholder objects for mapping values to each column
        during query excuation.
    """

    if not isinstance(table, str) or not isinstance(columns, (list, tuple, pd_Index)):
        raise TypeError(f'Expected table to be type string and columns of type list, tuple, or pandas Index.\n\t\
        Received table: {type(table)} and columns: {type(columns)}')

    query = sql.SQL('INSERT INTO {table} ({columns}) VALUES ({values})').format(
        table=sql.Identifier(table),
        columns=sql.SQL(', ').join(map(sql.Identifier, columns)),
        values=sql.SQL(', ').join(map(sql.Placeholder, columns))
    )

    return query


def query_get_data(table: str, columns: Union[tuple, list], limit: Union[int, None] = 20) -> sql.Composed:
    """
    Constructs query to get rows of data from the supplied table limited to the supplied columns.
    Gets all rows when limit is None or the number of rows defined by limit, which defaults to 20 rows.

        Examples:
        >>> # The below is multi-line for readability. Notice the value Placeholder objects mapped to each column.\
        query_get_data(table='table_name', columns=('col1', 'col2'), limit=10)
        Composed([
        SQL('SELECT '),
            Composed([Identifier('col1'),
            SQL(', '),
            Identifier('col2')]),
            SQL(') '),
            FROM
            Identifier('table_name'),
            SQL(' LIMIT '),
            Composed(Placeholder(limit))
        ])

    Args:
    table (str): String representation of the table name.
    columns (Union[tuple, list]): List containing all columns for which there is data.
    limit (Union[int, None): Absolute value of integer limiting the number of rows returned or returning all rows
        when None.

    Returns:
    psycopg2.sql.Composed: Composed psycopg2 object containing Placeholder objects for mapping values to each column
        during query excuation.
    """

    if not isinstance(table, str) or not isinstance(columns, (list, tuple, pd_Index)):
        raise TypeError(f'Expected table to be type string and columns of type list, tuple, or pandas Index.\n\t\
        Received table: {type(table)} and columns: {type(columns)}')

    if limit is None:
        limit = ' * '
    elif not isinstance(limit, int):
        raise TypeError(f'Variable limit must be an integer. Received limit: {type(limit)}.')

    query = sql.SQL('SELECT {columns} FROM {table} LIMIT {return_limit}').format(
        table=sql.Identifier(table),
        columns=sql.SQL(', ').join(map(sql.Identifier, columns)),
        return_limit=sql.SQL(str(limit))
    )

    return query
