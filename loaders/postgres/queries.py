from psycopg2 import sql
from pandas.core.indexes.base import Index as pd_Index
from pandas.core.series import Series


def auto_create_table(table, columns, types):
    """
    Using the supplied table name, column names, and column types, constructs a query which creates a table
    with these attributes. The types of the columns (Index) and types (Series) arguments are pandas specific.
    They can be obtained utilizing dataframe functions as outlined below.


    :param table: Name of the table as string.
    :param columns: Names of the columns to be used as a pandas Index.
                    This is obtained through the dataframe method .columns
    :param types: Types for each of the columns as a pandas Series.
                    This is obtained through the dataframe method .dtypes
    :return: The fully constructed query.
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


def query_insert_data(table, columns):
    """
    Inserts data into database. By default, this inserts data at the end of the database.
    Values are passed directly through the execute method of the cursor via placeholders
    generated within this function. This allows for inserting a single row or many rows.
    Ex: psycopg2.connect.cursor.execute(query, values)

    :param table: String representation of the table name.
    :param columns: List containing all columns for which there is data.
    :return: Constructed query containing SQL to insert data into the supplied table at the supplied columns
                with generated placeholders for passing values at execution.
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


def query_get_data(table, columns, limit=20):
    """
    Constructs query to get rows of data from the supplied table limited to the supplied columns.
    Gets all rows when limit is None or the number of rows defined by limit, which defaults to 20 rows.

    :param table: String representation of the table name.
    :param columns: List containing all columns for which data is requested.
    :param limit: Integer defining how many rows to retrieve. Value of None retrieves all rows. Default is 20.
    :return: Constructed query containing SQL to request data from the supplied table limited to the supplied columns.
    """

    if not isinstance(table, str) or not isinstance(columns, (list, tuple, pd_Index)):
        raise TypeError(f'Expected table to be type string and columns of type list, tuple, or pandas Index.\n\t\
        Received table: {type(table)} and columns: {type(columns)}')

    if limit is None:
        del limit
    elif not isinstance(limit, int):
        raise TypeError(f'Variable limit must be an integer. Received limit: {type(limit)}.')

    query = sql.SQL('SELECT {columns} FROM {table}').format(
        table=sql.Identifier(table),
        columns=sql.SQL(', ').join(map(sql.Identifier, columns))
    )

    return query
