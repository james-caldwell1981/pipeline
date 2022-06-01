from psycopg2 import sql


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

    if not isinstance(table, str) or not isinstance(columns, list):
        raise TypeError(f'Expected table to be type string and columns of type list.\n\t\
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

    if not isinstance(table, str) or not isinstance(columns, list):
        raise TypeError(f'Expected table to be type string and columns of type list.\n\t\
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
