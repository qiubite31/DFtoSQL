import pandas as pd


def to_insert_sql(df, cols, table, bind=False):
    insert_sql = """
    INSERT INTO {table} ({column})
    VALUES ({value})"""
    cols_str = ', '.join(cols)
    if bind:
        bind_val_str = ', '.join([':' + x for x in cols])
        print(insert_sql.format(table=table, column=cols_str, value=bind_val_str))
        for row in df[cols].itertuples(index=False, name=False):
            bind_var = dict(zip(cols, row))
            print(bind_var)
    else:
        for row in df[cols].itertuples(index=False, name=False):
            value_str = str(row)
            value_str = value_str.replace("(", '')
            value_str = value_str.replace(")", '')
            print(insert_sql.format(table=table, column=cols_str, value=value_str))


def to_update_sql(df, pri_cols, cols, table, bind=False):
    update_sql = """
    UPDATE {table}
       SET {cols}
     WHERE 1=1
     {pri_key}"""

    for row in df[cols].itertuples(index=False):
        row_dict = row._asdict()
        pri_cols = {k: v for k, v in row_dict.items() if k in pri_cols}
        set_cols = {k: v for k, v in row_dict.items() if k not in pri_cols}

        pri_key_str = ''
        for idx, pri_data in enumerate(pri_cols.items()):
            pri_col = pri_data[0]
            pri_val = "'{}'".format(pri_data[1]) if isinstance(pri_data[1], str) else pri_data[1]
            if bind:
                partial_key = '  AND {} = {}'.format(pri_col, ':' + pri_col)
            else:
                partial_key = '  AND {} = {}'.format(pri_col, pri_val)
            pri_key_str += partial_key
            pri_key_str += '\n     ' if idx + 1 != len(pri_cols) else ''

        set_val_str = ''
        for idx, set_data in enumerate(set_cols.items()):
            set_col = set_data[0]
            set_value = "'{}'".format(set_data[1]) if isinstance(set_data[1], str) else set_data[1]
            if bind:
                partial_set = '{} = {}'.format(set_col, ':' + set_col)
            else:
                partial_set = '{} = {}'.format(set_col, set_value)
            set_val_str += partial_set
            set_val_str += ',\n           ' if idx + 1 != len(set_cols) else ''

        print(update_sql.format(table=table, cols=set_val_str, pri_key=pri_key_str))
        if bind:
            bind_var = dict(zip(cols, row))
            print(bind_var)
