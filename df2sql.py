import pandas as pd


def to_insert_sql(df, cols, table, bind=False):
    template_sql = """
    INSERT INTO {table} ({column})
    VALUES ({value})"""
    insert_sql = None
    cols_str = ', '.join(cols)
    for row in df[cols].itertuples(index=False, name=False):
        if bind:
            bind_val_str = ', '.join([':' + x for x in cols])
            bind_var = dict(zip(cols, row))
        else:
            pur_val_str = str(row)
            pur_val_str = pur_val_str.replace("(", '')
            pur_val_str = pur_val_str.replace(")", '')

        val_str = bind_val_str if bind else pur_val_str
        insert_sql = template_sql.format(table=table, column=cols_str, value=val_str)

        print(insert_sql)
        print(bind_var if bind else '', end="")


def to_update_sql(df, pri_cols, cols, table, bind=False):
    template_sql = """
    UPDATE {table}
       SET {cols}
     WHERE 1=1
     {pri_key}"""
    update_sql = None
    for row in df[cols].itertuples(index=False):
        row_dict = row._asdict()
        pri_cols = {k: v for k, v in row_dict.items() if k in pri_cols}
        set_cols = {k: v for k, v in row_dict.items() if k not in pri_cols}

        pri_key_str = ''
        for idx, pri_data in enumerate(pri_cols.items()):
            pri_col = pri_data[0]
            pri_val = "'{}'".format(pri_data[1]) if isinstance(pri_data[1], str) else pri_data[1]
            select_pri_val = ':' + pri_col if bind else pri_val
            partial_key = '  AND {} = {}'.format(pri_col, select_pri_val)
            pri_key_str += partial_key
            pri_key_str += '\n     ' if idx + 1 != len(pri_cols) else ''

        set_val_str = ''
        for idx, set_data in enumerate(set_cols.items()):
            set_col = set_data[0]
            set_val = "'{}'".format(set_data[1]) if isinstance(set_data[1], str) else set_data[1]
            select_set_val = ':' + set_col if bind else set_val
            partial_set = '{} = {}'.format(set_col, select_set_val)
            set_val_str += partial_set
            set_val_str += ',\n           ' if idx + 1 != len(set_cols) else ''

        update_sql = template_sql.format(table=table, cols=set_val_str, pri_key=pri_key_str)
        bind_var = dict(row_dict)
        print(update_sql)
        print(bind_var if bind else '', end="")
