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
