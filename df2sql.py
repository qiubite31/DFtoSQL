import pandas as pd


def to_insert_sql(df, cols, table, bind=False, chunksize=100):
    template_sql = """
    INSERT INTO {table} ({column})
    VALUES ({value});"""
    insert_sql = """    BEGIN{all_insert}
    END;"""
    all_insert = ''
    cols_str = ', '.join(cols)
    for idx, row in enumerate(df[cols].itertuples(index=False, name=False)):
        if bind:
            bind_val_str = ', '.join([':' + x for x in cols])
            bind_var = dict(zip(cols, row))
            all_insert = template_sql.format(table=table, column=cols_str, value=bind_val_str)
            insert_sql = insert_sql.format(all_insert=all_insert)
            print(insert_sql)
            print(bind_var)
        else:
            pur_val_str = str(row)
            # 去頭去尾，去掉tuple的括號
            val_str = val_str[1:-1]
            # 把item拆解出來，如果是timestamp就去掉刮號
            val_list = val_str.split(',')
            val_list = [x.replace("(", '').replace(")", '')
                        if 'TIMESTAMP' in x.upper() else x
                        for x in val_list]
            pur_val_str = ', '.join(val_list)
            
            all_insert += template_sql.format(table=table, column=cols_str, value=pur_val_str)
            if (idx+1) % chunksize == 0 or idx == len(df.index)-1:
                print(insert_sql.format(all_insert=all_insert))
                all_insert = ''


def to_update_sql(df, pri_cols, cols, table, bind=False, chunksize=100):
    template_sql = """
    UPDATE {table}
       SET {cols}
     WHERE 1=1
     {pri_key};"""
    update_sql = """    BEGIN{all_update}
    END;"""
    all_update = ''
    for idx, row in enumerate(df[cols].itertuples(index=False)):
        row_dict = row._asdict()
        pri_cols = {k: v for k, v in row_dict.items() if k in pri_cols}
        set_cols = {k: v for k, v in row_dict.items() if k not in pri_cols}

        pri_key_str = ''
        for pri_idx, pri_data in enumerate(pri_cols.items()):
            pri_col = pri_data[0]
            pri_val = "'{}'".format(pri_data[1]) if isinstance(pri_data[1], str) else pri_data[1]
            select_pri_val = ':' + pri_col if bind else pri_val
            partial_key = '  AND {} = {}'.format(pri_col, select_pri_val)
            pri_key_str += partial_key
            pri_key_str += '\n     ' if pri_idx + 1 != len(pri_cols) else ''

        set_val_str = ''
        for set_idx, set_data in enumerate(set_cols.items()):
            set_col = set_data[0]
            set_val = "'{}'".format(set_data[1]) if isinstance(set_data[1], str) else set_data[1]
            select_set_val = ':' + set_col if bind else set_val
            partial_set = '{} = {}'.format(set_col, select_set_val)
            set_val_str += partial_set
            set_val_str += ',\n           ' if set_idx + 1 != len(set_cols) else ''

        if bind:
            all_update = template_sql.format(table=table, cols=set_val_str, pri_key=pri_key_str)
            update_sql = update_sql.format(all_update=all_update)
            bind_var = dict(row_dict)
            print(update_sql)
            print(bind_var)
        else:
            all_update += template_sql.format(table=table, cols=set_val_str, pri_key=pri_key_str)
            if (idx+1) % chunksize == 0 or idx == len(df.index)-1:
                print(update_sql.format(all_update=all_update))
                all_update = ''
