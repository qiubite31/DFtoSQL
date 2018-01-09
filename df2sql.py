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
            # 把nan取代成NULL
            for col, value in bind_var.items():
                bind_var[col] = '' if str(value) == 'nan' else value
            all_insert = template_sql.format(table=table, column=cols_str, value=bind_val_str)
            insert_sql = insert_sql.format(all_insert=all_insert)
            print(insert_sql)
            print(bind_var)
        else:
            pur_val_str = str(row)
            # 去頭去尾，去掉tuple的括號
            # 如果tuple只有一個item要多去掉一個,
            val_str = val_str[1:-1] if len(row) > 1 else val_str[1:-2]
            # 把item拆解出來，如果是timestamp就去掉刮號
            val_list = val_str.split(',')
            val_list = [x.replace("(", '').replace(")", '')
                        if 'TIMESTAMP' in x.upper() else x
                        for x in val_list]
            pur_val_str = ', '.join(val_list)
            # 把nan取代成NULL
            pur_val_str = pur_val_str.replace('nan', 'NULL')
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
            # 把nan取代成NULL
            partial_key = partial_key.replace('nan', 'NULL')
            pri_key_str += partial_key
            pri_key_str += '\n     ' if pri_idx + 1 != len(pri_cols) else ''

        set_val_str = ''
        for set_idx, set_data in enumerate(set_cols.items()):
            set_col = set_data[0]
            set_val = "'{}'".format(set_data[1]) if isinstance(set_data[1], str) else set_data[1]
            select_set_val = ':' + set_col if bind else set_val
            partial_set = '{} = {}'.format(set_col, select_set_val)
            # 把nan取代成NULL
            partial_set = partial_set.replace('nan', 'NULL')
            set_val_str += partial_set
            set_val_str += ',\n           ' if set_idx + 1 != len(set_cols) else ''

        if bind:
            all_update = template_sql.format(table=table, cols=set_val_str, pri_key=pri_key_str)
            update_sql = update_sql.format(all_update=all_update)
            bind_var = dict(row_dict)
            # 把nan取代成NULL
            for col, value in bind_var.items():
                bind_var[col] = '' if str(value) == 'nan' else value
            print(update_sql)
            print(bind_var)
        else:
            all_update += template_sql.format(table=table, cols=set_val_str, pri_key=pri_key_str)
            if (idx+1) % chunksize == 0 or idx == len(df.index)-1:
                print(update_sql.format(all_update=all_update))
                all_update = ''

 def _print_sql(self, sql, params):
    if not params:
        print(sql)
    else:
        params = {k: "'" + v + "'" if isinstance(v, str) else v
                  for k, v in params.items()}
        params_loc = re.findall('(?<= ):(?!MI|SS|mi|ss)\w+', sql)
        for loc in params_loc:
            sql = sql.replace(loc, '{' + loc[1:] + '}')
        sql = sql.format(**params)
        # 處理null
        sql = sql.replace("''", 'NULL')
        sql = sql.replace("'NULL'", 'NULL')
        print(sql)               
