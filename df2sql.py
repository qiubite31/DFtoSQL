import pandas as pd


def to_insert_sql(df, cols, table, bind=False, chunksize=100):
    template_sql = """
    INSERT INTO {table} ({column})
    VALUES ( {value});"""
    insert_sql = """        BEGIN{all_insert}
    END;"""

    all_insert = ''
    # 要把col name用雙引號括起來，避免col name有空格
    cols_str = ', '.join(['"' + col + '"' for col in cols])
    for idx, row in enumerate(df[cols].itertuples(index=False, name=False)):
        if bind:
            bind_val_str = ', '.join([':' + x for x in cols])
            bind_var = dict(zip(cols, row))
            # 把nan取代成NULL
            for col, value in bind_var.items():
                if 'TIMESTAMP' in repr(value).upper():
                    bind_var[col] = repr(value).replace("(", '').replace(")", '')
                elif 'nan' in str(value):
                    bind_var[col] = 'NULL'
                elif 'NaT' in str(value):
                    bind_var[col] = 'NULL'
                else:
                    bind_var[col] = value
            all_insert = template_sql.format(table=table, column=cols_str, value=bind_val_str)
            insert_sql = insert_sql.format(all_insert=all_insert)
            print(insert_sql)
            print(bind_var)
        else:
            val_list = []
            for val_str in '<br thIsfOrrEplAcE/>'.join(repr(_) for _ in row).split('<br thIsfOrrEplAcE/>'):
                if 'TIMESTAMP' in val_str.upper():
                    val_list.append(val_str.replace("(", '').replace(")", ''))
                elif 'nan' in val_str:
                    val_list.append('NULL')
                elif 'NaT' in val_str:
                    val_list.append('NULL')
                else:
                    val_list.append(val_str)

            pur_val_str = ', '.join(val_list)

            all_insert += template_sql.format(table=table, column=cols_str, value=pur_val_str)
            if (idx+1) % chunksize == 0 or idx == len(df.index)-1:
                insert_sql_str = insert_sql.format(all_insert=all_insert)
                print(insert_sql_str)
                all_insert = ''


def to_update_sql(df, pri_cols, cols, table, bind=False, chunksize=100):
    template_sql = """
    UPDATE {table}
       SET {cols}
     WHERE 1=1
     {pri_key};"""
    update_sql = """        BEGIN{all_update}
    END;"""
    all_update = ''
    for idx, row in enumerate(df[cols].itertuples(index=False)):
        row_dict = row._asdict()
        pri_cols = {k: v for k, v in row_dict.items() if k in pri_cols}
        set_cols = {k: v for k, v in row_dict.items() if k not in pri_cols}

        # 拆成set要接的str和where要接的str
        pri_key_str = ''
        for pri_idx, pri_data in enumerate(pri_cols.items()):
            pri_col = pri_data[0]
            pri_val = "'{}'".format(pri_data[1]) if isinstance(pri_data[1], str) else pri_data[1]
            select_pri_val = ':' + pri_col if bind else pri_val
            # 如果是Timestamp格式，要用repr轉出並取掉左右刮號
            if 'TIMESTAMP' in repr(select_pri_val).upper():
                partial_key = '  AND {} = {}'.format(pri_col, repr(select_pri_val).replace('(', '').replace(')', ''))
            else:
                partial_key = '  AND {} = {}'.format(pri_col, select_pri_val)
            # 處理nan和NaT的NULL
            if 'nan' in partial_key:
                partial_key = partial_key.replace('nan', 'NULL')
                partial_key = partial_key.replace('= NULL', 'IS NULL')
            if 'NaT' in partial_key:
                partial_key = partial_key.replace('NaT', 'NULL')
                partial_key = partial_key.replace('= NULL', 'IS NULL')
            pri_key_str += partial_key
            pri_key_str += '\n     ' if pri_idx + 1 != len(pri_cols) else ''

        set_val_str = ''
        for set_idx, set_data in enumerate(set_cols.items()):
            set_col = set_data[0]
            set_val = "'{}'".format(set_data[1].replace("'", "''")) if isinstance(set_data[1], str) else set_data[1]
            select_set_val = ':' + set_col if params else set_val
            # 如果是Timestamp格式，要用repr轉出並取掉左右刮號
            if 'TIMESTAMP' in repr(select_set_val).upper():
                partial_set = '{} = {}'.format(set_col, repr(select_set_val).replace('(', '').replace(')', ''))
            else:
                partial_set = '{} = {}'.format(set_col, select_set_val)

            # 處理nan和NaT的NULL
            if 'nan' in partial_set or 'NaT' in partial_set:
                partial_set = partial_set.replace('nan', 'NULL')
                partial_set = partial_set.replace('NaT', 'NULL')
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
                update_sql_str = update_sql.format(all_update=all_update)
                print(update_sql_str)
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
