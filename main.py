import re
from functools import reduce
import pandas as pd


def main():
    feature = {}
    feature['NAME'] = ['Tom', 'Jack', 'Amy', 'Alice']
    feature['AGE'] = [20, 24, 23, 21]
    feature['GENDER'] = ['M', 'M', 'F', 'F']
    col_list = ['NAME', 'AGE', 'GENDER']

    df = pd.DataFrame(feature, columns=col_list)
    print(df)

    # generate insert sql
    select_cols = ['NAME', 'AGE', 'GENDER']
    table_name = 'FRIEND'

    from df2sql import to_insert_sql
    to_insert_sql(df, select_cols, table_name, bind=False)
    to_insert_sql(df, select_cols, table_name, bind=True)

    # cols_str = reduce(lambda m, n: m + ',' + n, select_cols)
    cols_str = ', '.join(select_cols)
    bind_val_str = ', '.join([':' + x for x in select_cols])


if __name__ == '__main__':
    main()
