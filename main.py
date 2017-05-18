import re
from functools import reduce
import pandas as pd


def main():
    feature = {}
    feature['NAME'] = ['Tom', 'Jack', 'Amy', 'Alice']
    feature['AGE'] = [20, 24, 23, 21]
    feature['GENDER'] = ['M', 'M', 'F', 'F']
    feature['TAKE'] = ['Y', 'Y', 'N', 'N']
    col_list = ['NAME', 'AGE', 'GENDER', 'TAKE']

    df = pd.DataFrame(feature, columns=col_list)
    print(df)

    # generate insert sql
    select_cols = ['NAME', 'AGE', 'GENDER', 'TAKE']
    table_name = 'FRIEND'

    from df2sql import to_insert_sql, to_update_sql
    to_insert_sql(df, select_cols, table_name, bind=False)
    to_insert_sql(df, select_cols, table_name, bind=True)

    pri_key = ['NAME', 'AGE']
    select_cols = ['NAME', 'AGE', 'GENDER', 'TAKE']
    to_update_sql(df, pri_key, select_cols, table_name, bind=False)
    to_update_sql(df, pri_key, select_cols, table_name, bind=True)

    # cols_str = reduce(lambda m, n: m + ',' + n, select_cols)
    pass


if __name__ == '__main__':
    main()
