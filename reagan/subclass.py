from reagan.ssm_parameter_store import SSMParameterStore
from pandas.io.json import json_normalize
import pandas as pd
import numpy as np
import os


class Subclass(SSMParameterStore):
    def __init__(self, verbose=0):
        super().__init__()
        self.verbose = verbose

    def vprint(self, obj):
        if self.verbose:
            print(obj)
        return

    def _split_dataframe_list(self, df, target_column, separator):
        """ df = dataframe to split,
        target_column = the column containing the values to split
        separator = the symb used to perform the split
        returns: a dataframe with each entry for the target column separated, with each element moved into a new row.
        The values in the other columns are duplicated across the newly divided rows.
        """
        row_accumulator = []

        def _split_list_to_rows(row, separator):
            split_row = row[target_column]
            try:
                for s in split_row:
                    new_row = row.to_dict()
                    new_row[target_column] = s
                    row_accumulator.append(new_row)
            except:
                split_row = [np.nan]
                for s in split_row:
                    new_row = row.to_dict()
                    new_row[target_column] = s
                    row_accumulator.append(new_row)

        df.apply(_split_list_to_rows, axis=1, args=(separator,))
        new_df = pd.DataFrame(row_accumulator)
        return new_df

    def _get_nested_columns(self, df, columns):
        """
        When unnesting the
        """
        nested_cols = {}
        for row in df.iterrows():
            for num, value in enumerate(row[1]):
                col = df.columns.tolist()[num]
                # This behavior means we could potentially want a dict as a final value, but not a list
                if type(value) == list:
                    nested_cols[col] = "list"
                elif col in columns:
                    pass
                elif type(value) == dict:
                    nested_cols[col] = "dict"
        return nested_cols

    def _reduce_columns(self, df, final_columns, sep="_"):

        self.vprint("\nRemoving unused columns")

        current_columns = set(df.columns.tolist())
        columns_to_keep = set()

        for col in final_columns:

            split_col = col.split(sep)
            for i in range(len(split_col)):
                name = "_".join(split_col[0 : i + 1])
                if name in current_columns:
                    columns_to_keep.add(name)

        if self.verbose:
            for col in current_columns:
                action = "Keep" if col in columns_to_keep else "Remove"
                self.vprint(f"{col} - {action}")
        return df[list(columns_to_keep)]

    def _unnest(self, df, columns=None):

        if columns:
            df = self._reduce_columns(df, columns)
        cols = self._get_nested_columns(df, columns)

        while len(cols) > 0:
            for col, typ in cols.items():
                if typ == "list":
                    self.vprint("\nSplitting {0} into separate rows".format(col))
                    df = self._split_dataframe_list(df, col, separator="_")
                elif typ == "dict":
                    self.vprint("\nSplitting {0} into separate columns".format(col))
                    df_new = df[col].apply(lambda x: {} if pd.isnull(x) else x)
                    df_new = df_new.apply(pd.Series)
                    df_new.columns = list(
                        map(lambda x: str(col) + "_" + str(x), df_new.columns.tolist())
                    )
                    df = pd.concat([df, df_new], axis=1).drop(columns=col)
            if columns:
                df = self._reduce_columns(df, columns)
            cols = self._get_nested_columns(df, columns)
        return df

    def _format_query(self, query_input, replacements={}):
        """
        Takes in a string or .sql file and optional 'replacements' dictionary.

        Returns a string containing the formatted sql query and replaces the
        keys in the replacements dictionary with their values.
        """

        # checks if input is a file or query
        if query_input.split(".")[-1] == "sql":
            # print("Reading .sql File")
            f = open(query_input, "r")
            # reading files with a guillemet », add an uncessary Â to the string
            query = f.read().replace("Â", "")
            f.close()
        else:
            query = query_input
        if replacements:
            for key, value in replacements.items():
                query = query.replace(key, str(value))
        return query

    def _json_to_df(self, data, columns=[]):

        # df = json_normalize(data)
        df = pd.DataFrame(data)
        df.columns = [col.replace(".", "_") for col in df.columns.to_list()]
        df = self._unnest(df, columns)

        columns_not_found = set(columns) - set(df.columns)
        self.vprint(
            f'\nColumns not found in the object: {", ".join(columns_not_found)}'
        )

        return df
