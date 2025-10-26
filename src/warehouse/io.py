# src/warehouse/io.py
from __future__ import annotations
import duckdb, pandas as pd

class DB:
    def __init__(self, path: str = "warehouse.duckdb"):
        self.con = duckdb.connect(path)

    def _ensure_table(self, table: str, sample_df: pd.DataFrame):
        self.con.register("_sample_df", sample_df.head(0))
        self.con.execute(f'''
            CREATE TABLE IF NOT EXISTS "{table}" AS
            SELECT * FROM _sample_df LIMIT 0
        ''')
        self.con.unregister("_sample_df")

    def _add_missing_columns(self, table: str, df: pd.DataFrame):
        have = {r[1] for r in self.con.execute(f"PRAGMA table_info('{table}')").fetchall()}
        for col in df.columns:
            if col not in have:
                s = df[col]
                if pd.api.types.is_integer_dtype(s):           dtype = "BIGINT"
                elif pd.api.types.is_float_dtype(s):           dtype = "DOUBLE"
                elif pd.api.types.is_bool_dtype(s):            dtype = "BOOLEAN"
                elif pd.api.types.is_datetime64_any_dtype(s):  dtype = "TIMESTAMP WITH TIME ZONE"
                else:                                          dtype = "VARCHAR"
                self.con.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col}" {dtype}')

    def insert_df(self, table: str, df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        self._ensure_table(table, df)
        self._add_missing_columns(table, df)

        self.con.register("_tmp_df", df)
        table_cols = [r[1] for r in self.con.execute(f"PRAGMA table_info('{table}')").fetchall()]
        common = [c for c in table_cols if c in df.columns]
        cols = ", ".join(f'"{c}"' for c in common)
        self.con.execute(f'INSERT INTO "{table}" ({cols}) SELECT {cols} FROM _tmp_df')
        self.con.unregister("_tmp_df")
        return len(df)
