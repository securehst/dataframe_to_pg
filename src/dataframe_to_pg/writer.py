import json
import math
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any, Optional, Union

import numpy as np
import pandas as pd
import polars as pl
import sqlalchemy as sa
from sqlalchemy import Column, MetaData, Table, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, insert
from sqlalchemy.engine import Engine
from tqdm import tqdm


@dataclass
class WriteResult:
    """
    Dataclass to encapsulate the result of writing a DataFrame to PostgreSQL.

    Attributes:
        updated_columns_count: Number of non-primary key columns updated
                               (only applicable for 'replace' and 'upsert').
        columns: The list of column names after cleaning (only provided if clean_column_names=True).
    """

    updated_columns_count: int
    columns: list[str]


def is_text_type(sql_type: Any) -> bool:
    """
    Helper to determine if an SQL type is (or should be) considered Text.
    """
    return (isinstance(sql_type, type) and issubclass(sql_type, sa.Text)) or isinstance(sql_type, sa.Text)


def clean_value(x: Any) -> Any:
    """
    Convert missing values to None.

    For scalar values:
      - If x is a string that is empty (after stripping) or equals "NaT" or "nan"
        (case-insensitive), return None.
      - Otherwise, use pd.isna to check for missing values.
    For non-scalars:
      - Convert x to a NumPy array and return None if the array is empty or if all elements are missing.
      - Otherwise, return the original value.
    """
    if np.isscalar(x):
        if isinstance(x, str) and (x.strip() == "" or x.lower() in ("nat", "nan")):
            return None
        if pd.isna(x):
            return None
        return x

    # Handle dictionaries for JSON columns
    if isinstance(x, dict):
        # Keep dictionaries as-is for JSON columns
        return json.dumps(x)

    # Handle list/array-like structures
    if isinstance(x, (list, np.ndarray)):
        if len(x) == 0:
            return None

        # Convert NumPy arrays to Python lists for PostgreSQL compatibility
        if isinstance(x, np.ndarray):
            # Handle special case of structured arrays or record arrays
            if x.dtype.names is not None:
                return [dict(zip(x.dtype.names, item)) for item in x]

            # Convert to Python native types
            return x.tolist()

        # Clean individual elements within the list
        return [clean_value(item) for item in x]

    # For non-scalar, non-array objects (like dicts, custom objects, etc.)
    try:
        arr = np.array(x)
        if arr.size == 0 or np.all(pd.isna(arr)):
            return None
    except Exception:  # noqa: S110
        pass

    return x


def _infer_sqlalchemy_type(series: pd.Series) -> type[sa.types.TypeEngine]:
    """
    Infer a basic SQLAlchemy type from a pandas Series dtype.

    Handles arrays by detecting list/array-like objects and determining their element type.
    """
    dt = series.dtype

    # Check if the series contains arrays/lists
    contains_array = False
    element_type = None

    for v in series.dropna():
        if isinstance(v, (list, np.ndarray)):
            contains_array = True
            # Try to determine element type from non-empty arrays
            if len(v) > 0:
                sample = v[0]
                if isinstance(sample, bool):
                    element_type = sa.Boolean
                elif isinstance(sample, int):
                    element_type = sa.Integer
                elif isinstance(sample, float):
                    element_type = sa.Float
                elif isinstance(sample, str):
                    element_type = sa.Text
                break

    if contains_array:
        # Default to Text array if element type couldn't be determined
        return ARRAY(element_type or sa.Text)

    # If numeric but contains at least one non-null string value, force Text.
    if pd.api.types.is_numeric_dtype(dt):
        for v in series.dropna():
            if isinstance(v, str):
                return sa.Text

    # Original type inference logic...
    if dt is object:
        for v in series:
            if isinstance(v, str):
                if v.strip() != "":
                    try:
                        float(v)
                    except ValueError:
                        return sa.Text
            elif v is not None:
                if not isinstance(v, (int, float)):
                    return sa.Text

    if isinstance(dt, pd.DatetimeTZDtype):
        return sa.DateTime(timezone=True)
    elif pd.api.types.is_integer_dtype(dt) or isinstance(dt, pd.Int64Dtype):
        return sa.Integer
    elif np.issubdtype(dt, np.datetime64):
        return sa.DateTime
    elif np.issubdtype(dt, np.integer):
        return sa.Integer
    elif np.issubdtype(dt, np.floating):
        return sa.Float
    elif np.issubdtype(dt, np.bool_):
        return sa.Boolean
    elif np.issubdtype(dt, np.object_) and series.apply(lambda x: isinstance(x, dict)).any():
        return JSONB
    else:
        return sa.Text


def _infer_sqlalchemy_type_from_polars_dtype(pl_dtype: Any) -> type[sa.types.TypeEngine]:
    # Check for list type in Polars
    if str(pl_dtype).startswith("List["):
        # Extract inner type from List[type]
        inner_type_str = str(pl_dtype)[5:-1]

        # Map inner type to SQLAlchemy type
        if inner_type_str in {"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64"}:
            return ARRAY(sa.Integer)
        elif inner_type_str in {"Float32", "Float64"}:
            return ARRAY(sa.Float)
        elif inner_type_str == "Boolean":
            return ARRAY(sa.Boolean)
        elif inner_type_str in {"Datetime", "Date"}:
            return ARRAY(sa.DateTime)
        elif inner_type_str == "Utf8":
            return ARRAY(sa.Text)
        else:
            return ARRAY(sa.Text)

    # Original type inference logic...
    if pl_dtype in {pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64}:
        return sa.Integer
    elif pl_dtype in {pl.Float32, pl.Float64}:
        return sa.Float
    elif pl_dtype == pl.Boolean:
        return sa.Boolean
    elif pl_dtype in {pl.Datetime, pl.Date}:
        return sa.DateTime
    elif pl_dtype == pl.Utf8:
        return sa.Text
    elif pl_dtype == pl.Object:
        return JSONB
    else:
        return sa.Text


def write_dataframe_to_postgres(
    df: Union[pd.DataFrame, pl.DataFrame],
    engine: Engine,
    table_name: str,
    dtypes: Optional[dict[str, Any]] = None,
    sql_dtypes: Optional[dict[str, Any]] = None,
    write_method: str = "upsert",
    chunksize: Optional[Union[int, str]] = None,
    index: Optional[Union[str, list[str]]] = None,
    clean_column_names: bool = False,
    case_type: str = "snake",
    truncate_limit: int = 55,
    yield_chunks: bool = False,  # If True, yields each chunk as it's written.
    progress_bar: bool = True,
) -> Union[None, WriteResult, Generator[list[dict[str, Any]], None, Union[WriteResult, int]]]:
    """
    Write a DataFrame to a PostgreSQL table with conflict resolution,
    automatic addition of missing columns, optional processing in chunks,
    and support for a custom primary key.

    If `sql_dtypes` is provided then these SQLAlchemy types (one of Boolean, DateTime,
    Float, Integer, Text, or postgresql JSON) will be used for the columns and no
    type inference will be performed. You cannot pass both `dtypes` and `sql_dtypes`.

    Parameters:
      df:
          The DataFrame to be written to the PostgreSQL table. Can be either a pandas or Polars DataFrame.
      engine:
          SQLAlchemy engine object to connect to the PostgreSQL database.
      table_name:
          The name of the PostgreSQL table to write the DataFrame to.
      dtypes:
          Optional dictionary mapping column names (including primary key columns)
          to SQLAlchemy types. If not provided for a given column, the type is inferred.
      sql_dtypes:
          Optional dictionary mapping column names to SQLAlchemy types. If provided, these
          types (which should be one of Boolean, DateTime, Float, Integer, Text, or postgresql JSON)
          will be used for the columns without inferring. Cannot be passed alongside `dtypes`.
      write_method:
          One of the following options (default is 'upsert'):
            - 'insert': Insert rows; if the primary key(s) already exist, skip that row.
            - 'replace': Insert rows; if the primary key(s) already exist, update every
                         non-key column with the new value.
            - 'upsert':  Insert rows; if the primary key(s) already exist, update only those
                         non-key columns whose new value is not null.
      chunksize:
          Either None, a positive integer, or the string "auto". If provided, the data
          will be processed in chunks.
      index:
          Optional parameter specifying the primary key column(s). For pandas DataFrames,
          if not provided, the DataFrame's index (or MultiIndex) is used. For Polars DataFrames,
          this parameter is required.
      clean_column_names:
          If True, the DataFrame's column names will be cleaned using pyjanitors' `clean_names`
          method and the resulting column names will be returned in the WriteResult object.
      case_type:
          The case type to pass to pyjanitors' `clean_names` method (default is "snake").
      truncate_limit:
          The truncate limit to pass to pyjanitors `clean_names` method (default is 55).
      yield_chunks:
          If True, yields each chunk as it is written to the database and returns, via the
          generator's return value, a WriteResult (if clean_column_names=True) or the number
          of non-primary key columns updated. Otherwise, the function executes all chunks and,
          if clean_column_names is True, returns a WriteResult object; if False, returns None.
      progress_bar:
            If True, displays a progress bar for the operation. This is only applicable if

    Returns:
      - If yield_chunks is True, yields each chunk (as a list of dicts) and finally returns a
        WriteResult (if clean_column_names=True) or an int representing the updated columns count.
      - If yield_chunks is False and clean_column_names is True, returns a WriteResult object
        with the updated_columns_count and the cleaned column names (accessible via `.columns`).
      - Otherwise, returns None.

    Raises:
      ValueError: If write_method is invalid, if chunksize is invalid, if a Polars
                  DataFrame is passed without specifying the index parameter, if
                  column cleaning is requested but not supported, or if both `dtypes`
                  and `sql_dtypes` are provided.
    """

    # Prevent both dtypes and sql_dtypes from being provided.
    if dtypes is not None and sql_dtypes is not None:
        raise ValueError("Cannot pass both 'dtypes' and 'sql_dtypes'. Please provide only one.")

    allowed_methods = ["insert", "replace", "upsert"]
    if write_method not in allowed_methods:
        raise ValueError(f"write_method must be one of {allowed_methods}, got {write_method}")

    # --- Determine DataFrame type ---
    module_name = type(df).__module__
    if "polars" in module_name:
        import janitor.polars

        is_polars = True
    elif "pandas" in module_name:
        import janitor  # noqa

        is_polars = False
    else:
        raise ValueError("df must be either a pandas.DataFrame or a polars.DataFrame")

    # --- Optionally clean column names ---
    if clean_column_names:
        try:
            # This assumes that the DataFrame has the `clean_names` method (pyjanitor must be installed).
            df = df.clean_names(case_type=case_type, truncate_limit=truncate_limit)
            # Capture the cleaned column names.
            cleaned_columns = list(df.columns)
        except Exception as e:
            raise ValueError("Error cleaning column names: " + str(e)) from e
    else:
        cleaned_columns = None

    # Build a set of column names for which cleaning should be skipped (if sql_dtypes provided and type is text)
    skip_clean = set()
    if sql_dtypes is not None:
        for col, typ in sql_dtypes.items():
            if is_text_type(typ):
                skip_clean.add(col)

    pk_names: list[str] = []
    records: list[dict[str, Any]] = []

    # --- Polars branch ---
    if is_polars:
        # For Polars, the caller must supply the index parameter.
        if index is None:
            raise ValueError("For a Polars DataFrame the 'index' parameter is required.")
        if isinstance(index, str):
            pk_names = [index]
        elif isinstance(index, list) and all(isinstance(x, str) for x in index):
            pk_names = index
        else:
            raise ValueError("The 'index' parameter must be a string or a list of strings.")

        # Check that all key columns exist in the DataFrame.
        for key in pk_names:
            if key not in df.columns:
                raise ValueError(f"Primary key column '{key}' not found in the Polars DataFrame.")

        # Get the Polars schema (a dict mapping column names to their dtypes)
        schema: dict[str, Any] = df.schema

        # Build table columns: primary key columns first (in the order specified), then all others.
        table_columns: list[Column] = []
        for col_name in pk_names:
            if sql_dtypes is not None and col_name in sql_dtypes:
                col_type = sql_dtypes[col_name]
            elif dtypes is not None and col_name in dtypes:
                col_type = dtypes[col_name]
            else:
                col_type = _infer_sqlalchemy_type_from_polars_dtype(schema[col_name])
            table_columns.append(Column(col_name, col_type, primary_key=True))
        for col in df.columns:
            if col in pk_names:
                continue
            if sql_dtypes is not None and col in sql_dtypes:
                col_type = sql_dtypes[col]
            elif dtypes is not None and col in dtypes:
                col_type = dtypes[col]
            else:
                col_type = _infer_sqlalchemy_type_from_polars_dtype(schema[col])
            table_columns.append(Column(col, col_type))

        # Convert the Polars DataFrame to a list of dictionaries.
        records = df.to_dicts()
        new_records = []
        for record in records:
            new_record = {}
            for k, v in record.items():
                if sql_dtypes is not None and k in sql_dtypes and is_text_type(sql_dtypes[k]):
                    new_record[k] = v  # skip cleaning for text columns
                else:
                    new_record[k] = clean_value(v)
            new_records.append(new_record)
        records = new_records

    # --- Pandas branch ---
    else:
        if index is not None:
            if isinstance(index, str):
                pk_names = [index]
            elif isinstance(index, list) and all(isinstance(x, str) for x in index):
                pk_names = index
            else:
                raise ValueError("The 'index' parameter must be a string or a list of strings.")
            # Ensure that all primary key columns exist; if not, reset the index.
            if not all(col in df.columns for col in pk_names):
                df = df.reset_index(drop=False)
        else:
            # Use the DataFrame's index.
            if isinstance(df.index, pd.MultiIndex):
                pk_names = [name if name is not None else f"index_level_{i}" for i, name in enumerate(df.index.names)]
            else:
                pk_names = [df.index.name if df.index.name is not None else "index"]
            df = df.reset_index(drop=False)

        # --- Convert datetime columns to object dtype and replace NaT with None ---
        datetime_cols = df.select_dtypes(include=["datetime", "datetimetz"]).columns
        for col in datetime_cols:
            df[col] = df[col].apply(lambda x: None if pd.isna(x) else x).astype(object)

        # --- Conditionally clean each column ---
        if sql_dtypes is None:
            df = df.map(clean_value)
        else:
            for col in df.columns:
                if col in skip_clean:
                    continue
                else:
                    df[col] = df[col].apply(clean_value)

        table_columns = []
        for col_name in pk_names:
            if sql_dtypes is not None and col_name in sql_dtypes:
                col_type = sql_dtypes[col_name]
            elif dtypes is not None and col_name in dtypes:
                col_type = dtypes[col_name]
            else:
                col_type = _infer_sqlalchemy_type(df[col_name])
            table_columns.append(Column(col_name, col_type, primary_key=True))
        for col in df.columns:
            if col in pk_names:
                continue
            if sql_dtypes is not None and col in sql_dtypes:
                col_type = sql_dtypes[col]
            elif dtypes is not None and col in dtypes:
                col_type = dtypes[col]
            else:
                col_type = _infer_sqlalchemy_type(df[col])
            table_columns.append(Column(col, col_type))
        expected_columns = [col.name for col in table_columns]
        df = df[expected_columns]
        records = df.to_dict(orient="records")
        # --- Extra post-processing ---
        if sql_dtypes is None:
            for record in records:
                for key, value in record.items():
                    try:
                        is_na = pd.isna(value)
                    except Exception:
                        is_na = False
                    if isinstance(is_na, np.ndarray):
                        if is_na.size > 0 and np.all(is_na):
                            record[key] = None
                    else:
                        if is_na:
                            record[key] = None
        else:
            for record in records:
                for key, value in record.items():
                    if key in skip_clean:
                        continue
                    else:
                        try:
                            is_na = pd.isna(value)
                        except Exception:
                            is_na = False
                        if isinstance(is_na, np.ndarray):
                            if is_na.size > 0 and np.all(is_na):
                                record[key] = None
                        else:
                            if is_na:
                                record[key] = None

    # --- Create or update the table schema in PostgreSQL ---
    metadata = MetaData()
    table = Table(table_name, metadata, *table_columns)

    # Use engine.begin() to ensure that DDL commands are committed.
    with engine.begin() as conn:
        inspector = sa.inspect(conn)
        if not inspector.has_table(table_name):
            print(f"Creating table '{table_name}' in the database.")
            metadata.create_all(conn, tables=[table])
        else:
            # Determine which columns exist in the table.
            existing_columns = {col_info["name"] for col_info in inspector.get_columns(table_name)}
            expected_columns = [col.name for col in table.columns]
            missing_columns = [col for col in expected_columns if col not in existing_columns]
            for col_name in missing_columns:
                col_obj = table.columns[col_name]
                col_type_str = col_obj.type.compile(dialect=engine.dialect)
                alter_stmt = f'ALTER TABLE "{table_name}" ADD COLUMN "{col_name}" {col_type_str}'
                conn.execute(text(alter_stmt))

    # --- Build the INSERT statement with conflict handling ---
    stmt = insert(table)
    if write_method == "insert":
        stmt = stmt.on_conflict_do_nothing(index_elements=pk_names)
    elif write_method == "replace":
        stmt = stmt.on_conflict_do_update(
            index_elements=pk_names,
            set_={col.name: stmt.excluded[col.name] for col in table.columns if col.name not in pk_names},
        )
    elif write_method == "upsert":
        stmt = stmt.on_conflict_do_update(
            index_elements=pk_names,
            set_={
                col.name: sa.func.coalesce(stmt.excluded[col.name], table.c[col.name])
                for col in table.columns
                if col.name not in pk_names
            },
        )

    # Compute the number of non-primary key columns updated (only applicable for 'replace' and 'upsert').
    updated_columns_count = (
        len([col for col in table.columns if col.name not in pk_names]) if write_method in ["replace", "upsert"] else 0
    )

    # --- Process records in chunks if requested ---
    if chunksize is not None:
        if isinstance(chunksize, str):
            if chunksize.lower() == "auto":
                computed_chunksize = math.floor(30000 / (len(records[0]) if records else 1))
                chunksize = max(1, computed_chunksize)
            else:
                raise ValueError("chunksize must be a positive integer or 'auto'")
        elif isinstance(chunksize, int):
            if chunksize <= 0:
                raise ValueError("chunksize must be greater than 0")
        else:
            raise ValueError("chunksize must be a positive integer or 'auto'")
        chunks = [records[i : i + chunksize] for i in range(0, len(records), chunksize)]
    else:
        chunks = [records]

    # Define an inner function that processes the chunks.
    def _process_chunks(yield_results: bool) -> Generator[list[dict[str, Any]], None, None]:
        with engine.begin() as conn:
            chunk_iter = tqdm(chunks, desc=f"Writing to {table_name} table", disable=not progress_bar)
            for chunk in chunk_iter:
                conn.execute(stmt, chunk)
                if yield_results:
                    yield chunk

    # --- Execute the chunks and return the final result ---
    if yield_chunks:

        def _generator() -> Generator[list[dict[str, Any]], None, Union[WriteResult, int]]:
            yield from _process_chunks(True)
            return (
                WriteResult(updated_columns_count, cleaned_columns or [])
                if clean_column_names
                else updated_columns_count
            )

        return _generator()
    else:
        # Execute all chunks without yielding.
        list(_process_chunks(False))
        # Return a WriteResult if clean_column_names is True, otherwise behave as before.
        return WriteResult(updated_columns_count, cleaned_columns or [])
