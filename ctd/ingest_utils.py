import argparse
import os
import pandas
import uuid
from typing import Callable, TypeVar, Iterable, Union
from pydantic import BaseModel
from biolink_model.datamodel.pydanticmodel_v2 import (NamedThing, Association)

DataFrame = pandas.DataFrame


def make_fix_comment_in_column_name(comment_char: str) -> Callable:
    def _fix_comment_in_column_name(df: pandas.DataFrame):
        df.columns = [df.columns[0].lstrip(comment_char).lstrip(' ')] + list(df.columns[1:])
        return df
    return _fix_comment_in_column_name


def make_tx_col_func(col: str,
                     f: Callable) -> Callable:
    def _tx_col_func(df: DataFrame) -> DataFrame:
        df[col] = df[col].apply(f)
        return df
    return _tx_col_func


def make_mutator_filter(col: str,
                        value: Union[str, int, float, bool]):
    def _mutator_filter(df: DataFrame):
        return df[df[col] == value]
    return _mutator_filter


T = TypeVar('T', bound=NamedThing)
def make_nodes(df: pandas.DataFrame,
                id_col: str,
                name_col: str,
                cls: type[T]) -> tuple[T]:
    return tuple(
        cls(**row)
        for row in (
            df[[id_col, name_col]]
            .dropna()
            .drop_duplicates()
            .rename(columns={id_col: "id", name_col: "name"})
            .to_dict(orient="records")
        )
    )


A = TypeVar('A', bound=Association)
def make_assocs(df: pandas.DataFrame,
                subject_id_col: str,
                object_id_col: str,
                pmids_col: str,
                fixed_properties: dict,
                column_fixers: tuple[Callable, ...],
                cls: type[A]) -> tuple[A]:
    fixed_df = df[[subject_id_col,
                   object_id_col,
                   pmids_col]].dropna().drop_duplicates()    
    if column_fixers:
        for f in column_fixers:
            fixed_df = f(fixed_df)
    return tuple(
        cls(**(row |
               {'id': str(uuid.uuid4())} |
               fixed_properties))
        for row in (
                (
                fixed_df
                .rename(columns={subject_id_col: "subject",
                                 object_id_col: "object",
                                 pmids_col: "publications"})
                .to_dict(orient="records")) 
        )
    )


def save_to_jsonl(iter_serializable: Iterable[BaseModel],
                  file_name: str):
    if os.path.exists(file_name):
        os.remove(file_name)
    with open(file_name, 'a') as fo:
        for m in iter_serializable:
            print(m.model_dump_json(exclude_unset=True), file=fo)


def namespace_to_dict(namespace: argparse.Namespace) -> dict:
    return {
        k: namespace_to_dict(v) if isinstance(v, argparse.Namespace) else v
        for k, v in vars(namespace).items()
    }
