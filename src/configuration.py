import logging
from enum import Enum
from typing import List, Literal


from pydantic import BaseModel, Field, ValidationError, computed_field, field_validator
from keboola.component.exceptions import UserException


class LoadType(str, Enum):
    full_load = "full_load"
    incremental_load = "incremental_load"


class Source(BaseModel):
    app_ids: List[str]
    access_type: Literal["ONGOING", "ONE_TIME_SNAPSHOT"] = Field(default="ONGOING")
    report_categories: List[str] = Field(default=["APP_USAGE"])
    report_names: List[str] = None
    granularity: Literal["DAILY", "WEEKLY", "MONTHLY"] = Field(default="DAILY")

    @field_validator("app_ids")
    def split_id_string(cls, v):
        ids = []
        for id in v:
            ids.append(id.split("-", 1)[0])
        return ids


class Destination(BaseModel):
    load_type: LoadType = Field(default=LoadType.incremental_load)

    @computed_field
    def incremental(self) -> bool:
        return self.load_type == LoadType.incremental_load


class Configuration(BaseModel):
    vendor_id: str
    issuer_id: str
    key_id: str
    key_string: str = Field(alias="#key_string")
    source: Source
    destination: Destination
    debug: bool = False

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")

        if self.debug:
            logging.debug("Component will run in Debug mode")
