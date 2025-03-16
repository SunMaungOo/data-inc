from enum import Enum
from typing import Optional,List,Union,Tuple
from dataclasses import dataclass

class ComponentType(str,Enum):
    loader="load"
    transformer="transform"
    caller="call"

    def __str__(self) -> str:
        return self.value
    
class ComponentValueType(str,Enum):
    undefined="ud"
    blob="blob"
    database="db"

    def __str__(self) -> str:
        return self.value
    
class LocationType(str,Enum):
    cloud="cloud"
    prime="prime"

    def __str__(self) -> str:
        return self.value

@dataclass
class DBComponentValue:
    location:LocationType
    tag:str
    tables:List[str]

@dataclass
class UndefinedComponentValue:
    pass

@dataclass
class BlobComponentValue:
    location:str

@dataclass
class LoaderComponent:
    source:Union[DBComponentValue,UndefinedComponentValue,BlobComponentValue]
    target:Union[DBComponentValue,UndefinedComponentValue,BlobComponentValue]
    source_component_value_type:ComponentValueType
    target_component_value_type:ComponentValueType

@dataclass
class TransformerComponent:
    source:Union[DBComponentValue,UndefinedComponentValue]
    component_value_type:ComponentValueType

@dataclass
class CallerComponent:
    caller_type:str
    caller_value:str


def identify_component(value:str)->Optional[ComponentType]:
    
    blocks = remove_white_space(value).strip().split(":")

    component_type = blocks[0]

    try:
        return ComponentType(component_type)
    except ValueError:
        return None
    
def identify_component_value(value:str)->Optional[ComponentValueType]:

    blocks = remove_white_space(value).strip().split(":")

    parser_type = blocks[0]

    try:
        return ComponentValueType(parser_type)
    except ValueError:
        return None
    
def parse_blob_component_value(value:str)->Optional[BlobComponentValue]:

    value = remove_white_space(value.strip())

    return BlobComponentValue(location=value)

def parse_database_component_value(value:str)->Optional[DBComponentValue]:
        
    value = remove_white_space(value.strip())

    blocks = value.split(":")

    if len(blocks)!=2:
        return None
        
    location = None

    try:
        location = LocationType(blocks[0].strip())
    except ValueError:
        return None

    tag_value = blocks[1].strip()

    try:

        list_start = tag_value.index("[")

    except ValueError:
        return None
        
    last_character = tag_value[-1]

    if last_character!="]":
        return None

    tag_name = tag_value[:list_start]
        
    tables = tag_value[list_start+1:-1].split(",")

    return DBComponentValue(location=location,\
                            tag=tag_name,\
                            tables=tables)

def parse_loader_component(value:str)->Optional[LoaderComponent]:

    value = remove_white_space(value.strip())

    value = remove_component_identifier(component_type=ComponentType.loader,value=value)

    target_identifier_index = value.find("|target")

    if target_identifier_index<=0:
        return None
    
    source_component_value = value[:target_identifier_index].removeprefix("source:")

    target_component_value = value[target_identifier_index+1:].removeprefix("target:")

    (source_component_value_type,source) = parse_component_value(value=source_component_value)

    (target_component_value_type,target) = parse_component_value(value=target_component_value)

    if source is None or target is None:
        return None

    return LoaderComponent(
        source=source,\
        target=target,\
        source_component_value_type=source_component_value_type,\
        target_component_value_type=target_component_value_type
    )


def parse_caller_component(value:str)->Optional[CallerComponent]:

    value = remove_white_space(value.strip())

    value = remove_component_identifier(component_type=ComponentType.caller,value=value)

    separator_index = value.find("|")

    if separator_index<=0:
        return None
    
    caller_type = value[:separator_index]

    caller_value = value[separator_index+1:]

    return CallerComponent(caller_type=caller_type,\
                           caller_value=caller_value)

def parse_transformer_component(value:str)->Optional[TransformerComponent]:

    value = remove_white_space(value.strip())

    value = remove_component_identifier(component_type=ComponentType.transformer,value=value)

    allowed_component_value_type = [ComponentValueType.database,ComponentValueType.undefined]

    component_value_type = identify_component_value(value=value)

    if component_value_type not in allowed_component_value_type:
        return None
    
    (source_component_value_type,source) = parse_component_value(value=value)

    if source is None:
        return None
    
    return TransformerComponent(
        source=source,\
        component_value_type=source_component_value_type
    )

def parse_component_value(value:str)->Tuple[ComponentValueType,Optional[Union[DBComponentValue,UndefinedComponentValue,BlobComponentValue]]]:

    component_value_type = identify_component_value(value=value)

    component_value = None

    if component_value_type is None:
        return (None,None)
    
    value = value.removeprefix(str(component_value_type)+":")

    if component_value_type==ComponentValueType.undefined:
        component_value = UndefinedComponentValue()
    elif component_value_type==ComponentValueType.blob:
        component_value = parse_blob_component_value(value=value)
    elif component_value_type==ComponentValueType.database:
        component_value = parse_database_component_value(value=value)
    
    return (component_value_type,component_value)


def remove_component_identifier(component_type:ComponentType,value:str)->str:
    
    start_length = len(str(component_type)+":")

    return value[start_length:]

def remove_white_space(value:str)->str:
    return value.replace(" ","")