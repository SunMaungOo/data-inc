from core import identify_component,ComponentType
from core import identify_component_value,ComponentValueType
from core import parse_database_component_value,DBComponentValue,UndefinedComponentValue
from core import parse_blob_component_value,BlobComponentValue
from core import parse_loader_component,parse_caller_component,parse_transformer_component

def test_value_identify_component():
    assert identify_component("load:")==ComponentType.loader

def test_null_identify_component():
    assert identify_component("foo:") is None

def test_value_identify_component_value():
    assert identify_component_value("db:")==ComponentValueType.database

def test_null_identify_component_value():
    assert identify_component_value("foo:") is None

def test_value_parse_database_component_value():

    value = "cloud:foo[hello.people,hello.cat]"

    component_value:DBComponentValue = parse_database_component_value(value=value)
    
    assert component_value is not None
    assert component_value.location=="cloud"
    assert component_value.tag=="foo"
    assert len(component_value.tables)==2
    assert component_value.tables[0]=="hello.people"
    assert component_value.tables[1]=="hello.cat"

def test_value_parse_blob_component_value():

    value = "/location/to/blob/path"

    component_value:BlobComponentValue = parse_blob_component_value(value=value)

    assert component_value is not None
    assert component_value.location == value

def test_value_parse_loader_component():

    loader_component = parse_loader_component("load:source:db:cloud:foo[hello.people,hello.cat]|target:ud:")

    assert loader_component is not None
    assert loader_component.source_component_value_type==ComponentValueType.database
    assert loader_component.target_component_value_type==ComponentValueType.undefined
    
    source:DBComponentValue = loader_component.source

    assert source is not None
    assert isinstance(source,DBComponentValue)
    assert source.location=="cloud"
    assert source.tag=="foo"
    assert len(source.tables)==2
    assert source.tables[0]=="hello.people"
    assert source.tables[1]=="hello.cat"

    target:UndefinedComponentValue = loader_component.target

    assert target is not None
    assert isinstance(target,UndefinedComponentValue)

def test_value_parse_caller_component():

    caller_component = parse_caller_component("call:api|http://api.foo.com")

    assert caller_component is not None
    assert caller_component.caller_type=="api"
    assert caller_component.caller_value=="http://api.foo.com"

def test_value_parse_transformer_component():

    transformer_component = parse_transformer_component("transform:db:cloud:foo[hello.people]")

    assert transformer_component is not None
    assert transformer_component.component_value_type==ComponentValueType.database
    
    source:DBComponentValue = transformer_component.source

    assert source is not None
    assert source.tag=="foo"
    assert len(source.tables)==1
    assert source.tables[0]=="hello.people"


def test_null_parse_transformer_component():

    transformer_component = parse_transformer_component("transform:blob:")
    
    assert transformer_component is None

def main():
    test_value_identify_component()
    test_null_identify_component()
    test_value_identify_component_value()
    test_null_identify_component_value()
    test_value_parse_database_component_value()
    test_value_parse_blob_component_value()
    test_value_parse_loader_component()
    test_value_parse_caller_component()
    test_value_parse_transformer_component()
    test_null_parse_transformer_component()

if __name__=="__main__":
    main()