from core import identify_component,ComponentType
from core import identify_component_value,ComponentValueType
from core import parse_database_component_value,DBComponentValue,UndefinedComponentValue
from core import parse_blob_component_value,BlobComponentValue
from core import parse_loader_component,parse_caller_component,parse_transformer_component
from graph import remove_node,Edge,edge_to_dict,merge_edge,merge_edges,replace_nodes
from graph import get_disjointed_nodes,get_last_nodes,get_first_nodes,join_to_node
from graph import replace_node_parents,replace_node_with_edge
from typing import List

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

def test_remove_disjointed_node_remove_node():
    
    # A -> B -> C

    edges:List[Edge] = list()

    edges.append(
        Edge(
            node_name="A",
            parent_nodes=[]
        )
    )

    edges.append(
        Edge(
            node_name="B",
            parent_nodes=["A"]
        )
    )

    edges.append(
        Edge(
            node_name="C",
            parent_nodes=["B"]
        )
    )


    # A -> B

    new_edges = remove_node(node_name="C",edges=edges)

    new_dicts = edge_to_dict(edges=new_edges)

    assert len(new_edges)==2
    assert len(new_dicts["A"])==0 
    assert len(new_dicts["B"])==1 
    assert "A" in new_dicts["B"]

def test_remove_non_disjointed_node_remove_node():

    # A -> B -> C -> D

    edges:List[Edge] = list()

    edges.append(
        Edge(
            node_name="A",
            parent_nodes=[]
        )
    )

    edges.append(
        Edge(
            node_name="B",
            parent_nodes=["A"]
        )
    )

    edges.append(
        Edge(
            node_name="C",
            parent_nodes=["B"]
        )
    )

    edges.append(
        Edge(
            node_name="D",
            parent_nodes=["C"]
        )
    )


    # A -> B -> D

    new_edges = remove_node(node_name="C",edges=edges)

    new_dicts = edge_to_dict(edges=new_edges)

    assert len(new_edges)==3

    assert len(new_dicts["A"])==0 
    assert len(new_dicts["B"])==1 
    assert "A" in new_dicts["B"]
    assert len(new_dicts["D"])==1
    assert "B" in new_dicts["D"]

def test_remove_non_disjointed_node_multi_path_remove_node():

    # A -> B -> C -> D
    #             -> E

    edges:List[Edge] = list()

    edges.append(
        Edge(
            node_name="A",
            parent_nodes=[]
        )
    )

    edges.append(
        Edge(
            node_name="B",
            parent_nodes=["A"]
        )
    )

    edges.append(
        Edge(
            node_name="C",
            parent_nodes=["B"]
        )
    )

    edges.append(
        Edge(
            node_name="D",
            parent_nodes=["C"]
        )
    )

    edges.append(
        Edge(
            node_name="E",
            parent_nodes=["C"]
        )
    )



    # A -> B -> D
    #        -> E

    new_edges = remove_node(node_name="C",edges=edges)

    new_dicts = edge_to_dict(edges=new_edges)

    assert len(new_edges)==4

    assert len(new_dicts["A"])==0 
    assert len(new_dicts["B"])==1 
    assert "A" in new_dicts["B"]
    assert len(new_dicts["D"])==1
    assert "B" in new_dicts["D"]
    assert len(new_dicts["E"])==1
    assert "B" in new_dicts["E"]

def test_simple_merge_edge():
    
    # left
    # B,C -> A

    # right 
    # C -> D

    # merge
    # B,C -> A
    # C -> D 

    left_edge:List[Edge] = list()

    left_edge.append(
        Edge
        (
            node_name="A",
            parent_nodes=["B","C"]
        )
    )

    left_edge.append(
        Edge
        (
            node_name="B",
            parent_nodes=[]
        )
    )


    left_edge.append(
        Edge
        (
            node_name="C",
            parent_nodes=[]
        )
    )


    right_edge:List[Edge] = list()

    right_edge.append(
        Edge
        (
            node_name="C",
            parent_nodes=[]
        )
    )

    right_edge.append(
        Edge
        (
            node_name="D",
            parent_nodes=["C"]
        )
    )

    merged = merge_edge(left_edges=left_edge,right_edges=right_edge)

    merge_dict = edge_to_dict(edges=merged)

    assert len(merged)==4
    assert len(merge_dict["A"])==2
    assert "B" in merge_dict["A"]
    assert "C" in merge_dict["A"]
    assert len(merge_dict["B"])==0
    assert len(merge_dict["C"])==0
    assert len(merge_dict["D"])==1
    assert "C" in merge_dict["D"]

def test_multi_node_merge_edge():

    # left
    # A -> B -> C

    # right
    # B -> D -> E -> C -> F

    # merge
    #  A -> B -> D -> E -> C -> F
    #         -> C

    left_edge:List[Edge] = list()


    left_edge.append(
        Edge
        (
            node_name="A",
            parent_nodes=[]
        )
    )

    left_edge.append(
        Edge
        (
            node_name="B",
            parent_nodes=["A"]
        )
    )

    left_edge.append(
        Edge
        (
            node_name="C",
            parent_nodes=["B"]
        )
    )

    right_edge:List[Edge] = list()

    right_edge.append(
        Edge
        (
            node_name="B",
            parent_nodes=[]
        )
    )

    right_edge.append(
        Edge
        (
            node_name="D",
            parent_nodes=["B"]
        )
    )

    right_edge.append(
        Edge
        (
            node_name="E",
            parent_nodes=["D"]
        )
    )


    right_edge.append(
        Edge
        (
            node_name="C",
            parent_nodes=["E"]
        )
    )


    right_edge.append(
        Edge
        (
            node_name="F",
            parent_nodes=["C"]
        )
    )

    merged = merge_edge(left_edges=left_edge,right_edges=right_edge)

    merge_dict = edge_to_dict(edges=merged)

    assert len(merged)==6
    assert len(merge_dict["A"])==0
    assert len(merge_dict["B"])==1
    assert "A" in merge_dict["B"]
    assert len(merge_dict["C"])==2
    assert "B" in merge_dict["C"]
    assert "E" in merge_dict["C"]
    assert len(merge_dict["D"])==1
    assert "B" in merge_dict["D"]
    assert len(merge_dict["E"])==1
    assert "D" in merge_dict["E"]
    assert len(merge_dict["F"])==1
    assert "C" in merge_dict["F"]

def test_simple_merge_edges():

    # left
    # B,C -> A

    # right 
    # C -> D

    # merge
    # B,C -> A
    # C -> D 

    left_edge:List[Edge] = list()

    left_edge.append(
        Edge
        (
            node_name="A",
            parent_nodes=["B","C"]
        )
    )

    left_edge.append(
        Edge
        (
            node_name="B",
            parent_nodes=[]
        )
    )


    left_edge.append(
        Edge
        (
            node_name="C",
            parent_nodes=[]
        )
    )


    right_edge:List[Edge] = list()

    right_edge.append(
        Edge
        (
            node_name="C",
            parent_nodes=[]
        )
    )

    right_edge.append(
        Edge
        (
            node_name="D",
            parent_nodes=["C"]
        )
    )

    merged = merge_edges(graphs=[left_edge,right_edge])

    merge_dict = edge_to_dict(edges=merged)

    assert len(merged)==4
    assert len(merge_dict["A"])==2
    assert "B" in merge_dict["A"]
    assert "C" in merge_dict["A"]
    assert len(merge_dict["B"])==0
    assert len(merge_dict["C"])==0
    assert len(merge_dict["D"])==1
    assert "C" in merge_dict["D"]

def test_value_replace_nodes():
    # old
    # A -> B -> C

    # replace B with D and E

    # new 
    # A -> D  -> C
    #   -> E  -> C

    edges:List[Edge] = list()

    edges.append(
        Edge
        (
            node_name="A",
            parent_nodes=[]
        )
    )

    edges.append(
        Edge
        (
            node_name="B",
            parent_nodes=["A"]
        )
    )

    edges.append(
        Edge
        (
            node_name="C",
            parent_nodes=["B"]
        )
    )

    replace_edges = replace_nodes(node_name="B",replace_node_names=["D","E"],edges=edges)

    replace_edges_dict = edge_to_dict(edges=replace_edges)

    assert len(replace_edges)==4
    assert len(replace_edges_dict["A"])==0
    assert len(replace_edges_dict["C"])==2
    assert "D" in replace_edges_dict["C"]
    assert "E" in replace_edges_dict["C"]
    assert len(replace_edges_dict["D"])==1
    assert "A" in replace_edges_dict["D"]
    assert len(replace_edges_dict["E"])==1
    assert "A" in replace_edges_dict["E"]

def test_value_get_disjointed_nodes():

    # A -> B
    # C

    edges:List[Edge] = list()

    edges.append(
        Edge
        (
            node_name="A",
            parent_nodes=[]
        )
    )

    edges.append(
        Edge
        (
            node_name="B",
            parent_nodes=["A"]
        )
    )

    edges.append(
        Edge
        (
            node_name="C",
            parent_nodes=[]
        )
    )

    disjointed_nodes = get_disjointed_nodes(edges=edges)

    assert len(disjointed_nodes)==1
    assert disjointed_nodes[0].node_name=="C"
    assert len(disjointed_nodes[0].parent_nodes)==0


def test_value_get_last_nodes():
    # A -> B
    # C

    edges:List[Edge] = list()

    edges.append(
        Edge
        (
            node_name="A",
            parent_nodes=[]
        )
    )

    edges.append(
        Edge
        (
            node_name="B",
            parent_nodes=["A"]
        )
    )

    edges.append(
        Edge
        (
            node_name="C",
            parent_nodes=[]
        )
    )

    last_nodes = get_last_nodes(edges=edges)

    assert len(last_nodes)==1
    assert last_nodes[0].node_name=="B"
    assert len(last_nodes[0].parent_nodes)>0


def test_value_get_first_nodes():
    # A -> B
    # C

    edges:List[Edge] = list()

    edges.append(
        Edge
        (
            node_name="A",
            parent_nodes=[]
        )
    )

    edges.append(
        Edge
        (
            node_name="B",
            parent_nodes=["A"]
        )
    )

    edges.append(
        Edge
        (
            node_name="C",
            parent_nodes=[]
        )
    )

    first_nodes = get_first_nodes(edges=edges)

    assert len(first_nodes)==1
    assert first_nodes[0].node_name=="A"
    assert len(first_nodes[0].parent_nodes)==0

def test_no_existing_node_join_to_node():
    
    # org
    # A -> B -> C

    # concate edge
    # D->E

    # merge 
    # A -> B -> C
    #        -> D -> E

    edges:List[Edge] = list()

    edges.append(
        Edge
        (
            node_name="A",
            parent_nodes=[]
        )
    )

    edges.append(
        Edge
        (
            node_name="B",
            parent_nodes=["A"]
        )
    )

    edges.append(
        Edge
        (
            node_name="C",
            parent_nodes=["B"]
        )
    )

    concate_edges:List[Edge] = list()

    concate_edges.append(
        Edge
        (
            node_name="D",
            parent_nodes=[]
        )
    )

    concate_edges.append(
        Edge
        (
            node_name="E",
            parent_nodes=["D"]
        )
    )

    join_edges = join_to_node(node_name="B",concate_edges=concate_edges,edges=edges)

    join_edges_dicts = edge_to_dict(edges=join_edges)

    assert len(join_edges)==5
    assert len(join_edges_dicts["A"])==0
    assert len(join_edges_dicts["B"])==1
    assert "A" in join_edges_dicts["B"]
    assert len(join_edges_dicts["C"])==1
    assert "B" in join_edges_dicts["C"]
    assert len(join_edges_dicts["D"])==1
    assert "B" in join_edges_dicts["D"]
    assert len(join_edges_dicts["E"])==1
    assert "D" in join_edges_dicts["E"]

def test_existing_node_join_to_node():
    
    # org
    # A -> B -> C

    # concate edge
    # B->D->E
    # F 

    # merge 
    # A -> B -> C
    #        -> D -> E
    #        -> F

    edges:List[Edge] = list()

    edges.append(
        Edge
        (
            node_name="A",
            parent_nodes=[]
        )
    )

    edges.append(
        Edge
        (
            node_name="B",
            parent_nodes=["A"]
        )
    )

    edges.append(
        Edge
        (
            node_name="C",
            parent_nodes=["B"]
        )
    )

    concate_edges:List[Edge] = list()

    concate_edges.append(
        Edge
        (
            node_name="B",
            parent_nodes=[]
        )
    )

    concate_edges.append(
        Edge
        (
            node_name="D",
            parent_nodes=["B"]
        )
    )

    concate_edges.append(
        Edge
        (
            node_name="E",
            parent_nodes=["D"]
        )
    )

    concate_edges.append(
        Edge
        (
            node_name="F",
            parent_nodes=[]
        )
    )


    join_edges = join_to_node(node_name="B",concate_edges=concate_edges,edges=edges)

    join_edges_dicts = edge_to_dict(edges=join_edges)

    assert len(join_edges)==6
    assert len(join_edges_dicts["A"])==0
    assert len(join_edges_dicts["B"])==1
    assert "A" in join_edges_dicts["B"]
    assert len(join_edges_dicts["C"])==1
    assert "B" in join_edges_dicts["C"]
    assert len(join_edges_dicts["D"])==1
    assert "B" in join_edges_dicts["D"]
    assert len(join_edges_dicts["E"])==1
    assert "D" in join_edges_dicts["E"]
    assert len(join_edges_dicts["F"])==1
    assert "B" in join_edges_dicts["F"]

def test_value_replace_node_parents():

    # org 
    # A -> B -> C -> D -> E
    #             

    # replace B with D,E

    # new
    # A -> B
    # C <--> D -> E
    # 
    # C used both D and E
    # both C and D have circular dependency
    #

    edges:List[Edge] = list()

    edges.append(
        Edge
        (
            node_name="A",
            parent_nodes=[]
        )
    )

    edges.append(
        Edge
        (
            node_name="B",
            parent_nodes=["A"]
        )
    )


    edges.append(
        Edge
        (
            node_name="C",
            parent_nodes=["B"]
        )
    )

    edges.append(
        Edge
        (
            node_name="D",
            parent_nodes=["C"]
        )
    )

    edges.append(
        Edge
        (
            node_name="E",
            parent_nodes=["D"]
        )
    )

    replace_nodes = replace_node_parents(node_name="B",replace_node_names=["D","E"],edges=edges)

    replace_nodes_dict = edge_to_dict(replace_nodes)

    assert len(replace_nodes)==5
    assert len(replace_nodes_dict["A"])==0
    assert len(replace_nodes_dict["B"])==1
    assert "A" in replace_nodes_dict["B"]
    assert len(replace_nodes_dict["C"])==2
    assert "D" in replace_nodes_dict["C"]
    assert "E" in replace_nodes_dict["C"]
    assert len(replace_nodes_dict["D"])==1
    assert "C" in replace_nodes_dict["D"]
    assert len(replace_nodes_dict["E"])==1
    assert "D" in replace_nodes_dict["E"]

def test_value_replace_node_with_edge():

    # org 
    # A -> B -> G

    # replace edge
    # C -> D

    # merge 
    # A -> C <--> D -> G

    edges:List[Edge] = list()

    edges.append(
        Edge
        (
            node_name="A",
            parent_nodes=[]
        )
    )

    edges.append(
        Edge
        (
            node_name="B",
            parent_nodes=["A"]
        )
    )

    edges.append(
        Edge
        (
            node_name="G",
            parent_nodes=["B"]
        )
    )

    replace_edges:List[Edge] = list()

    replace_edges.append(
        Edge
        (
            node_name="C",
            parent_nodes=[]
        )
    )

    replace_edges.append(
        Edge
        (
            node_name="D",
            parent_nodes=["C"]
        )
    )

    replace_nodes = replace_node_with_edge(node_name="B",\
                                           replace_edges=replace_edges,\
                                           edges=edges)
    
    replace_nodes_dict = edge_to_dict(replace_nodes)

    assert len(replace_nodes)==4
    assert len(replace_nodes_dict["A"])==0
    assert len(replace_nodes_dict["C"])==2
    assert "A" in replace_nodes_dict["C"]
    assert "D" in replace_nodes_dict["C"]
    assert len(replace_nodes_dict["D"])==1
    assert "C" in replace_nodes_dict["D"]
    assert len(replace_nodes_dict["G"])==1
    assert "D" in replace_nodes_dict["G"]


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
    test_remove_disjointed_node_remove_node()
    test_remove_non_disjointed_node_remove_node()
    test_remove_non_disjointed_node_multi_path_remove_node()
    test_simple_merge_edge()
    test_multi_node_merge_edge()
    test_multi_node_merge_edge()
    test_simple_merge_edges()
    test_value_replace_nodes()
    test_value_get_disjointed_nodes()
    test_value_get_last_nodes()
    test_value_get_first_nodes()
    test_no_existing_node_join_to_node()
    test_existing_node_join_to_node()
    test_value_replace_node_parents()
    test_value_replace_node_with_edge()

if __name__=="__main__":
    main()