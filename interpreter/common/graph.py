from typing import List,Dict,Optional
from dataclasses import dataclass


@dataclass
class Edge:
    node_name:str
    parent_nodes:List[str]


def is_valid_edge(edge:Edge)->bool:
    return len(edge.parent_nodes)==len(set(edge.parent_nodes))

def is_valid_edges(edges:List[Edge])->bool:

    node_names:List[str] = list()

    for edge in edges:
        if not is_valid_edge(edge=edge):
            return False
        
        node_names.append(edge.node_name)

    return len(node_names)==len(set(node_names))

def remove_node(node_name:str,edges:List[Edge])->Optional[List[Edge]]:
    
    node = get_node(node_name=node_name,edges=edges)

    if node is None:
        return None

    if is_disjointed_node(node_name=node_name,edges=edges):
        return force_remove_node(node_name=node_name,edges=edges)
    
    new_edges = force_remove_node(node_name=node_name,edges=edges)

    used_edges = get_used_edge(node_name=node_name,edges=new_edges)

    new_used_edges:List[Edge] = list()

    #prepare new edge 

    for edge in used_edges:

        used_parent_node = edge.parent_nodes

        #concate the parent node of the remove node to the node which is using that remove node

        for parent_node in node.parent_nodes:
            if parent_node not in used_parent_node:
                used_parent_node.append(parent_node)
        
        #remove the node that we are going to remove as a parents

        used_parent_node.remove(node.node_name)

        new_used_edges.append(Edge
                              (
                                  node_name=edge.node_name,
                                  parent_nodes=used_parent_node
                              ))

    new_parent_used_edge = edge_to_dict(new_used_edges)

    final_edge:List[Edge] = list()
    
    for edge in new_edges:

        # add the new edge that we have prepare

        if edge.node_name in new_parent_used_edge:

            final_edge.append(
                Edge
                (
                    node_name=edge.node_name,
                    parent_nodes=new_parent_used_edge[edge.node_name]
                )
            )

            continue

        final_edge.append(edge)
    
    return final_edge

def force_remove_node(node_name:str,edges:List[Edge])->List[Edge]:
    """
    Forcefully remove the node in edge
    Return edge with the node remove
    """

    new_edges:List[Edge] = list()

    for edge in edges:

        if edge.node_name==node_name:
            continue

        new_edges.append(edge)


    return new_edges


def is_disjointed_node(node_name:str,edges:List[Edge])->bool:
    """
    Return whether there is no node which use the node as the parents
    """

    parent_nodes:List[str] = list()

    for edge in edges:
        parent_nodes.extend(edge.parent_nodes)

    return not(node_name in parent_nodes)

def get_used_edge(node_name:str,edges:List[Edge])->List[Edge]:
    """
    Return which edge used the node name
    """
    
    used_edge:List[Edge] = list()

    for edge in edges:
        if node_name in edge.parent_nodes:
            used_edge.append(edge)

    return used_edge

def get_node(node_name:str,edges:List[Edge])->Optional[Edge]:

    for edge in edges:
        if node_name==edge.node_name:
            return edge
        
    return None

def edge_to_dict(edges:List[Edge])->Dict[str,List[str]]:
    return {x.node_name:x.parent_nodes for x in edges}

def merge_edge(left_edges:List[Edge],right_edges:List[Edge])->List[Edge]:
    
    merge_edge:List[Edge] = list()

    for left_node in left_edges:

        parents = left_node.parent_nodes

        for right_node in right_edges:
            """
            if there are same node in both edge
            concat the parent
            """            
            if left_node.node_name==right_node.node_name:
                for parent in right_node.parent_nodes:
                    if parent not in parents:
                        parents.append(parent)
        
        merge_edge.append(
            Edge
            (
                node_name=left_node.node_name,\
                parent_nodes=parents
            )
        )

    left_node_names = [node.node_name for node in left_edges]

    # node which only exists in right edges

    unqiue_right_nodes = [node for node in right_edges if not node.node_name in left_node_names]

    for node in unqiue_right_nodes:

        merge_edge.append(
            Edge
            (
                node_name=node.node_name,\
                parent_nodes=node.parent_nodes
            )
        )

    return merge_edge

def merge_edges(graphs:List[List[Edge]])->List[Edge]:

    if len(graphs)==1:
        return graphs[0]
    
    left_edge = graphs[0]

    merge_graph = None

    for index in range(1,len(graphs)):

        right_edges = graphs[index]

        merge_graph = merge_edge(left_edges=left_edge,\
                                 right_edges=right_edges)
        
        left_edge = merge_graph

    return merge_graph
    
    



