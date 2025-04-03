import json
from typing import Optional,Dict,Any,List,Union,Set
from dataclasses import dataclass
from interpreter.common.core import ComponentType,CallerComponent,LoaderComponent,TransformerComponent
from interpreter.common.core import identify_component,parse_loader_component
from interpreter.common.core import parse_caller_component,parse_transformer_component
from interpreter.common.graph import Edge,join_to_node,remove_node

@dataclass
class Activity:
    name:str
    parents:List[str]
    user_properties:Dict[str,str]
    outer_activity:Optional[str]

@dataclass
class Component:
    name:str
    component_type:ComponentType
    component:Union[CallerComponent,LoaderComponent,TransformerComponent]

@dataclass
class Pipeline:
    name:str
    edges:List[Edge]
    components:List[Component]


def get_pipeline_name(pipeline_json:str)->Optional[str]:

    json_object:Dict = None

    pipeline_name = "/"
    
    try:
        json_object:Dict[str,Any] = json.loads(pipeline_json)
    except:
        return None

    if "name" not in json_object:
        return None
    
    pipeline_name+=json_object["name"]

    if "properties" in json_object:

        if "folder" in json_object["properties"] and\
            "name" in json_object["properties"]["folder"]:

            pipeline_name=json_object["properties"]["folder"]["name"]+ pipeline_name
        
    
    return pipeline_name


def get_activities(pipeline_json:str)->Optional[List[Activity]]:

    json_object:Dict = None

    try:
        json_object:Dict[str,Any] = json.loads(pipeline_json)
    except:
        return None
    
    if not "properties" in pipeline_json:
        return None
    
    if not "activities" in pipeline_json["properties"]:
        return None
    
    activities:List[Activity] = list()

    for activity_json in pipeline_json["properties"]["activities"]:

        activity = get_activity(activity_json=activity_json,outer_activity=None)

        if "type" not in activity_json:
            return None
        
        activities.append(activity)

        
        #if for each activity

        if activity_json["type"]=="foreach":
            activities.extend(get_for_each_nested_activities(activity_json=activity_json,\
                                                             outer_activity=activity.name))

        #if conditional activity

        elif activity_json["type"]=="condition":
            activities.extend(get_conditional_nested_activities(activity_json=activity_json,\
                                                                outer_activity=activity.name))

    return activities

def get_for_each_nested_activities(activity_json:str,\
                          outer_activity:str)->Optional[List[Activity]]:
    
    if activity_json["type"]!="foreach":
        return None
    
    if "typeProperties" not in activity_json:
        return None
    
    if "activities" not in activity_json["typeProperties"]:
        return None
    
    return [get_activity(activity_json=x,\
                         outer_activity=outer_activity)\
                        for x in activity_json["typeProperties"]["activities"]]


def get_conditional_nested_activities(activity_json:str,\
                                      outer_activity:str)->Optional[List[Activity]]:
    
    activities:List[Activity] = list()
    
    if activity_json["type"]!="condition":
        return None
    
    if "typeProperties" not in activity_json:
        return None
    
    if "ifFalseActivities" in activity_json["typeProperties"]:
        activities.extend([get_activity(activity_json=x,\
                             outer_activity=outer_activity)\
                            for x in activity_json["typeProperties"]["ifFalseActivities"]])
    
    if "ifTrueActivities" in activity_json["typeProperties"]:
        activities.extend([get_activity(activity_json=x,\
                             outer_activity=outer_activity)\
                            for x in activity_json["typeProperties"]["ifTrueActivities"]])
    
    return activities
    

def get_activity(activity_json:str,outer_activity:str=None)->Optional[Activity]:

    if "name" not in activity_json:
        return None
    
    activity_name = activity_json["name"]

    parent_activities = get_parent_activities(activity_json=activity_json)

    user_properties = get_user_properties(activity_json=activity_json)

    return Activity(name=activity_name,\
                    parents=parent_activities,\
                    user_properties=user_properties,\
                    outer_activity=outer_activity)


def get_parent_activities(activity_json:str)->List[str]:
     
    parent_activities:List[str] = list()

    if "dependsOn" in activity_json:
        
        for depend_activity in activity_json["dependsOn"]:
            parent_activities.append(depend_activity["activity"])

    return parent_activities

def get_user_properties(activity_json:str)->Dict[str,str]:

    user_properties:Dict[str,str] = list()

    if "userProperties" in activity_json:
        for property in activity_json["userProperties"]:
            user_properties[property["name"]] = property["value"]

    return user_properties

    
def get_components(key:str,activities:List[Activity])->List[Component]:

    components:List[Component] = list()

    for actv in activities:

        if key not in actv.user_properties[key]:
            continue

        
        component_type = identify_component(actv.user_properties[key])

        if component_type is None:
            continue
        
        component = None

        if component_type==ComponentType.loader:
            component = parse_loader_component(actv.user_properties[key])
        elif component_type==ComponentType.caller:
            component = parse_caller_component(actv.user_properties[key])
        elif component_type==ComponentType.transformer:
            component = parse_transformer_component(actv.user_properties[key])

        
        components.append(
            Component(
                name=actv.name,
                component_type=component_type,
                component=component
            )
        )

    return components

def get_pipeline(pipeline_name:str,activities:List[Activity],components:List[Component])->Optional[Pipeline]:

    #replace_node_with_edge

    edges = get_edge(activities=activities,has_inner_activity=True)

    components_name:Set[str] = set([component.name for component in components])

    node_names:List[str] = [x.node_name for x in edges]

    removable_node_name:List[str] = list()

    # if the node is not a component we should remove it
    for x in node_names:
        if x not in components_name:
            removable_node_name.append(x)

    #remove a non component node

    for x in removable_node_name:
        edges = remove_node(node_name=x,edges=edges)

    return Pipeline(
        name=pipeline_name,\
        edges=edges,
        components=components
    )

def get_edge(activities:List[Activity],has_inner_activity:bool=True)->List[Edge]:
    """
    activities : activity we wanted to create the edge of
    has_inner_activity : whether to process the inner activity
    """

    outer_activities = [actv for actv in activities if actv.outer_activity is None]

    outer_activities_edge:List[Edge] = list()

    for actv in outer_activities:
        
        outer_activities_edge.append(
            Edge(
                node_name=actv.name,
                parent_nodes=actv.parents
            )
        )

    if not has_inner_activity:
        return outer_activities_edge


    inner_activities = [actv for actv in activities if actv.outer_activity is not None]

    # group the activities which have the same node as parent node

    group_inner_activities:Dict[str,List[Activity]] = dict()

    for actv in inner_activities:

        if actv.outer_activity not in group_inner_activities:
            group_inner_activities[actv.outer_activity] = list()

        group_inner_activities[actv.outer_activity].append(actv)

    for outer_activity_name in group_inner_activities:

        concate_edges = get_edge(activities=group_inner_activities[outer_activity_name],\
                                     has_inner_activity=False)

        outer_activities_edge = join_to_node(node_name=outer_activity_name,\
                     concate_edges=concate_edges,\
                     edges=outer_activities_edge)
        
    return outer_activities_edge
        






    