from interpreter.azure.adf import get_pipeline_name,get_activities,get_edge,get_components,get_pipeline
from interpreter.common.graph import edge_to_dict

def test_value_single_get_pipeline_name():
    pipeline_json = """
    {
        "name": "pl_testing"
    }
    """

    pipeline_name = get_pipeline_name(pipeline_json=pipeline_json)
    
    assert pipeline_name=="/pl_testing"

def test_value_folder_get_pipeline_name():
    pipeline_json = """
    {
        "name": "pl_testing",
        "properties":{
            "folder": {
                "name": "foo"
            }
        }
    }
    """

    pipeline_name = get_pipeline_name(pipeline_json=pipeline_json)
    
    assert pipeline_name=="foo/pl_testing"

def test_value_get_activities():

    pipeline_json = """
    {
        "properties": 
            {
                "activities": [
                    {
                        "name": "actv_testing",
                        "type": "Copy",
                        "dependsOn": [
                            {
                                "activity": "parent_actv1"
                            },
                            {
                                "activity": "parent_actv2"
                            }
                        ],
                        "userProperties": [
                            {
                                "name": "prop1",
                                "value": "prop1_value"
                            },
                            {
                                "name": "prop2",
                                "value": "prop2_value"
                            }
                        ]
                    },
                    {
                        "name": "parent_actv1",
                        "type": "Copy",
                        "userProperties": []
                    },
                    {
                        "name": "parent_actv2",
                        "type": "Copy",
                        "userProperties": []
                    }

                ]
            }
    }


    """

    activities = get_activities(pipeline_json=pipeline_json)

    assert len(activities)==3
    assert activities[0].name=="actv_testing"
    assert len(activities[0].parents)==2
    assert activities[0].parents[0]=="parent_actv1"
    assert activities[0].parents[1]=="parent_actv2"
    assert len(activities[0].user_properties)==2
    assert activities[0].user_properties["prop1"]=="prop1_value"
    assert activities[0].user_properties["prop2"]=="prop2_value"
    assert activities[1].name=="parent_actv1"
    assert len(activities[1].parents)==0
    assert activities[2].name=="parent_actv2"
    assert len(activities[2].parents)==0

def test_value_get_edge():

    pipeline_json = """
    {
        "properties": 
            {
                "activities": [
                    {
                        "name": "actv_testing",
                        "type": "Copy",
                        "dependsOn": [
                            {
                                "activity": "parent_actv1"
                            },
                            {
                                "activity": "parent_actv2"
                            }
                        ],
                        "userProperties": [
                            {
                                "name": "prop1",
                                "value": "prop1_value"
                            },
                            {
                                "name": "prop2",
                                "value": "prop2_value"
                            }
                        ]
                    },
                    {
                        "name": "parent_actv1",
                        "type": "Copy",
                        "userProperties": []
                    },
                    {
                        "name": "parent_actv2",
                        "type": "Copy",
                        "userProperties": []
                    }

                ]
            }
    }

    """

    edges = get_edge(activities=get_activities(pipeline_json=pipeline_json))

    edges_dict = edge_to_dict(edges=edges)

    assert len(edges_dict)==3
    assert len(edges_dict["actv_testing"])==2
    assert "parent_actv1" in edges_dict["actv_testing"]
    assert "parent_actv2" in edges_dict["actv_testing"]
    assert len(edges_dict["parent_actv1"])==0
    assert len(edges_dict["parent_actv2"])==0




def test_value_get_components():

    pipeline_json = """
    {
        "properties": 
            {
                "activities": [
                    {
                        "name": "actv_testing",
                        "type": "Copy",
                        "dependsOn": [
                            {
                                "activity": "parent_actv1"
                            },
                            {
                                "activity": "parent_actv2"
                            }
                        ],
                        "userProperties": [
                            {
                                "name": "data-inc",
                                "value": "load:source:db:cloud:foo.hello[testing.apple]|target:db:cloud:foo.hello[testing.ball]"
                            },
                            {
                                "name": "prop2",
                                "value": "prop2_value"
                            }
                        ]
                    },
                    {
                        "name": "parent_actv1",
                        "type": "Copy",
                        "userProperties": [
                            {
                                "name": "data-inc",
                                "value": "load:source:db:cloud:foo.hello[testing.apple]|target:db:cloud:foo.hello[testing.ball]"
                            }
                        ]
                    },
                    {
                        "name": "parent_actv2",
                        "type": "Copy",
                        "userProperties": []
                    }

                ]
            }
    }

    """

    components = get_components(key="data-inc",\
                                activities=get_activities(pipeline_json=pipeline_json))
    
    assert len(components)==2
    assert components[0].name=="parent_actv1" or components[0].name=="actv_testing"
    assert components[1].name=="parent_actv1" or components[0].name=="actv_testing"


def test_value_get_pipeline():

    pipeline_json = """
    {
        "properties": 
            {
                "activities": [
                    {
                        "name": "actv_testing",
                        "type": "Copy",
                        "dependsOn": [
                            {
                                "activity": "parent_actv1"
                            },
                            {
                                "activity": "parent_actv2"
                            }
                        ],
                        "userProperties": [
                            {
                                "name": "data-inc",
                                "value": "load:source:db:cloud:foo.hello[testing.apple]|target:db:cloud:foo.hello[testing.ball]"
                            },
                            {
                                "name": "prop2",
                                "value": "prop2_value"
                            }
                        ]
                    },
                    {
                        "name": "parent_actv1",
                        "type": "Copy",
                        "userProperties": [
                            {
                                "name": "data-inc",
                                "value": "load:source:db:cloud:foo.hello[testing.apple]|target:db:cloud:foo.hello[testing.ball]"
                            }
                        ]
                    },
                    {
                        "name": "parent_actv2",
                        "type": "Copy",
                        "userProperties": []
                    }

                ]
            }
    }

    """

    activities = get_activities(pipeline_json=pipeline_json)

    components = get_components(key="data-inc",activities=activities)

    pipeline = get_pipeline(pipeline_name="foo",\
                            activities=activities,\
                            components=components)
    
    edges_dict = edge_to_dict(edges=pipeline.edges)
    
    assert len(pipeline.components)==2
    assert len(pipeline.edges)==2
    assert len(pipeline.components)==len(pipeline.edges)
    assert len(edges_dict["actv_testing"])==1
    assert "parent_actv1" in edges_dict["actv_testing"]
    assert len(edges_dict["parent_actv1"])==0

    

def main():
    test_value_single_get_pipeline_name()
    test_value_folder_get_pipeline_name()
    test_value_get_activities()
    test_value_get_edge()
    test_value_get_components()
    test_value_get_pipeline()


if __name__=="__main__":
    main()