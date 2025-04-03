from interpreter.azure.adf import get_pipeline_name

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

def main():
    test_value_single_get_pipeline_name()
    test_value_folder_get_pipeline_name()


if __name__=="__main__":
    main()