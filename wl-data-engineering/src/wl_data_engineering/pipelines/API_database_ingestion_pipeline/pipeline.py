"""
This is a boilerplate pipeline 'API_database_ingestion_pipeline'
generated using Kedro 0.19.1
"""

from kedro.pipeline import Pipeline, pipeline, node

from wl_data_engineering.pipelines.API_database_ingestion_pipeline.nodes import call_API, get_next_depart_and_convert_format, prefilter_for_direction


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([

        node(   
            func = call_API,
            inputs  = ["API_Call_13A", 
                       "params:api_error_dict"
                       ],
            outputs = "api_response",
            name    = "call_API_node"),

        node(   
            func = get_next_depart_and_convert_format,
            inputs  = ["params:dataframe_schema_for_db",
                       "api_response"
                       ],
            outputs = "relational_data",
            name    = "get_next_depart"),

        node(   
            func = prefilter_for_direction,
            inputs  = ["relational_data",
                       "params:line_directions_to_consider"
                       ],
            outputs = "API_response_stage_table",
            name    = "prefilter_for_direction"),
    ])
