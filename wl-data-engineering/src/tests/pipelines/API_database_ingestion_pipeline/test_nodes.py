import pytest
import requests
from unittest.mock import Mock

from wl_data_engineering.pipelines.API_database_ingestion_pipeline.nodes import call_API
import os

def test_call_API():

    # Given
    api_response = Mock(spec=requests.models.Response)
    
    api_error_dict = {311: "DB_nicht_verfügbar",
                      312: "Haltepunkt_existiert_nicht",
                      316: "Abfragelimit erreicht",
                      320: "GET Abfrage invalid",
                      321: "GET Abfrage parameter fehlt", 
                      322: "Keine Daten vorhanden"}

    # Test when status code is 200
    api_response.status_code = 200
    api_response.json.return_value = {"success": True}
    assert call_API(api_response, api_error_dict) == {"success": True}

    # Test when status code is in api_error_dict
    api_response.status_code = 320
    with pytest.raises(ValueError) as e:
        call_API(api_response, api_error_dict)
    assert str(e.value) == "API call not successful due to Wiener Linien internal defined error! Reason 320: GET Abfrage invalid"

    # Test when status code is not in api_error_dict
    api_response.status_code = 300
    with pytest.raises(ValueError) as e:
        call_API(api_response, api_error_dict)
    assert str(e.value) == "API call not successful and status code not defined by to Wiener Linien! Reason 300"
