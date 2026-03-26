"""
Test the polymorphic interpretation of a KafkaEvent
"""

import unittest

from pydantic import ValidationError

from esgf_core_utils.models.kafka import (
    CreatePayload,
    KafkaEvent,
    PatchPayload,
    UpdatePayload,
)


class TestKafkaEvent(unittest.TestCase):
    """Test that events with different methods are correctly resovled"""

    def test_create_payload_valid(self) -> None:
        """Test that a creation payload can be interpreted from a valid payload"""

        payload = """
{
  "data": {
    "type": "STAC",
    "version": "1.0.0",
    "payload": {
      "collection_id": "cmip6",
      "method": "POST",
      "item": {
        "type": "Feature",
        "stac_version": "1.1.0",
        "stac_extensions": [
          "https://stac-extensions.github.io/cmip6/v3.0.1/schema.json",
          "https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json",
          "https://stac-extensions.github.io/file/v2.1.0/schema.json"
        ],
        "id": "CMIP6.ScenarioMIP.IPSL.IPSL-CM6A-LR.ssp119.r4i1p1f1.CFmon.clcalipso.gr.v20191122",
        "collection": "CMIP6",
        "geometry": {
          "type": "Polygon",
          "coordinates": [
            [
              [
                0,
                -90
              ],
              [
                357.5,
                -90
              ],
              [
                357.5,
                90
              ],
              [
                0,
                90
              ],
              [
                0,
                -90
              ]
            ]
          ]
        },
        "bbox": [
          0,
          -90,
          357.5,
          90
        ],
        "properties": {
          "size": 1691149215,
          "created": "2026-03-06T01:15:44Z",
          "updated": "2026-03-06T01:15:44Z",
          "retracted": false,
          "datetime": null,
          "start_datetime": "2015-01-16T12:00:00Z",
          "end_datetime": "2015-01-16T12:00:00Z",
          "version": "20191122",
          "access": [
            "HTTPServer",
            "Globus"
          ],
          "latest": true,
          "project": "CMIP6",
          "title": "CMIP6.ScenarioMIP.IPSL.IPSL-CM6A-LR.ssp119.r4i1p1f1.CFmon.clcalipso.gr",
          "base_id": "CMIP6.ScenarioMIP.IPSL.IPSL-CM6A-LR.ssp119.r4i1p1f1.CFmon.clcalipso.gr",
          "cmip6:grid": "LMDZ grid",
          "cmip6:experiment": "low-end scenario reaching 1.9 W m-2, based on SSP1",
          "cmip6:activity_id": [
            "ScenarioMIP"
          ],
          "cmip6:data_specs_version": "01.00.28",
          "cmip6:experiment_id": "ssp119",
          "cmip6:frequency": "mon",
          "cmip6:grid_label": "gr",
          "cmip6:institution_id": "IPSL",
          "cmip6:member_id": "r4i1p1f1",
          "cmip6:nominal_resolution": "250 km",
          "cmip6:product": "model-output",
          "cmip6:realm": [
            "atmos"
          ],
          "cmip6:pid": "hdl:21.14100/eecd8a20-295d-4a51-a806-feefea8fba66",
          "cmip6:source_id": "IPSL-CM6A-LR",
          "cmip6:source_type": [
            "AOGCM",
            "BGC"
          ],
          "cmip6:sub_experiment_id": "none",
          "cmip6:table_id": "CFmon",
          "cmip6:variable_id": "clcalipso",
          "cmip6:variable_cf_standard_name": "cloud_area_fraction_in_atmosphere_layer",
          "cmip6:variable_long_name": "CALIPSO Percentage Cloud Cover",
          "cmip6:variable_units": "%",
          "cmip6:variant_label": "r4i1p1f1"
        },
        "links": [
          {
            "rel": "self",
            "type": "application/geo+json",
            "href": "https://integration-testing.api.stac.esgf-west.org/collections/CMIP6/items/CMIP6.ScenarioMIP.IPSL.IPSL-CM6A-LR.ssp119.r4i1p1f1.CFmon.clcalipso.gr.v20191122"
          },
          {
            "rel": "parent",
            "type": "application/json",
            "href": "https://integration-testing.api.stac.esgf-west.org/collections/CMIP6"
          },
          {
            "rel": "collection",
            "type": "application/json",
            "href": "https://integration-testing.api.stac.esgf-west.org/collections/CMIP6"
          },
          {
            "rel": "root",
            "type": "application/json",
            "href": "https://integration-testing.api.stac.esgf-west.org/"
          }
        ],
        "assets": {
          "globus": {
            "href": "https://app.globus.org/file-manager?origin_id=904ff241-867c-404e-b355-64b701ba6ac1&origin_path=/css03_data/CMIP6/ScenarioMIP/IPSL/IPSL-CM6A-LR/ssp119/r4i1p1f1/CFmon/clcalipso/gr/v20191122/",
            "description": "Globus Web App Link",
            "type": "text/html",
            "roles": [
              "data"
            ],
            "alternate:name": "esgf-data.nersc.gov",
            "created": "2019-11-22T10:15:56Z",
            "updated": "2019-11-22T10:15:56Z",
            "protocol": "globus",
            "node": "esgf-data.nersc.gov",
            "file:local_path": "CMIP6/ScenarioMIP/IPSL/IPSL-CM6A-LR/ssp119/r4i1p1f1/CFmon/clcalipso/gr/v20191122"
          },
          "clcalipso_CFmon_IPSL-CM6A-LR_ssp119_r4i1p1f1_gr_201501-210012.nc": {
            "href": "https://g-eba899.6b7bd8.0ec8.data.globus.org//css03_data/CMIP6/ScenarioMIP/IPSL/IPSL-CM6A-LR/ssp119/r4i1p1f1/CFmon/clcalipso/gr/v20191122/clcalipso_CFmon_IPSL-CM6A-LR_ssp119_r4i1p1f1_gr_201501-210012.nc",
            "description": "HTTPServer Link",
            "type": "application/netcdf",
            "roles": [
              "data"
            ],
            "alternate:name": "esgf-data.nersc.gov",
            "file:size": 1691149215,
            "file:checksum": "1220fc151c37b78dd4534478975bffaf58b70acdd49ee4541de84ebb221be3ce756a",
            "cmip6:tracking_id": "hdl:21.14100/eecd8a20-295d-4a51-a806-feefea8fba66",
            "created": "2019-11-22T10:15:56Z",
            "updated": "2019-11-22T10:15:56Z",
            "protocol": "https",
            "node": "esgf-data.nersc.gov",
            "file:local_path": "CMIP6/ScenarioMIP/IPSL/IPSL-CM6A-LR/ssp119/r4i1p1f1/CFmon/clcalipso/gr/v20191122/clcalipso_CFmon_IPSL-CM6A-LR_ssp119_r4i1p1f1_gr_201501-210012.nc"
          }
        }
      }
    }
  },
  "metadata": {
    "event_id": "3a8a2536-323b-4259-a218-6002a3491d25",
    "request_id": "dee8c54c-e815-4863-87fb-7b606efd2501",
    "auth": {
        "auth_policy_id": "ESGF-Publish-00012",
        "requester_data": {
          "client_id": "b16b12b6-d274-11e5-8e41-5fea585a1aa2",
          "iss": "auth.globus.org",
          "sub": "b16b12b6-d274-11e5-8e41-5fea585a1aa2"
        }
    },
    "publisher": {
        "package": "some_python_package",
        "version": "0.3.0"
    },
    "time": "2024-07-04T14:17:35Z",
    "schema_version": "1.0.0"
  }
}
"""

        kafka_event = KafkaEvent.model_validate_json(payload)

        self.assertIsInstance(kafka_event.data.payload, CreatePayload)

    def test_create_payload_invalid(self) -> None:
        """Test that a creation payload with the wrong verb is not validated"""

        payload = """
{
  "data": {
    "type": "STAC",
    "version": "1.0.0",
    "payload": {
      "collection_id": "cmip6",
      "method": "DELETE",
      "item": {
        "bbox": [
          -71.04548482586011,
          -40.70724540817599,
          70.95154804339046,
          18.041543625553206
        ],
        "type": "Feature",
        "geometry": {
          "type": "Polygon",
          "coordinates": [
            [
              [
                -71.04548482586011,
                -40.70724540817599
              ],
              [
                70.95154804339046,
                -40.70724540817599
              ],
              [
                70.95154804339046,
                18.041543625553206
              ],
              [
                -71.04548482586011,
                18.041543625553206
              ],
              [
                -71.04548482586011,
                -40.70724540817599
              ]
            ]
          ]
        },
        "properties": {
          "title": "CMIP6.CMIP.IPSL.CMCC-ESM2.ssp585.r1i1000p1f2.3hr.vas.gr.v20220101",
          "description": null,
          "datetime": "2720-11-02T18:18:05.322660Z",
          "created": "1980-07-05T06:25:41.304211Z",
          "updated": "2020-06-02T12:29:58.311307Z",
          "start_datetime": null,
          "end_datetime": null,
          "license": "GqmvOMeWcuNhryVbwniL",
          "providers": [
            {
              "name": "21",
              "description": null,
              "roles": [
                "JiKhIJraixVQrDjSOdrf"
              ],
              "url": "EtVBRiTpUeXBoGBlLRsx"
            }
          ],
          "platform": null,
          "instruments": null,
          "constellation": "yaTflgiUXEwqXFUgjmer",
          "mission": null,
          "gsd": null,
          "citation_url": "http://cera-www.dkrz.de/WDCC/meta/CMIP6/CMIP6.CMIP.IPSL.CMCC-ESM2.ssp585.r1i1000p1f2.3hr.vas.gr.v20220101.json",
          "variable_long_name": "Precipitation",
          "variable_units": "m s-1",
          "cf_standard_name": "air_temperature",
          "activity_id": ["CMIP",]
          "data_specs_version": "01.00.21",
          "experiment_title": "Simulation of recent past (1850 to 2014). Impose changing conditions (consistent with observations). Should be initialised from a point early enough in the pre-industrial control run to ensure that the end of all the perturbed runs branching from the end of this historical run end before the end of the control. Only one ensemble member is requested but modelling groups are strongly encouraged to submit at least three ensemble members of their CMIP historical simulation.",
          "frequency": "3hrPt",
          "further_info_url": "https://furtherinfo.es-doc.org/CMIP6.CMIP.IPSL.CMCC-ESM2.ssp585.r1i1000p1f2.3hr.vas.gr.v20220101",
          "grid": "gs1x1",
          "grid_label": "gr",
          "institution_id": "IPSL",
          "mip_era": "CMIP6",
          "source_id": "CMCC-ESM2",
          "source_type": "AOGCM BGC AER CHEM",
          "experiment_id": "ssp585",
          "sub_experiment_id": "none",
          "nominal_resolution": "10 km",
          "table_id": "3hr",
          "variable_id": "vas",
          "variant_label": "r1i1000p1f2",
          "instance_id": "CMIP6.CMIP.IPSL.CMCC-ESM2.ssp585.r1i1000p1f2.3hr.vas.gr.v20220101"
        },
        "id": "CMIP6.CMIP.IPSL.CMCC-ESM2.ssp585.r1i1000p1f2.3hr.vas.gr.v20220101",
        "stac_version": "1.0.0",
        "assets": {
          "cudrjPnkDBusxfZNPXib": {
            "href": "252",
            "type": "uLBhENjsjwedAAYcAlZp",
            "title": "UwlqbQPrzhfkvbIDxYSI",
            "description": "CMwrUkEnGNshrhDiwsHk",
            "roles": null
          }
        },
        "links": [
          {
            "href": "http://ceda.stac.ac.uk/collections/cmip6/items/CMIP6.CMIP.IPSL.CMCC-ESM2.ssp585.r1i1000p1f2.3hr.vas.gr.v20220101",
            "rel": "self",
            "type": "application/geo+json"
          },
          {
            "href": "http://ceda.stac.ac.uk/collections/cmip6",
            "rel": "parent",
            "type": "application/json"
          },
          {
            "href": "http://ceda.stac.ac.uk/collections/cmip6",
            "rel": "collection",
            "type": "application/json"
          },
          {
            "href": "http://ceda.stac.ac.uk",
            "rel": "root",
            "type": "application/json"
          }
        ],
        "stac_extensions": [],
        "collection": "cmip6"
      }
    }
  },
  "metadata": {
    "event_id": "3a8a2536-323b-4259-a218-6002a3491d25",
    "request_id": "dee8c54c-e815-4863-87fb-7b606efd2501",
    "auth": {
        "auth_policy_id": "ESGF-Publish-00012",
        "client_id": "CEDA-transaction-client",
        "requester_data": {
          "iss": "auth.globus.org",
          "sub": "b16b12b6-d274-11e5-8e41-5fea585a1aa2",
          "identity_provider": "0dcf5063-bffd-40f7-b403-24f97e32fa47",
          "identity_provider_display_name": "University of Chicago"
        }
    },
    "publisher": {
        "package": "some_python_package",
        "version": "0.3.0"
    },
    "time": "2024-07-04T14:17:35Z",
    "schema_version": "1.0.0"
  }
}
"""

        with self.assertRaises(ValidationError):
            KafkaEvent.model_validate_json(payload)

    def test_update_payload(self) -> None:
        """Test that a update payload can be interpreted from a valid payload"""
        payload = """
{
  "data": {
    "type": "STAC",
    "version": "1.0.0",
    "payload": {
      "collection_id": "cmip6",
      "method": "PUT",
      "item_id": "an_item",
      "item": {
        "type": "Feature",
        "id": "CMIP6.ScenarioMIP.IPSL.IPSL-CM6A-LR.ssp119.r4i1p1f1.CFmon.clcalipso.gr.v20191122",
        "collection": "CMIP6",
        "properties": {
          "size": 1691149215,
          "created": "2026-03-06T01:15:44Z",
          "updated": "2026-03-06T01:15:44Z",
          "retracted": false,
          "datetime": null,
          "start_datetime": "2015-01-16T12:00:00Z",
          "end_datetime": "2015-01-16T12:00:00Z",
          "version": "20191122",
          "access": [
            "HTTPServer",
            "Globus"
          ],
          "latest": true,
          "project": "CMIP6",
          "title": "CMIP6.ScenarioMIP.IPSL.IPSL-CM6A-LR.ssp119.r4i1p1f1.CFmon.clcalipso.gr",
          "base_id": "CMIP6.ScenarioMIP.IPSL.IPSL-CM6A-LR.ssp119.r4i1p1f1.CFmon.clcalipso.gr",
          "cmip6:grid": "LMDZ grid",
          "cmip6:experiment": "low-end scenario reaching 1.9 W m-2, based on SSP1",
          "cmip6:activity_id": [
            "ScenarioMIP"
          ],
          "cmip6:data_specs_version": "01.00.28",
          "cmip6:realm": [
            "atmos"
          ],
          "cmip6:pid": "hdl:21.14100/eecd8a20-295d-4a51-a806-feefea8fba66",
          "cmip6:source_id": "IPSL-CM6A-LR",
          "cmip6:variable_cf_standard_name": "cloud_area_fraction_in_atmosphere_layer",
          "cmip6:variable_long_name": "CALIPSO Percentage Cloud Cover",
          "cmip6:variable_units": "%",
          "cmip6:variant_label": "r4i1p1f1"
        }
      }
    }
  },
  "metadata": {
    "event_id": "3a8a2536-323b-4259-a218-6002a3491d25",
    "request_id": "dee8c54c-e815-4863-87fb-7b606efd2501",
    "auth": {
        "auth_policy_id": "ESGF-Publish-00012",
        "requester_data": {
          "client_id": "b16b12b6-d274-11e5-8e41-5fea585a1aa2",
          "iss": "auth.globus.org",
          "sub": "b16b12b6-d274-11e5-8e41-5fea585a1aa2"
        }
    },
    "publisher": {
        "package": "some_python_package",
        "version": "0.3.0"
    },
    "time": "2024-07-04T14:17:35Z",
    "schema_version": "1.0.0"
  }
}
"""

        kafka_event = KafkaEvent.model_validate_json(payload)

        self.assertIsInstance(kafka_event.data.payload, UpdatePayload)

    def test_patch_payload(self) -> None:
        """Test that a patch payload can be interpreted from a valid payload"""

        payload = """
{
  "data": {
    "type": "STAC",
    "version": "1.0.0",
    "payload": {
      "collection_id": "cmip6",
      "method": "PATCH",
      "item_id": "an_item",
      "patch": [
        {
          "op": "add",
          "path": "/properties/license",
          "value": "GqmvOMeWcuNhryVbwniL2"
        }
      ]
    }
  },
  "metadata": {
    "event_id": "3a8a2536-323b-4259-a218-6002a3491d25",
    "request_id": "dee8c54c-e815-4863-87fb-7b606efd2501",
    "auth": {
        "auth_policy_id": "ESGF-Publish-00012",
        "requester_data": {
          "client_id": "b16b12b6-d274-11e5-8e41-5fea585a1aa2",
          "iss": "auth.globus.org",
          "sub": "b16b12b6-d274-11e5-8e41-5fea585a1aa2"
        }
    },
    "publisher": {
        "package": "some_python_package",
        "version": "0.3.0"
    },
    "time": "2024-07-04T14:17:35Z",
    "schema_version": "1.0.0"
  }
}
"""

        kafka_event = KafkaEvent.model_validate_json(payload)

        self.assertIsInstance(kafka_event.data.payload, PatchPayload)
