from jsonschema import Draft202012Validator

PATCH_SCHEMAS = {
    "citation": {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Citation Link Patch",
        "type": "array",
        "minItems": 1,
        "items": {
            "type": "object",
            "required": ["op", "path", "value"],
            "properties": {
                "op": {"const": "add"},
                "path": {"const": "/links/-"},
                "value": {
                    "type": "object",
                    "required": ["href", "type", "rel"],
                    "properties": {
                        "href": {"type": "string", "format": "uri"},
                        "type": {"const": "application/json"},
                        "rel": {"const": "citeas"},
                    },
                    "additionalProperties": False,
                },
            },
            "additionalProperties": False,
        },
    },
    "errata": {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Errata Link Patch",
        "type": "array",
        "minItems": 1,
        "items": {
            "type": "object",
            "required": ["op", "path", "value"],
            "properties": {
                "op": {"const": "add"},
                "path": {"const": "/links/-"},
                "value": {
                    "type": "object",
                    "required": ["rel", "href", "title", "type"],
                    "properties": {
                        "rel": {"const": "related"},
                        "href": {"type": "string", "format": "uri"},
                        "title": {"const": "Errata issue"},
                        "type": {"enum": ["text/html", "application/json"]},
                    },
                    "additionalProperties": False,
                },
            },
            "additionalProperties": False,
        },
    },
    "replicate": {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Replicate Asset Patch",
        "type": "array",
        "items": {
            "type": "object",
            "required": ["op", "path", "value"],
            "properties": {
                "op": {"type": "string", "const": "add"},
                "path": {
                    "type": "string",
                    "pattern": "^/assets/[^/]+/alternate/[^/]+$",
                },
                "value": {"type": "object"},
            },
            "additionalProperties": False,
        },
    },
    "retract": {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Retract Patch",
        "type": "array",
        "minItems": 3,
        "maxItems": 3,
        "prefixItems": [
            {
                "type": "object",
                "required": ["op", "path", "value"],
                "properties": {
                    "op": {"const": "replace"},
                    "path": {"const": "/properties/latest"},
                    "value": {"const": False},
                },
                "additionalProperties": False,
            },
            {
                "type": "object",
                "required": ["op", "path", "value"],
                "properties": {
                    "op": {"const": "replace"},
                    "path": {"const": "/properties/retracted"},
                    "value": {"const": True},
                },
                "additionalProperties": False,
            },
            {
                "type": "object",
                "required": ["op", "path", "value"],
                "properties": {
                    "op": {"const": "replace"},
                    "path": {"const": "/assets"},
                    "value": {"type": "object", "maxProperties": 0},
                },
                "additionalProperties": False,
            },
        ],
        "items": False,
    },
}

PATCH_VALIDATORS = {
    schema_name.upper(): Draft202012Validator(schema)
    for schema_name, schema in PATCH_SCHEMAS.items()
}
