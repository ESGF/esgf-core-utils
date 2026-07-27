from typing import Any, Pattern

from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_EXTENSIONS = {
    "CMIP6": {
        "CMIP6": {
            "regex": [
                r"https://esgf\.github\.io/stac-transaction-api/cmip6/v[0-9]\.[0-9]\.[0-9]/schema\.json"
            ],
            "default": "https://esgf.github.io/stac-transaction-api/cmip6/v2.0.0/schema.json",
        },
        "alternate_assets": {
            "regex": [
                r"https://stac-extensions\.github\.io/alternate-assets/v[0-9]\.[0-9]\.[0-9]/schema\.json"
            ],
            "default": "https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json",
        },
        "file": {
            "regex": [
                r"https://stac-extensions\.github\.io/file/v[0-9]\.[0-9]\.[0-9]/schema\.json"
            ],
            "default": "https://stac-extensions.github.io/file/v2.1.0/schema.json",
        },
    },
    "CMIP6Plus": {
        "CMIP6Plus": {
            "regex": [
                r"https://esgf\.github\.io/stac-transaction-api/cmip6plus/v[0-9]\.[0-9]\.[0-9]/schema\.json"
            ],
            "default": "https://esgf.github.io/stac-transaction-api/cmip6plus/v2.0.1/schema.json",
        },
        "alternate_assets": {
            "regex": [
                r"https://stac-extensions\.github\.io/alternate-assets/v[0-9]\.[0-9]\.[0-9]/schema\.json"
            ],
            "default": "https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json",
        },
        "file": {
            "regex": [
                r"https://stac-extensions\.github\.io/file/v[0-9]\.[0-9]\.[0-9]/schema\.json"
            ],
            "default": "https://stac-extensions.github.io/file/v2.1.0/schema.json",
        },
    },
    "CMIP7": {
        "CMIP7": {
            "regex": [
                r"https://esgf\.github\.io/stac-transaction-api/cmip7/v[0-9]\.[0-9]\.[0-9]/schema\.json"
            ],
            "default": "https://esgf.github.io/stac-transaction-api/cmip7/v1.2.9/schema.json",
        },
        "alternate_assets": {
            "regex": [
                r"https://stac-extensions\.github\.io/alternate-assets/v[0-9]\.[0-9]\.[0-9]/schema\.json"
            ],
            "default": "https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json",
        },
        "file": {
            "regex": [
                r"https://stac-extensions\.github\.io/file/v[0-9]\.[0-9]\.[0-9]/schema\.json"
            ],
            "default": "https://stac-extensions.github.io/file/v2.1.0/schema.json",
        },
    },
    "INPUT4MIP": {
        "INPUT4MIP": {
            "regex": [
                r"https://esgf\.github\.io/stac-transaction-api/input4mips/v[0-9]\.[0-9]\.[0-9]/schema\.json"
            ],
            "default": "https://esgf.github.io/stac-transaction-api/input4mips/v1.2.9/schema.json",
        },
        "alternate_assets": {
            "regex": [
                r"https://stac-extensions\.github\.io/alternate-assets/v[0-9]\.[0-9]\.[0-9]/schema\.json"
            ],
            "default": "https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json",
        },
        "file": {
            "regex": [
                r"https://stac-extensions\.github\.io/file/v[0-9]\.[0-9]\.[0-9]/schema\.json"
            ],
            "default": "https://stac-extensions.github.io/file/v3.0.0/schema.json",
        },
    },
    "CORDEX-CMIP6": {
        "CORDEX-CMIP6": {
            "regex": [
                r"https://esgf\.github\.io/stac-transaction-api/cordex-cmip6/v[0-9]\.[0-9]\.[0-9]/schema\.json"
            ],
            "default": "https://esgf.github.io/stac-transaction-api/cordex-cmip6/v1.2.1/schema.json",
        },
        "alternate_assets": {
            "regex": [
                r"https://stac-extensions\.github\.io/alternate-assets/v[0-9]\.[0-9]\.[0-9]/schema\.json"
            ],
            "default": "https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json",
        },
        "file": {
            "regex": [
                r"https://stac-extensions\.github\.io/file/v[0-9]\.[0-9]\.[0-9]/schema\.json"
            ],
            "default": "https://stac-extensions.github.io/file/v2.1.0/schema.json",
        },
    },
    "obs4MIPs": {
        "obs4MIPs": {
            "regex": [
                r"https://esgf\.github\.io/stac-transaction-api/obs4mips/v[0-9]\.[0-9]\.[0-9]/schema\.json"
            ],
            "default": "https://esgf.github.io/stac-transaction-api/obs4mips/v1.0.0/schema.json",
        },
        "alternate_assets": {
            "regex": [
                r"https://stac-extensions\.github\.io/alternate-assets/v[0-9]\.[0-9]\.[0-9]/schema\.json"
            ],
            "default": "https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json",
        },
        "file": {
            "regex": [
                r"https://stac-extensions\.github\.io/file/v[0-9]\.[0-9]\.[0-9]/schema\.json"
            ],
            "default": "https://stac-extensions.github.io/file/v2.1.0/schema.json",
        },
    },
    "OBS4REF": {
        "OBS4REF": {
            "regex": [
                r"https://esgf\.github\.io/stac-transaction-api/obs4ref/v[0-9]\.[0-9]\.[0-9]/schema\.json"
            ],
            "default": "https://esgf.github.io/stac-transaction-api/obs4ref/v1.0.1/schema.json",
        },
        "alternate_assets": {
            "regex": [
                r"https://stac-extensions\.github\.io/alternate-assets/v[0-9]\.[0-9]\.[0-9]/schema\.json"
            ],
            "default": "https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json",
        },
        "file": {
            "regex": [
                r"https://stac-extensions\.github\.io/file/v[0-9]\.[0-9]\.[0-9]/schema\.json"
            ],
            "default": "https://stac-extensions.github.io/file/v2.1.0/schema.json",
        },
    },
}


class Settings(BaseSettings):
    """
    Event Stream Settings
    """

    model_config = SettingsConfigDict(
        env_prefix="VALIDATION_",
        env_nested_delimiter="__",
        env_file=".env",
        extra="ignore",
    )

    version_regex: Pattern[str] = (
        r"/v((?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)"  # type: ignore[assignment]
        r"(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?)/"
    )

    default_extensions: dict[str, Any] = DEFAULT_EXTENSIONS


settings = Settings()
