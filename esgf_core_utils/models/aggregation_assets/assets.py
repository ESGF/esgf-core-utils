"""
Example when put together: (yaml for simplicity)

assets:
  kerchunk:
    href: <URL>
    type: application/vnd.zarr+kerchunk
    roles: [virtual]
    protocol: https
    description: <DESCRIPTION BELOW>
    alternate:name: <SITE>
    created: <DATETIME>
    updated: <DATETIME>
    file:size: <INT>
"""


AGGREGATED_ASSETS = [
    'kerchunk',
    'CFA',
    'zarr',
    'icechunk'
]

ASSET_AGG_DESCRIPTIONS = {
    'kerchunk': "Kerchunk virtual aggregation reference file.",
    'CFA':      "CF-Compliant NetCDF virtual aggregation file.",
    'zarr':     "Cloud-native Zarr aggregation store.",
    'icechunk': "Transactional tensor storage engine for cloud-native dataset."
}

ASSET_AGG_MIMETYPES = {
    'kerchunk': "application/vnd.zarr+kerchunk",
    'CFA':      "application/netcdf",
    'zarr':     "application/vnd.zarr",
    'icechunk': "application/vnd.icechunk"
}

ASSET_AGG_PROTOCOLS = ['https','http','s3','kerchunk']

ASSET_AGG_PATHS = {
    asset: f'/assets/{asset}' for asset in AGGREGATED_ASSETS
}
