[![image-alt](grass_logo.png)](https://grass.osgeo.org/grass-stable/manuals/index.html)

------------------------------------------------------------------------

## NAME

***t.sentinel*** - Toolset for download and processing time series of
Copernicus Sentinel products. This toolset is an addition to the
[i.sentinel](https://grass.osgeo.org/grass-stable/manuals/addons/i.sentinel.html)
toolset.

## KEYWORDS

[imagery](https://grass.osgeo.org/grass-stable/manuals/raster.html),
[import](https://grass.osgeo.org/grass-stable/manuals/topic_import.html),
[satellite](keywords.md#satellite), [Sentinel](keywords.md#Sentinel)

## DESCRIPTION

The *t.sentinel* toolset consists of currently four modules.

*Helper modules:*

[i.sentinel.import.worker](i.sentinel.import.worker.md)  
imports Sentinel-2 data into a new mapset, and optionally resamples
bands to 10m, usually called by **t.sentinel.import**

[i.sentinel.mask.worker](i.sentinel.mask.worker.md)  
runs **i.sentinel.mask** as a worker in different mapsets, usually
called by **t.sentinel.mask**

*Processing modules:*

[t.sentinel.import](t.sentinel.import.md)  
downloads and imports multiple Sentinel-2 scenes in parallel and creates
a STRDS

[t.sentinel.mask](t.sentinel.mask.md)  
creates a space time raster data set of cloud masks and shadow masks by
running **i.sentinel.mask** parallelized

## REQUIREMENTS

- [psutil library](https://pypi.python.org/pypi/psutil)

## AUTHORS

Anika Weinmann and Guido Riembauer,
[mundialis](https://www.mundialis.de/), Germany
