## DESCRIPTION

*t.sentinel.import* is a GRASS GIS addon Python script to download and
import the Sentinel-2 scenes and create a STRDS.

Since Sentinel-2 [Processing Baseline
4.0.0](https://forum.step.esa.int/t/info-introduction-of-additional-radiometric-offset-in-pb04-00-products/35431),
a systematic offset has been introduced to all reflectance values.  
To account for this, the **offset** option allows the user to indicate
how reflectance data should be corrected (e.g. -1000).

When using the *-c* flag to import cloud masks, the user may define the
type and name of the  
cloud STDS to be imported by the *stvds_clouds*/*strds_clouds* option.
If only the *-c* flag is given, a cloud STVDS is imported and named
*\[strds_output\]\_clouds*

## EXAMPLE

```sh
t.sentinel.import s2names=s2names2.txt settings=/mnt/pgpass/.sentinel.txt \
  nprocs=7 memory=8000 -c pattern='B0(4|8)_10m' strds_output=test
```

## SEE ALSO

*[i.sentinel.download](i.sentinel.download.md),
[i.sentinel.import](i.sentinel.import.md),
[i.sentinel.parallel.download](i.sentinel.parallel.download.md),
[i.sentinel-2.sen2cor.html](i.sentinel-2.sen2cor.md),
[i.sentinel.import.worker](i.sentinel.import.worker.md),
[t.create](t.create.md), [t.register](t.register.md)*

## REQUIREMENTS

If atmospheric correction should be run using the **-a** flag,
[Sen2Cor](http://step.esa.int/main/snap-supported-plugins/sen2cor/)
needs to be installed. For additional information, see
[i.sentinel-2.sen2cor](i.sentinel-2.sen2cor.md).

## AUTHOR

Anika Weinmann, [mundialis](https://www.mundialis.de/), Germany
