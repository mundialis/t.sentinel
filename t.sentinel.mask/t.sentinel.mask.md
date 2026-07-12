## DESCRIPTION

*t.sentinel.mask* is a GRASS GIS addon Python script to create a space
time raster data set of cloud masks and shadow masks by running
*i.sentinel.mask* parallelized.

## EXAMPLE

```sh
t.sentinel.clouds input=s2 output=s2clouds nprocs=12
```

## SEE ALSO

*[i.sentinel.mask](i.sentinel.mask.md), [t.create](t.create.md),
[t.register](t.register.md)*

## AUTHOR

Anika Weinmann, [mundialis](https://www.mundialis.de/), Germany
