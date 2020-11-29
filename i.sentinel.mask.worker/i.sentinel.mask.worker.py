#!/usr/bin/env python3
#
############################################################################
#
# MODULE:       i.sentinel.mask.worker
#
# AUTHOR(S):    Anika Weinmann
#
# PURPOSE:      This is a worker addon to run i.sentinel.mask in different mapsets
#
# COPYRIGHT: (C) 2019 by mundialis and the GRASS Development Team
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
############################################################################

#%module
#% description: Runs i.sentinel.mask as a worker in different mapsets usually called by t.sentinel.mask.
#% keyword: imagery
#% keyword: satellite
#% keyword: Sentinel
#% keyword: cloud detection
#% keyword: shadow
#% keyword: reflectance
#%end
#%option
#% key: newmapset
#% type: string
#% required: yes
#% multiple: no
#% key_desc: name
#% description: Name of new mapset to run i.sentinel.mask
#% guisection: Required
#%end
#%option G_OPT_F_INPUT
#% key: input_file
#% description: Name of the .txt file containing the list of input bands
#% required : no
#% guisection: Input
#%end
#%option G_OPT_R_INPUT
#% key: blue
#% description: Blue input band
#% required : no
#% guisection: Input
#%end
#%option G_OPT_R_INPUT
#% key: green
#% description: Green input band
#% required : no
#% guisection: Input
#%end
#%option G_OPT_R_INPUT
#% key: red
#% description: Red input band
#% required : no
#% guisection: Input
#%end
#%option G_OPT_R_INPUT
#% key: nir
#% description: NIR input band
#% required : no
#% guisection: Input
#%end
#%option G_OPT_R_INPUT
#% key: nir8a
#% description: NIR8a input band
#% required : no
#% guisection: Input
#%end
#%option G_OPT_R_INPUT
#% key: swir11
#% description: SWIR11 input band
#% required : no
#% guisection: Input
#%end
#%option G_OPT_R_INPUT
#% key: swir12
#% description: SWIR12 input band
#% required : no
#% guisection: Input
#%end
#%option G_OPT_V_OUTPUT
#% key: cloud_mask
#% description: Name of output vector cloud mask
#% required : no
#% guisection: Output
#%end
#%option G_OPT_R_OUTPUT
#% key: cloud_raster
#% description: Name of output raster cloud mask
#% required : no
#% guisection: Output
#%end
#%option G_OPT_V_OUTPUT
#% key: shadow_mask
#% description: Name of output vector shadow mask
#% required : no
#% guisection: Output
#%end
#%option G_OPT_R_OUTPUT
#% key: shadow_raster
#% description: Name of output vector shadow mask
#% required : no
#% guisection: Output
#%end
#%option
#% key: cloud_threshold
#% type: integer
#% description: Threshold for cleaning small areas from cloud mask (in square meters)
#% required : yes
#% answer: 50000
#% guisection: Parameters
#%end
#%option
#% key: shadow_threshold
#% type: integer
#% description: Threshold for cleaning small areas from shadow mask (in square meters)
#% required : yes
#% answer: 10000
#% guisection: Parameters
#%end
#%option G_OPT_F_INPUT
#% key: mtd_file
#% description: Name of the image metadata file (MTD_TL.xml)
#% required : no
#% guisection: Metadata
#%end
#%option G_OPT_F_INPUT
#% key: metadata
#% description: Name of Sentinel metadata json dump
#% required : no
#% guisection: Metadata
#%end
#%option
#% key: scale_fac
#% type: integer
#% description: Rescale factor
#% required : no
#% answer: 10000
#% guisection: Parameters
#%end
#%flag
#% key: r
#% description: Set computational region to maximum image extent
#%end
#%flag
#% key: t
#% description: Do not delete temporary files
#%end
#%flag
#% key: s
#% description: Rescale input bands
#% guisection: Parameters
#%end
#%flag
#% key: c
#% description: Compute only the cloud mask
#%end

#%rules
#% collective: blue,green,red,nir,nir8a,swir11,swir12
#% requires: shadow_mask,mtd_file,metadata
#% requires: shadow_raster,mtd_file,metadata
#% excludes: mtd_file,metadata
#% required: cloud_mask,cloud_raster,shadow_mask,shadow_raster
#% excludes: -c,shadow_mask,shadow_raster
#% required: input_file,blue,green,red,nir,nir8a,swir11,swir12,mtd_file
#% excludes: input_file,blue,green,red,nir,nir8a,swir11,swir12,mtd_file
#%end

import sys
import os
import shutil
import grass.script as grass


def main():

    # check if we have i.sentinel.mask
    if not grass.find_program('i.sentinel.mask', '--help'):
        grass.fatal(_("The 'i.sentinel.mask' module was not found, install it first:") +
                    "\n" +
                    "g.extension i.sentinel")

    # set some common environmental variables, like:
    os.environ.update(dict(GRASS_COMPRESS_NULLS='1',
                           GRASS_COMPRESSOR='ZSTD',
                           GRASS_MESSAGE_FORMAT='plain'))

    # actual mapset, location, ...
    env = grass.gisenv()
    gisdbase = env['GISDBASE']
    location = env['LOCATION_NAME']
    old_mapset = env['MAPSET']

    new_mapset = options['newmapset']
    grass.message("New mapset: <%s>" % new_mapset)
    grass.utils.try_rmdir(os.path.join(gisdbase, location, new_mapset))

    # create a private GISRC file for each job
    gisrc = os.environ['GISRC']
    newgisrc = "%s_%s" % (gisrc, str(os.getpid()))
    grass.try_remove(newgisrc)
    shutil.copyfile(gisrc, newgisrc)
    os.environ['GISRC'] = newgisrc

    ### change mapset
    grass.message("GISRC: <%s>" % os.environ['GISRC'])
    grass.run_command('g.mapset', flags='c', mapset=new_mapset)

    ### import data
    grass.message(_("Running i.sentinel.mask ..."))
    kwargs = dict()
    for opt,val in options.items():
        if opt != 'newmapset' and val:
            if opt in ['green', 'red', 'blue', 'nir', 'nir8a', 'swir11', 'swir12']:
                valnew = val.split('@')[0]
                grass.run_command('g.copy', raster="%s,%s" % (val,valnew), quiet=True)
                kwargs[opt] = valnew
            else:
                kwargs[opt] = val
    flagstr = ''
    for flag,val in flags.items():
        if val:
            flagstr += flag
    grass.run_command('g.region', raster=kwargs['nir'])
    grass.run_command('i.sentinel.mask', quiet=True,
        flags=flagstr, **kwargs)

    grass.utils.try_remove(newgisrc)
    return 0


if __name__ == "__main__":
    options, flags = grass.parser()
    sys.exit(main())
