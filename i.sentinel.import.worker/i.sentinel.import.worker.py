#!/usr/bin/env python3

############################################################################
#
# MODULE:       i.sentinel.import.worker
#
# AUTHOR(S):    Guido Riembauer and Anika Weinmann
#
# PURPOSE:      Imports Sentinel-2 data into a new mapset, and optionally resamples bands to 10m.
#
# COPYRIGHT:    (C) 2019-2020 by mundialis and the GRASS Development Team
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

# %module
# % description: Imports Sentinel-2 data into a new mapset, and optionally resamples bands to 10m.
# % keyword: imagery
# % keyword: satellite
# % keyword: Sentinel
# % keyword: import
# %end

# %option G_OPT_F_INPUT
# % key: input
# % type: string
# % required: yes
# % multiple: no
# % key_desc: name
# % description: Name of input directory with downloaded Sentinel data
# %end

# %option
# % key: pattern
# % type: string
# % required: no
# % multiple: no
# % description: Band name pattern to import
# %end

# %option
# % key: pattern_file
# % type: string
# % required: no
# % multiple: no
# % description: File name pattern to import
# %end

# %option
# % key: mapsetid
# % type: string
# % required: yes
# % multiple: no
# % description: ID for mapset
# %end

# %option
# % key: region
# % type: string
# % required: no
# % multiple: no
# % key_desc: name
# % description: Set current region from named region
# %end

# %option G_OPT_MEMORYMB
# %end

# %option
# % key: directory
# % type: string
# % required: no
# % multiple: no
# % description: Directory to hold temporary files (they can be large)
# %end

# %option G_OPT_M_DIR
# % key: metadata
# % description: Name of directory into which Sentinel metadata json dumps are saved
# % required: no
# %end

# %flag
# % key: r
# % description: Reproject raster data using r.import if needed
# %end

# %flag
# % key: i
# % description: Resample 20/60m bands to 10m using r.resamp.interp
# %end

# %flag
# % key: c
# % description: Import cloud masks as raster maps
# % guisection: Settings
# %end

# %flag
# % key: j
# % description: Write metadata json for each band to LOCATION/MAPSET/json folder
# % guisection: Print
# %end

# %flag
# % key: n
# % description: reclassify pixels with value 0 to null() using i.zero2null
# %end

# %rules
# % exclusive: metadata,-j
# %end


import atexit
import os
import shutil
import subprocess
import sys

import grass.script as grass
try:
    import psutil
except ImportError:
    grass.warning('You need to install psutil to use this module: '
                  'pip install psutil')

# initialize global vars
rm_rasters = []


def cleanup():
    grass.message(_("Cleaning up..."))
    nuldev = open(os.devnull, 'w')
    for rm_r in rm_rasters:
        grass.run_command(
            'g.remove', flags='f', type='raster', name=rm_r, quiet=True, stderr=nuldev)


def freeRAM(unit, percent=100):
    """ The function gives the amount of the percentages of the installed RAM.
    Args:
        unit(string): 'GB' or 'MB'
        percent(int): number of percent which shoud be used of the free RAM
                      default 100%
    Returns:
        memory_MB_percent/memory_GB_percent(int): percent of the free RAM in
                                                  MB or GB

    """
    # use psutil cause of alpine busybox free version for RAM/SWAP usage
    mem_available = psutil.virtual_memory().available
    swap_free = psutil.swap_memory().free
    memory_GB = (mem_available + swap_free)/1024.0**3
    memory_MB = (mem_available + swap_free)/1024.0**2

    if unit == "MB":
        memory_MB_percent = memory_MB * percent / 100.0
        return int(round(memory_MB_percent))
    elif unit == "GB":
        memory_GB_percent = memory_GB * percent / 100.0
        return int(round(memory_GB_percent))
    else:
        grass.fatal("Memory unit <%s> not supported" % unit)


def main():

    global rm_rasters

    memory = int(options['memory'])
    input = options['input']
    new_mapset = options['mapsetid']
    pattern = options['pattern']
    flag = ''
    if flags['r']:
        flag += 'r'
    if flags['c']:
        flag += 'c'
    if flags['j']:
        flag += 'j'

    # check if we have the i.sentinel.import addon
    if not grass.find_program('i.sentinel.import', '--help'):
        grass.fatal(_("The 'i.sentinel.import' module was not found, install it first:") +
                    "\n" +
                    "g.extension i.sentinel")

    if not grass.find_program('i.zero2null', '--help'):
        grass.fatal(_("The 'i.zero2null' module was not found, install it first:") +
                    "\n" +
                    "g.extension i.zero2null")

    # set some common environmental variables, like:
    os.environ.update(dict(GRASS_COMPRESS_NULLS='1',
                           GRASS_COMPRESSOR='LZ4',
                           GRASS_MESSAGE_FORMAT='plain'))

    # actual mapset, location, ...
    env = grass.gisenv()
    gisdbase = env['GISDBASE']
    location = env['LOCATION_NAME']
    old_mapset = env['MAPSET']

    grass.message("New mapset: <%s>" % new_mapset)
    grass.utils.try_rmdir(os.path.join(gisdbase, location, new_mapset))

    # create a private GISRC file for each job
    gisrc = os.environ['GISRC']
    newgisrc = "%s_%s" % (gisrc, str(os.getpid()))
    grass.try_remove(newgisrc)
    shutil.copyfile(gisrc, newgisrc)
    os.environ['GISRC'] = newgisrc

    # change mapset
    grass.message("GISRC: <%s>" % os.environ['GISRC'])
    grass.run_command('g.mapset', flags='c', mapset=new_mapset)

    # Test memory settings
    free_ram = freeRAM('MB', 100)
    if free_ram < memory:
        memory = free_ram
        grass.warning(
            "Free RAM only %d MB. <memory> set to it."
            % (memory))

    # import data
    grass.message(_("Importing (and reprojecting as needed) Sentinel-2 data..."))
    kwargs = {
        'input': input,
        'memory': memory,
        'pattern': pattern,
        'flags': flag
    }
    if options['region']:
        grass.run_command('g.region', region=options['region'] + '@' + old_mapset)
        kwargs['extent'] = 'region'
    if options['metadata']:
        kwargs['metadata'] = options['metadata']
    if options["pattern_file"]:
        kwargs["pattern_file"] = options["pattern_file"]

    kwargsstr = ""
    flagstr = ""
    for key, val in kwargs.items():
        if not key == "flags":
            kwargsstr += (" %s='%s'" % (key, val))
        else:
            flagstr += val
    cmd = grass.Popen("i.sentinel.import --q %s -%s" % (kwargsstr, flagstr), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    resp = cmd.communicate()
    for resp_line in resp:
        if 'Input raster does not overlap current computational region' in resp_line.decode("utf-8"):
            if options["pattern_file"]:
                raster_var = options["pattern_file"]
            else:
                raster_var = options["input"]
            grass.warning(_("Input raster <%s> does not overlap current computational region") % raster_var)

    if flags["n"]:
        rasters = list(grass.parse_command(
            "g.list", type="raster", mapset=".").keys())
        for raster in rasters:
            # check if the entire raster is null()
            stats = grass.parse_command("r.info", map=raster, flags="r")
            if not (stats["min"] == "NULL" or stats["max"] == "NULL"):
                grass.run_command("i.zero2null", map=raster, quiet=True)
    # resampling
    if flags['i']:
        grass.message('Resampling bands to 10m')
        raster = list(grass.parse_command('g.list', type='raster').keys())
        if len(raster) < 1:
            grass.fatal('No band found')
        grass.run_command('g.region',raster=raster[0], res=10, flags='pa')

        # get all rasters to be resampled
        raster_resamp_list = list(grass.parse_command('g.list', type='raster', pattern='*B*_10m').keys())
        list_20m = list(grass.parse_command('g.list', type='raster', pattern='*B*_20m').keys())
        list_60m = list(grass.parse_command('g.list', type='raster', pattern='*B*_60m').keys())
        raster_resamp_list.extend(list_20m)
        raster_resamp_list.extend(list_60m)

        # resample
        if len(raster_resamp_list) > 0:
            for raster in raster_resamp_list:
                outname=raster
                if raster.endswith('10m'):
                    grass.run_command('g.rename', raster="%s,%sTMP" % (raster, raster))
                    raster = "%sTMP" % (raster)
                if raster.endswith('20m'):
                    outname = outname.replace('20m','10m')
                elif raster.endswith('60m'):
                    outname = outname.replace('60m','10m')
                grass.run_command('r.resamp.interp',input=raster,output=outname,method='bilinear',quiet=True)
                # remove the old bands
                rm_rasters.append(raster)


if __name__ == "__main__":
    options, flags = grass.parser()
    atexit.register(cleanup)
    sys.exit(main())
