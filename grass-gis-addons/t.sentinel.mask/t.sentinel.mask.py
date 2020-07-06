#!/usr/bin/env python3

############################################################################
#
# MODULE:       t.sentinel.mask
#
# AUTHOR(S):    Anika Bettge <bettge at mundialis.de>
#
# PURPOSE:      Creates a space time raster data set of cloud masks and shadow
#               masks by using i.sentinel.mask
#
# COPYRIGHT:	(C) 2020 by mundialis and the GRASS Development Team
#
#		This program is free software under the GNU General Public
#		License (>=v2). Read the file COPYING that comes with GRASS
#		for details.
#
#############################################################################

#%Module
#% description: Creates a space time raster data set of cloud masks and shadow masks by using i.sentinel.mask.
#% keyword: temporal
#% keyword: satellite
#% keyword: Sentinel
#% keyword: cloud detection
#% keyword: shadow detection
#%end

#%option
#% key: input
#% type: string
#% required: yes
#% multiple: no
#% description: STRDS with Sentinel-2 scenes (with bands B02,B03,B04,B08,B8A,B11,B12)
#%end

#%option
#% key: threshold
#% type: double
#% required: no
#% multiple: no
#% description: Minimum ESA cloud percentage to trigger cloud and shadow detection.
#% answer: 0
#%end

#%option
#% key: output_clouds
#% type: string
#% required: yes
#% multiple: no
#% description: STRDS with cloud masks of Sentinel-2 scenes
#%end

#%option
#% key: output_shadows
#% type: string
#% required: no
#% multiple: no
#% description: STRDS with shodow masks of Sentinel-2 scenes
#%end

#%option
#% key: metadata
#% type: string
#% required: no
#% multiple: no
#% key_desc: name
#% label: Name of folder with Sentinel metadata json files
#% description: Default is LOCATION/MAPSET/cell_misc/
#% gisprompt: old,file,file
#% answer: default
#% guisection: Metadata
#%end

#%option
#% key: nprocs
#% type: integer
#% required: no
#% multiple: no
#% label: Number of parallel processes to use
#% answer: 1
#%end

#%rules
#% requires_all: output_shadows,metadata
#% requires_all: threshold,metadata
#%end

import atexit
from datetime import datetime
import json
import os
import re
import sys
import multiprocessing as mp

import grass.script as grass
from grass.pygrass.modules import Module, ParallelModuleQueue


# initialize global vars
rm_regions = []
rm_vectors = []
rm_rasters = []


def cleanup():
    nuldev = open(os.devnull, 'w')
    kwargs = {
        'flags': 'f',
        'quiet': True,
        'stderr': nuldev
    }
    for rmr in rm_regions:
        if rmr in [x for x in grass.parse_command('g.list', type='region')]:
            grass.run_command(
                'g.remove', type='region', name=rmr, **kwargs)
    for rmv in rm_vectors:
        if grass.find_file(name=rmv, element='vector')['file']:
            grass.run_command(
                'g.remove', type='vector', name=rmv, **kwargs)
    for rmrast in rm_rasters:
        if grass.find_file(name=rmrast, element='raster')['file']:
            grass.run_command(
                'g.remove', type='raster', name=rmrast, **kwargs)


def test_nprocs():
    # Test nprocs settings
    nprocs = int(options['nprocs'])
    nprocs_real = mp.cpu_count()
    if nprocs > nprocs_real:
        grass.warning(
            "Using %d parallel processes but only %d CPUs available."
            % (nprocs, nprocs_real))


def main():

    global rm_regions, rm_rasters, rm_vectors, tmpfolder

    # parameters
    strds = options['input']
    strdsout = options['output_clouds']
    threshold = float(options['threshold'])

    test_nprocs()

    # test if necessary GRASS GIS addons are installed
    if not grass.find_program('i.sentinel.mask', '--help'):
        grass.fatal(_("The 'i.sentinel.mask' module was not found, install it first:") +
        "\n" +
        "g.extension i.sentinel")
    if not grass.find_program('i.sentinel.mask.worker', '--help'):
        grass.fatal(_("The 'i.sentinel.mask.worker' module was not found, install it first:") +
        "\n" +
        "g.extension i.sentinel.mask.worker url=...")


    strdsrasters = [x.split('|')[0] for x in grass.parse_command('t.rast.list', input=strds, flags='u')]
    times = [x.split('|')[2] for x in grass.parse_command('t.rast.list', input=strds, flags='u')]
    s2_scenes = dict()
    for strdsrast, time in zip(strdsrasters, times):
        parts = strdsrast.split('_')
        name = "%s_%s" % (parts[0],parts[1])
        band = parts[2]
        if not name in s2_scenes:
            s2_scene = {'B02': None, 'B03': None, 'B04': None, 'B08': None,
                'B8A': None, 'B11': None, 'B12': None, 'date': None}
            s2_scene['clouds'] = "%s_clouds" % name
            if options['output_shadows']:
                s2_scene['shadows'] = "%s_shadows" % name
                s2_scene['shadows'] = "%s_shadows" % name
            if threshold > 0 or options['output_shadows']:
                if options['metadata'] == 'default':
                    env = grass.gisenv()
                    json_standard_folder = os.path.join(env['GISDBASE'], env['LOCATION_NAME'], env['MAPSET'], 'cell_misc')
                    s2_scene['metadata'] = os.path.join(json_standard_folder, strdsrast, "description.json")
                elif options['metadata']:
                    json_standard_folder = options['metadata']
                    s2_scene['metadata'] = os.path.join(json_standard_folder, strdsrast, "description.json")
            s2_scenes[name] = s2_scene
        s2_scenes[name][band] = strdsrast
        if not s2_scenes[name]['date']:
            if '.' in time:
                dateformat = '%Y-%m-%d %H:%M:%S.%f'
            else:
                dateformat = '%Y-%m-%d %H:%M:%S'
            s2_scenes[name]['date'] = datetime.strptime(time, dateformat)

    # check if all input bands are in strds
    for key in s2_scenes:
        if any([val is None for key2, val in s2_scenes[key].items()]):
            grass.fatal(_("Not all needed bands are given"))

    grass.message(_("Find clouds (and shadows) in Sentinel scenes ..."))
    env = grass.gisenv()
    start_gisdbase = env['GISDBASE']
    start_location = env['LOCATION_NAME']
    start_cur_mapset = env['MAPSET']

    queue = ParallelModuleQueue(nprocs=options['nprocs'])
    bands = ['B02', 'B03', 'B04', 'B08', 'B8A', 'B11', 'B12']
    number_of_scenes = len(s2_scenes)
    number = 0
    for s2_scene_name in s2_scenes:
        s2_scene = s2_scenes[s2_scene_name]
        number += 1
        grass.message(_("Processing %d of %d scenes") % (number, number_of_scenes))
        if threshold > 0:
            with open(s2_scene['metadata'], 'r') as f:
                data = json.load(f)
            if threshold > float(data['CLOUDY_PIXEL_PERCENTAGE']):
                computingClouds = False
            else:
                computingClouds = True
        else:
            computingClouds = True
        for band in bands:
            rm_rasters.append("%s_double" % s2_scene[band])
        if computingClouds:
            kwargs = dict()
            if options['output_shadows']:
                kwargs['shadow_raster'] = s2_scene['shadows']
                kwargs['metadata'] = s2_scene['metadata']
                kwargs['shadow_threshold'] = 1000
                flags='s'
            else:
                flags='sc'
            newmapset = s2_scene['clouds']
            # grass.run_command(
            i_sentinel_mask = Module(
                'i.sentinel.mask.worker',
                blue="%s@%s" % (s2_scene['B02'], start_cur_mapset),
                green="%s@%s" % (s2_scene['B03'], start_cur_mapset),
                red="%s@%s" % (s2_scene['B04'], start_cur_mapset),
                nir="%s@%s" % (s2_scene['B08'], start_cur_mapset),
                nir8a="%s@%s" % (s2_scene['B8A'], start_cur_mapset),
                swir11="%s@%s" % (s2_scene['B11'], start_cur_mapset),
                swir12="%s@%s" % (s2_scene['B12'], start_cur_mapset),
                flags=flags,
                cloud_raster=s2_scene['clouds'],
                newmapset=newmapset,
                quiet=True,
                run_=False,
                **kwargs
            )
            queue.put(i_sentinel_mask)
    queue.wait()

    # verify that switching the mapset worked
    env = grass.gisenv()
    gisdbase = env['GISDBASE']
    location = env['LOCATION_NAME']
    cur_mapset = env['MAPSET']
    if cur_mapset != start_cur_mapset:
        grass.fatal("New mapset is %s, but should be %s" % (cur_mapset, start_cur_mapset))

    # copy maps to current mapset
    for s2_scene_name in s2_scenes:
        s2_scene = s2_scenes[s2_scene_name]
        newmapset = s2_scene['clouds']
        if grass.find_file(s2_scene['clouds'], element = 'raster',mapset = newmapset)['file']:
            grass.run_command('g.copy', raster="%s@%s,%s" % (s2_scene['clouds'], newmapset, s2_scene['clouds']))
        else:
            grass.run_command('r.mapcalc', expression="%s = null()" % s2_scene['clouds'])
        if options['output_shadows']:
            if grass.find_file(s2_scene['shadows'], element = 'raster',mapset = newmapset)['file']:
                grass.run_command('g.copy', raster="%s@%s,%s" % (s2_scene['shadows'], newmapset, s2_scene['shadows']))
            else:
                grass.run_command('r.mapcalc', expression="%s = null()" % s2_scene['shadows'])
        grass.utils.try_rmdir(os.path.join(gisdbase, location, newmapset))

    grass.message(_("Create space time raster data set of clouds ..."))
    grass.run_command(
        't.create', output=strdsout, title="Sentinel-2 cloud mask",
        desc="Sentinel-2 cloud mask", quiet=True)
    # create register file
    registerfile = grass.tempfile()
    file = open(registerfile, 'w')
    for s2_scene_name in s2_scenes:
        s2_scene = s2_scenes[s2_scene_name]
        file.write("%s|%s\n" % (s2_scene['clouds'], s2_scene['date'].strftime("%Y-%m-%d %H:%M:%S")))
    file.close()
    grass.run_command('t.register', input=strdsout, file=registerfile, quiet=True)
    # remove registerfile
    grass.try_remove(registerfile)

    if options['output_shadows']:
        grass.message(_("Create space time raster data set of shadows ..."))
        grass.run_command(
            't.create', output=options['output_shadows'], title="Sentinel-2 shadow mask",
            desc="Sentinel-2 shadow mask", quiet=True)
        # create register file
        registerfile = grass.tempfile()
        file = open(registerfile, 'w')
        for s2_scene_name in s2_scenes:
            s2_scene = s2_scenes[s2_scene_name]
            file.write("%s|%s\n" % (s2_scene['shadows'], s2_scene['date'].strftime("%Y-%m-%d %H:%M:%S")))
        file.close()
        grass.run_command('t.register', input=options['output_shadows'], file=registerfile, quiet=True)
        # remove registerfile
        grass.try_remove(registerfile)


if __name__ == "__main__":
    options, flags = grass.parser()
    atexit.register(cleanup)
    main()
