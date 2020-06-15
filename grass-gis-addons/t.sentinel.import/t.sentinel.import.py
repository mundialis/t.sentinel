#!/usr/bin/env python3

############################################################################
#
# MODULE:       t.sentinel.import
#
# AUTHOR(S):    Anika Bettge <bettge at mundialis.de>
#
# PURPOSE:      Downloads and imports the Sentinel-2 scenes and create a STRDS
#
# COPYRIGHT:	(C) 2020 by mundialis and the GRASS Development Team
#
#		This program is free software under the GNU General Public
#		License (>=v2). Read the file COPYING that comes with GRASS
#		for details.
#
#############################################################################

#%Module
#% description: downloads and imports Sentinel-2 scenes and creates a STRDS.
#% keyword: temporal
#% keyword: satellite
#% keyword: Sentinel
#% keyword: download
#% keyword: import
#%end

#%flag
#% key: c
#% description: Import cloud masks as raster maps
#% guisection: Settings
#%end

#%flag
#% key: i
#% description: Resample 20/60m bands to 10m using r.resamp.interp
#%end

#%flag
#% key: f
#% description: Use footprint to set null values TODO!!!
#% guisection: Settings
#%end

#%option G_OPT_F_INPUT
#% key: settings
#% label: Full path to settings file (user, password)
#%end

#%option
#% key: s2names
#% type: string
#% required: yes
#% multiple: yes
#% description: List of Sentinel-2 names or file with this list
#%end

#%option
#% key: pattern
#% type: string
#% required: no
#% multiple: no
#% description: Band name pattern to import
#% guisection: Filter
#%end

#%option
#% key: strds_output
#% type: string
#% required: no
#% multiple: no
#% key_desc: name
#% description: Name of the output space time dataset
#% gisprompt: new,stds,strds
#%end

#%option
#% key: directory
#% type: string
#% required: no
#% multiple: no
#% description: Directory to hold temporary files (they can be large)
#%end

#%option
#% key: memory
#% type: integer
#% required: no
#% multiple: no
#% label: Maximum memory to be used (in MB)
#% description: Cache size for raster rows
#% answer: 300
#%end

#%option
#% key: nprocs
#% type: integer
#% required: no
#% multiple: no
#% label: Number of parallel processes to use
#% answer: 1
#%end


import atexit
from datetime import date
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
tmpfolder = None


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
    if tmpfolder:
        grass.try_rmdir(os.path.join(tmpfolder))


def test_nprocs_memory():
    # Test nprocs settings
    nprocs = int(options['nprocs'])
    nprocs_real = mp.cpu_count()
    if nprocs > nprocs_real:
        grass.warning(
            "Using %d parallel processes but only %d CPUs available."
            % (nprocs, nprocs_real))
    # check momory
    memory = int(options['memory'])
    free_ram = abs(freeRAM('MB', 100))
    if free_ram < memory:
        grass.warning(
            "Using %d MB but only %d MB RAM available."
            % (memory, free_ram))
        options['memory'] = free_ram
        grass.warning(
            "Set used memory to %d MB." % (options['memory']))


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
    # parse 'free' output for RAM/SWAP usage
    try:
        if "alpine" in os.popen('cat /etc/os-release').readlines()[0].strip().split('=')[1].lower():
            os.popen('apk add freetype-dev')
        tot_m, used_m, free_m = map(
            int, os.popen('free -m -t').readlines()[-2].split()[1:4])
        swap_tot_m, swap_used_m, swap_free_m = map(
            int, os.popen('free -m -t').readlines()[-1].split()[1:4])
        memory_GB = (tot_m - swap_tot_m)/1024
        memory_MB = (tot_m - swap_tot_m)

        if unit == "MB":
            memory_MB_percent = memory_MB * percent / 100.0
            return int(round(memory_MB_percent))
        elif unit == "GB":
            memory_GB_percent = memory_GB * percent / 100.0
            return int(round(abs(memory_GB_percent)))
        else:
            grass.fatal("unit %s not supported." % unit)
    except:
        grass.warning("Free RAM is not checked")


def main():

    global rm_regions, rm_rasters, rm_vectors, tmpfolder

    # parameters
    s2names = options['s2names'].split(',')
    tmpdirectory = options['directory']

    test_nprocs_memory()

    grass.message(_("Downloading Sentinel scenes ..."))
    if not grass.find_program('i.sentinel.download', '--help'):
        grass.fatal(_("The 'i.sentinel.download' module was not found, install it first:") +
                    "\n" +
                    "g.extension i.sentinel")
    if not grass.find_program('i.sentinel.import', '--help'):
        grass.fatal(_("The 'i.sentinel.import' module was not found, install it first:") +
                    "\n" +
                    "g.extension i.sentinel")
    if not grass.find_program('i.sentinel.parallel.download', '--help'):
        grass.fatal(_("The 'i.sentinel.parallel.download' module was not found, install it first:") +
                    "\n" +
                    "g.extension i.sentinel.parallel.download url=...")

    # create temporary directory to download data
    if tmpdirectory:
        if not os.path.isdir(tmpdirectory):
            try:
                os.makedirs(tmpdirectory)
            except:
                grass.fatal(_("Unable to create temp dir"))
    else:
        tmpdirectory = grass.tempdir()
        tmpfolder = tmpdirectory

    if os.path.isfile(s2names[0]):
        with open(s2names[0], 'r') as f:
            s2namesstr = f.read()
    else:
        s2namesstr = ','.join(s2names)

    grass.run_command(
        'i.sentinel.parallel.download',
        settings=options['settings'],
        scene_name=s2namesstr,
        nprocs=options['nprocs'],
        output=tmpdirectory,
        flags="fs",
        quiet=True)

    grass.message(_("Importing Sentinel scenes ..."))
    env = grass.gisenv()
    start_gisdbase = env['GISDBASE']
    start_location = env['LOCATION_NAME']
    start_cur_mapset = env['MAPSET']

    if len(s2namesstr.split(',')) < int(options['nprocs']):
        procs_import = len(s2namesstr.split(','))
    else:
        procs_import = int(options['nprocs'])
    ### save current region
    id = str(os.getpid())
    currentregion = 'tmp_region_' + id
    grass.run_command('g.region', save=currentregion, flags='p')

    queue_import = ParallelModuleQueue(nprocs=procs_import)
    memory_per_proc = round(float(options['memory'])/procs_import)
    mapsetids = []
    importflag = 'r'
    if flags['i']:
        importflag += 'i'
    if flags['c']:
        importflag += 'c'
    json_standard_folder = os.path.join(env['GISDBASE'], env['LOCATION_NAME'], env['MAPSET'], 'cell_misc')
    if not os.path.isdir(json_standard_folder):
        os.makedirs(json_standard_folder)
    for idx,subfolder in enumerate(os.listdir(tmpdirectory)):
        if os.path.isdir(os.path.join(tmpdirectory, subfolder)):
            mapsetid = 'S2_import_%s' %(str(idx+1))
            mapsetids.append(mapsetid)
            directory = os.path.join(tmpdirectory, subfolder)
            i_sentinel_import = Module(
            # grass.run_command(
                'i.sentinel.import.worker',
                input=directory,
                mapsetid=mapsetid,
                memory=memory_per_proc,
                pattern=options['pattern'],
                flags=importflag,
                region=currentregion,
                metadata=json_standard_folder,
                run_=False
            )
            queue_import.put(i_sentinel_import)
    queue_import.wait()
    grass.run_command('g.remove', type='region', name=currentregion, flags='f')

    # verify that switching the mapset worked
    env = grass.gisenv()
    gisdbase = env['GISDBASE']
    location = env['LOCATION_NAME']
    cur_mapset = env['MAPSET']
    if cur_mapset != start_cur_mapset:
        grass.fatal("New mapset is %s, but should be %s" % (cur_mapset, start_cur_mapset))

    # copy maps to current mapset
    maplist = []
    cloudlist = []
    for new_mapset in mapsetids:
        for vect in grass.parse_command('g.list', type='vector', mapset=new_mapset):
            cloudlist.append(vect)
            grass.run_command('g.copy', vector=vect + '@' + new_mapset + ',' + vect)
        for rast in grass.parse_command('g.list', type='raster', mapset=new_mapset):
            maplist.append(rast)
            grass.run_command('g.copy', raster=rast + '@' + new_mapset + ',' + rast)
        grass.utils.try_rmdir(os.path.join(gisdbase, location, new_mapset))

    # space time dataset
    grass.message(_("Creating STRDS of Sentinel scenes ..."))
    if options['strds_output']:
        strds = options['strds_output']
        grass.run_command(
            't.create', output=strds, title="Sentinel-2",
            desc="Sentinel-2", quiet=True)

        # create register file
        registerfile = grass.tempfile()
        file = open(registerfile, 'w')
        for imp_rast in list(set(maplist)):
            date_str1 = imp_rast.split('_')[1].split('T')[0]
            date_str2 = "%s-%s-%s" % (date_str1[:4], date_str1[4:6], date_str1[6:])
            time_str = imp_rast.split('_')[1].split('T')[1]
            clock_str2 = "%s:%s:%s" % (time_str[:2], time_str[2:4], time_str[4:])
            file.write("%s|%s %s\n" % (imp_rast, date_str2, clock_str2))
        file.close()
        grass.run_command('t.register', input=strds, file=registerfile, quiet=True)
        # remove registerfile
        grass.try_remove(registerfile)

        if flags['c']:
            stvdsclouds = strds + '_clouds'
            grass.run_command(
                't.create', output=stvdsclouds, title="Sentinel-2 clouds",
                desc="Sentinel-2 clouds", quiet=True, type='stvds')
            registerfileclouds = grass.tempfile()
            fileclouds = open(registerfileclouds, 'w')
            for imp_clouds in cloudlist:
                date_str1 = imp_clouds.split('_')[1].split('T')[0]
                date_str2 = "%s-%s-%s" % (date_str1[:4], date_str1[4:6], date_str1[6:])
                time_str = imp_clouds.split('_')[1].split('T')[1]
                clock_str2 = "%s:%s:%s" % (time_str[:2], time_str[2:4], time_str[4:])
                fileclouds.write("%s|%s %s\n" % (imp_clouds, date_str2, clock_str2))
            fileclouds.close()
            grass.run_command(
                't.register', type='vector', input=stvdsclouds, file=registerfileclouds, quiet=True)
            grass.message("<%s> is created" % (stvdsclouds))
            # remove registerfile
            grass.try_remove(registerfileclouds)

        # extract strds for each band
        bands = []
        pattern = options['pattern']
        if "(" in pattern:
            global beforebrackets, afterbrackets
            beforebrackets = re.findall(r"(.*?)\(", pattern)[0]
            inbrackets = re.findall(r"\((.*?)\)", pattern)[0]
            afterbrackets = re.findall(r"\)(.*)", pattern)[0]
            bands = ["%s%s%s" % (beforebrackets, x, afterbrackets) for x in inbrackets.split('|')]
        else:
            bands = pattern.split('|')

        for band in bands:
            if flags['i'] and ( '20' in band or '60' in band ):
                band.replace('20', '10').replace('60', '10')
            grass.run_command('t.rast.extract', input=strds, where="name like '%" + band + "%'", output="%s_%s" % (strds, band), quiet=True)
            grass.message("<%s_%s> is created" % (strds, band))


if __name__ == "__main__":
    options, flags = grass.parser()
    atexit.register(cleanup)
    main()
