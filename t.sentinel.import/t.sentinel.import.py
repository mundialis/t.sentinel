#!/usr/bin/env python3

############################################################################
#
# MODULE:       t.sentinel.import
#
# AUTHOR(S):    Anika Weinmann <bettge at mundialis.de>
#
# PURPOSE:      Downloads and imports multiple Sentinel-2 scenes in parallel
#               and creates a STRDS
#
# COPYRIGHT:	(C) 2020 by mundialis and the GRASS Development Team
#
#		This program is free software under the GNU General Public
#		License (>=v2). Read the file COPYING that comes with GRASS
#		for details.
#
#############################################################################

#%Module
#% description: Downloads and imports multiple Sentinel-2 scenes in parallel and creates a STRDS.
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

#%flag
#% key: e
#% description: Use ESA-style scenename/s to download from USGS
#%end

#%flag
#% key: a
#% description: run atmospheric correction with sen2cor before importing
#%end

#%option G_OPT_F_INPUT
#% key: settings
#% required: yes
#% label: Full path to settings file (user, password)
#%end

#%option
#% key: clouds
#% type: integer
#% description: Maximum cloud cover percentage for Sentinel scene
#% required: no
#% guisection: Filter
#% answer: 100
#%end

#%option
#% key: producttype
#% type: string
#% description: Sentinel-2 product type to filter
#% required: no
#% options: S2MSI1C,S2MSI2A,S2MSI2Ap
#% answer: S2MSI2A
#% guisection: Filter
#%end

#%option
#% key: start
#% type: string
#% description: Start date ('YYYY-MM-DD')
#% guisection: Filter
#%end

#%option
#% key: end
#% type: string
#% description: End date ('YYYY-MM-DD')
#% guisection: Filter
#%end

#%option
#% key: datasource
#% description: Data-Hub to download scenes from.
#% label: Default is ESA Copernicus Open Access Hub (ESA_COAH), but Sentinel-2 L1C data can also be acquired from USGS Earth Explorer (USGS_EE). Download from USGS is currently only available when used together with the scene_name option.
#% options: ESA_COAH,USGS_EE
#% answer: ESA_COAH
#% guisection: Filter
#%end

#%option
#% key: limit
#% type: integer
#% description: Maximum number of scenes to filter/download
#% required: no
#% guisection: Filter
#%end

#%option
#% key: s2names
#% type: string
#% required: no
#% multiple: yes
#% description: List of Sentinel-2 names or file with this list
#%end

#%option
#% key: sen2cor_path
#% required: no
#% type: string
#% label: Path to sen2cor home directory
#% description: e.g. /home/user/sen2cor
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

#%option G_OPT_MEMORYMB
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
#% collective: start, end, producttype
#% excludes: s2names, start, end, producttype
#% requires: -a, sen2cor_path
#% requires: -e, s2names
#%end


import atexit
from datetime import date
import multiprocessing as mp
import os
import psutil # pip3 install psutil
import re
import sys

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
        grass.warning(_(
            "Using {} parallel processes but only {} CPUs available. "
            "Setting nprocs to {}.").format(nprocs, nprocs_real, nprocs_real))
        options['nprocs'] = nprocs_real
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

    global rm_regions, rm_rasters, rm_vectors, tmpfolder

    # parameters
    if options['s2names']:
        s2names = options['s2names'].split(',')
        if os.path.isfile(s2names[0]):
            with open(s2names[0], 'r') as f:
                s2namesstr = f.read()
        else:
            s2namesstr = ','.join(s2names)
    tmpdirectory = options['directory']

    test_nprocs_memory()


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
                    "g.extension i.sentinel")

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

    # make distinct download and sen2cor directories
    try:
        download_dir = os.path.join(tmpdirectory, 'download_{}'.format(
            os.getpid()))
        os.makedirs(download_dir)
    except Exception as e:
        grass.fatal(_('Unable to create temp dir {}').format(download_dir))

    download_args = {
        'settings': options['settings'],
        'nprocs': options['nprocs'],
        'output': download_dir,
        'datasource': options['datasource'],
        'flags': 'f'
    }
    if options['limit']:
        download_args['limit'] = options['limit']
    if options['s2names']:
        download_args['flags'] += 's'
        download_args['scene_name'] = s2namesstr.strip()
        if options['datasource'] == 'USGS_EE':
            if flags['e']:
                download_args['flags'] += 'e'
            download_args['producttype'] = 'S2MSI1C'
    else:
        download_args['clouds'] = options['clouds']
        download_args['start'] = options['start']
        download_args['end'] = options['end']
        download_args['producttype'] = options['producttype']

    grass.run_command('i.sentinel.parallel.download',
                      **download_args)
    number_of_scenes = len(os.listdir(download_dir))
    nprocs_final = min(number_of_scenes, int(options['nprocs']))

    # run atmospheric correction
    if flags['a']:
        sen2cor_folder = os.path.join(tmpdirectory, 'sen2cor_{}'.format(
            os.getpid()))
        try:
            os.makedirs(sen2cor_folder)
        except Exception as e:
            grass.fatal(_(
                "Unable to create temporary sen2cor folder {}").format(
                sen2cor_folder))
        grass.message(_('Starting atmospheric correction with sen2cor...').format(nprocs_final))
        queue_sen2cor = ParallelModuleQueue(nprocs=nprocs_final)
        for idx, subfolder in enumerate(os.listdir(download_dir)):
            folderpath = os.path.join(download_dir, subfolder)
            for file in os.listdir(folderpath):
                if file.endswith('.SAFE'):
                    filepath = os.path.join(folderpath, file)
            output_dir = os.path.join(
                sen2cor_folder, 'sen2cor_result_{}'.format(idx))
            sen2cor_module = Module(
                'i.sentinel-2.sen2cor',
                input_file=filepath,
                output_dir=output_dir,
                sen2cor_path=options['sen2cor_path'],
                nprocs=1,
                run_=False
                # all remaining sen2cor parameters can be left as default
            )
            queue_sen2cor.put(sen2cor_module)
        queue_sen2cor.wait()
        download_dir = sen2cor_folder

    grass.message(_("Importing Sentinel scenes ..."))
    env = grass.gisenv()
    start_gisdbase = env['GISDBASE']
    start_location = env['LOCATION_NAME']
    start_cur_mapset = env['MAPSET']
    ### save current region
    id = str(os.getpid())
    currentregion = 'tmp_region_' + id
    grass.run_command('g.region', save=currentregion, flags='p')

    queue_import = ParallelModuleQueue(nprocs=nprocs_final)
    memory_per_proc = round(float(options['memory'])/nprocs_final)
    mapsetids = []
    importflag = 'r'
    if flags['i']:
        importflag += 'i'
    if flags['c']:
        importflag += 'c'
    json_standard_folder = os.path.join(env['GISDBASE'], env['LOCATION_NAME'], env['MAPSET'], 'cell_misc')
    if not os.path.isdir(json_standard_folder):
        os.makedirs(json_standard_folder)
    subfolders = []
    for idx, subfolder in enumerate(os.listdir(download_dir)):
        if os.path.isdir(os.path.join(download_dir, subfolder)):
            subfolders.append(subfolder)
            mapsetid = 'S2_import_%s' %(str(idx+1))
            mapsetids.append(mapsetid)
            directory = os.path.join(download_dir, subfolder)
            i_sentinel_import = Module(
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
        grass.fatal("New mapset is <%s>, but should be <%s>" % (cur_mapset, start_cur_mapset))
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
            # set nulls
            grass.run_command('i.zero2null', map=rast, quiet=True)
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
            if flags['i'] and ('20' in band or '60' in band):
                band.replace('20', '10').replace('60', '10')
            grass.run_command('t.rast.extract', input=strds, where="name like '%" + band + "%'", output="%s_%s" % (strds, band), quiet=True)
            grass.message("<%s_%s> is created" % (strds, band))


if __name__ == "__main__":
    options, flags = grass.parser()
    atexit.register(cleanup)
    main()
