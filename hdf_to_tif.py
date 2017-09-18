#!/usr/bin/env python3

"""
Copyright 2017 Erik Perillo <erik.perillo@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 SOFTWARE.
"""

from collections import OrderedDict
import subprocess as sp
import tempfile
import shutil
import glob
import oarg
import os

#command for hdf-tif conversion tool
CVT_CMD = "/home/erik/bin/heg/bin/resample"
CVT_LOG_FILENAME = "resample.log"
#command for hdf file stat tool
STAT_CMD = "/home/erik/bin/heg/bin/hegtool"
STAT_OUT_FILENAME = "HegHdr.hdr"
STAT_LOG_FILENAME = "hegtool.log"
#command for reprojecting
WARP_CMD = "/usr/bin/gdalwarp"

#environment variables
ENV = {
    "HEGUSER": "BOB",
    "MRTDATADIR": "/home/erik/bin/heg/data",
    "PGSHOME": "/home/erik/bin/heg/TOOLKIT_MTD"
}

#default configuration parameters for conversion tool
DEF_CONF = OrderedDict({
    "input_filename": "",
    "object_name": "",
    "field_name": "",
    "band_number": "1",
    "spatial_subset_ul_corner": "",
    "spatial_subset_lr_corner": "",
    "resampling_type": "BI",
    "output_projection_type": "GEO",
    "ellipsoid_code": "WGS84",
    "output_projection_parameters": \
        "( 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0  )",
    "output_filename": "",
    "output_type": "GEO",
})

#specific configurations for MODIS products
CONFS = {
    "MOD13Q1": {
        "object_name": "MODIS_Grid_16DAY_250m_500m_VI|",
    },
    "MYD13Q1": {
        "object_name": "MODIS_Grid_16DAY_250m_500m_VI|",
    },
}

#supported products
PRODUCTS = list(CONFS.keys())

#bands mapping for MODIS products
BANDS = {
    "MOD13Q1": {
        "ndvi": "250m 16 days NDVI",
        "evi": "250m 16 days EVI",
        "vi-quality": "250m 16 days VI Quality",
        "red": "250m 16 days red reflectance",
        "nir": "250m 16 days NIR reflectance",
        "blue": "250m 16 days blue reflectance",
        "mir": "250m 16 days MIR reflectance",
        "day": "250m 16 days composite day of the year",
        "pix-rel": "250m 16 days pixel reliability",
    },
    "MYD13Q1": {
        "ndvi": "250m 16 days NDVI",
        "evi": "250m 16 days EVI",
        "vi-quality": "250m 16 days VI Quality",
        "red": "250m 16 days red reflectance",
        "nir": "250m 16 days NIR reflectance",
        "blue": "250m 16 days blue reflectance",
        "mir": "250m 16 days MIR reflectance",
        "day": "250m 16 days composite day of the year",
        "pix-rel": "250m 16 days pixel reliability",
    }
}

def mk_conf_str(conf):
    """
    Formats configuration in HEG program format.
    """
    conf_str = "\n".join([
        "{} = {}".format(k.upper(), v) for k, v in conf.items()
    ])
    conf_str = "\n".join([
        "",
        "NUM_RUNS = 1",
        "",
        "BEGIN",
        conf_str,
        "END",
        ""
    ])
    return conf_str

def mk_conf_file(conf):
    """
    Creates config file in temporary file.
    """
    tmp_fp = tempfile.mktemp()
    with open(tmp_fp, "w") as f:
        print(mk_conf_str(conf), file=f)
    return tmp_fp

def error(msg, code=1):
    """
    Prints error message and exits.
    """
    print("error:", msg)
    exit(code)

def run_convert_cmd(params_fp, verbose=True):
    """
    Wrapper for conversion command.
    """
    cmd = [
        CVT_CMD,
        "-p",
        params_fp
    ]
    proc = sp.run(cmd, stdout=None if verbose else sp.PIPE, check=True)
    return proc

def run_stat_cmd(inp_fp, verbose=True):
    """
    Wrapper for stat command.
    """
    cmd = [
        STAT_CMD,
        "-h",
        inp_fp
    ]
    proc = sp.run(cmd, stdout=None if verbose else sp.PIPE, check=True)
    return proc

def run_warp_cmd(inp_fp, out_fp, proj, verbose=True):
    """
    Wrapper for reprojection command.
    """
    cmd = [
        WARP_CMD,
        inp_fp,
        out_fp,
        "-t_srs",
        proj.upper()
    ]
    proc = sp.run(cmd, stdout=None if verbose else sp.PIPE, check=True)
    return proc

def stat(inp_fp, verbose=True):
    """
    Gets information from HDF file.
    """
    #running command
    run_stat_cmd(inp_fp, verbose=verbose)

    #deleting unwanted log file
    log_fp = STAT_LOG_FILENAME
    if os.path.isfile(log_fp):
        os.remove(log_fp)

    #parsing output of stat
    out_fp = STAT_OUT_FILENAME
    with open(out_fp, "r") as f:
        text = f.read().replace("\\\n", "").replace("\t", " ")
    lines = text.split("\n")
    lines = [l.strip(" ") for l in lines if l.strip(" ")]
    #deleting file with output of command
    os.remove(out_fp)

    params = {l.split("=")[0].lower(): l.split("=")[1] for l in lines}
    return params

def hdf_to_tif(inp_fp, out_fp, band, proj, verbose=True):
    """
    Converts hdf to tif file, optionally projecting output.
    """
    #updating env variables
    os.environ.update(ENV)

    #getting params of input file
    params = stat(inp_fp, verbose=verbose)
    #print("params:", params)

    #setting up configuration
    conf = dict(DEF_CONF)
    #input/output filenames
    conf["input_filename"] = inp_fp
    conf["output_filename"] = out_fp
    #spatial stuff
    conf["spatial_subset_ul_corner"] =\
        "( {} )".format(params["grid_ul_corner_latlon"])
    conf["spatial_subset_lr_corner"] =\
        "( {} )".format(params["grid_lr_corner_latlon"])
    #name of object
    conf["object_name"] = params["grid_names"].replace(",", "") + "|"
    #band to use
    product = params["input_shortname"]
    conf["field_name"] = BANDS.get(product, {}).get(band, band)

    #making configuration file
    conf_fp = mk_conf_file(conf)

    #calling command
    run_convert_cmd(conf_fp, verbose=verbose)
    #removing unwanted files
    for fp in [CVT_LOG_FILENAME, conf_fp, out_fp + ".met"]\
        + glob.glob("filetable.temp_*") + glob.glob("GetAttrtemp_*"):
        if os.path.isfile(fp):
            os.remove(fp)

    #reprojecting if required
    if proj:
        tmp = tempfile.mktemp()
        run_warp_cmd(out_fp, tmp, proj, verbose=verbose)
        shutil.move(tmp, out_fp)

def main():
    #command-line args
    inp_fp = oarg.Oarg("-i --input", "", "input filepath", 0)
    band = oarg.Oarg("-b --band", "", "band name", 1)
    out_fp = oarg.Oarg("-o --output", "", "output filepath", 3)
    silence = oarg.Oarg("-s --silence", False, "suppress convert tool output")
    proj = oarg.Oarg("-p --projection", "", "warp from geo to EPSG:XXXX")
    hlp = oarg.Oarg("-h --help", False, "this help message")
    oarg.parse()

    #help message
    if hlp.val:
        oarg.describe_args("options:", def_val=True)
        return

    #checking args validity
    if not inp_fp.found:
        error("must provide input filepath (use --help)")
    if not band.found:
        error("must provide band (use --help)")
    if not out_fp.found:
        if not inp_fp.val[-4:].lower() == ".hdf":
            out_fp = inp_fp.val + ".tif"
        else:
            out_fp = inp_fp.val[:-4] + ".tif"
    else:
        out_fp = out_fp.val

    #converting
    hdf_to_tif(inp_fp.val, out_fp, band.val, proj.val, not silence.val)

if __name__ == "__main__":
    main()
