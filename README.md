[![Build Status](https://github.com/IGBIllinois/cbc_tools/actions/workflows/main.yml/badge.svg)](https://github.com/IGBIllinois/cbc_tools/actions/workflows/main.yml)


# cbc_tools
Tools written by CNRG for use by the Carver Biotech Center

## generate_report.py
A program to take project data as created by bcltofastq from cbc_scripts and prepare that data to be posted for retrieval by the end user.  This program:
1.  Makes .tar.gz files of the fastq data
2.  Generates an excel report that displays counts of the sequences in each file as well as the preparation
3.  Uses falco to do QC on the fastq files
4.  Makes .tar.gz files of the falco data

Programs Required to run:
* tar
* pigz
* falco

Python Packages Used
* os
* re
* sys
* subprocess
* xlsxwriter
* json
* xml.etree.ElementTree
* csv
* argparse
* pathlib
* shutil
* time
