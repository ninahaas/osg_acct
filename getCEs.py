#!/usr/bin/python2

import os
import xml.etree.ElementTree as ET

tree = ET.fromstring(os.popen("curl 'https://topology.opensciencegrid.org/rgsummary/xml?service&service_1=on'").read())
print("\n".join(x.text for x in tree.findall("./ResourceGroup/Facility/Name")))
