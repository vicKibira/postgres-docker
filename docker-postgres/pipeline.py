import pandas as pd
import os
import logging
import sys
import argparse



logging.info(sys.argv)

day = sys.argv[1]

#doing some fancy stuff with pandas just to see whether we can build a docker image that has pandas
logging.info(F"job completed successfully on  day = {day}")