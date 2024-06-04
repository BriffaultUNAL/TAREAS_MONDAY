#!/usr/bin/python

from src.utils import *
import sys
import os

if __name__ == "__main__":

    # webscraping(**source2)
    load(transform_colombia(extract(os.path.join(act_dir, '..', 'data', file_name))))
    # load(transform_mexico(extract(os.path.join(act_dir, '..', 'data', file_name))))
    # transform(extract(os.path.join(act_dir, '..', 'data', file_name)))
