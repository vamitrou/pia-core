#!/usr/bin/env python

import os
import sys
import yaml
import getopt
import random
import inspect
import hashlib


DIR_APPLICATIONS = "applications"
DIR_APP_NAMES = "application_names"


def main(argv):
    name = ""
    lang = ""
    try:
        opts, args = getopt.getopt(argv, "hnl", ["help", "name=", "lang="])
        if not opts:
            usage()
            sys.exit(2)
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage()
            sys.exit()
        elif opt in ("-n", "--name"):
            name = arg
        elif opt in ("-l", "--lang"):
            lang = arg
    if len(name) == 0 or len(lang) == 0:
        usage()
        sys.exit(2)
    edit_config(name, lang)


def usage():
    print "\nApplication configuration utility\n"
    print "Usage: " + sys.argv[0] + " --name [app_name] --lang [app_language]"


def edit_config(name, lang):
    try:
        with open("catalog.yml") as f:
            catalog = yaml.load(f.read())
    except:
        catalog = {"applications": []}

    for app in catalog["applications"]:
        if app["name"] == name:
            print "Application %s already exists with ID: %s" % (name, app["id"])
            sys.exit(2)


    app_hash = hashlib.sha1(str(random.getrandbits(2048))).hexdigest()
    catalog["applications"].append({"name": name, "id": app_hash, "prog_lang": lang})

    with open("catalog.yml", "w") as f:
        f.write(yaml.safe_dump(catalog, default_flow_style=False))

    ensure_dir("%s/%s" % (DIR_APPLICATIONS, app_hash))
    ensure_dir("%s" % DIR_APP_NAMES)

    cur_path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    os.symlink("%s/%s/%s" % (cur_path, DIR_APPLICATIONS, app_hash),
               "%s/%s/%s" % (cur_path, DIR_APP_NAMES, name))

    print "Application %s created." % name


def ensure_dir(dir_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)


if __name__ == "__main__":
    main(sys.argv[1:])
