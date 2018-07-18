#!python2
import sys
import os
import shutil
import subprocess

import globalconf

# Static CONF
JANUS_ZIP_NAME = "janusgraph-0.2.1-hadoop2.zip"
JANUS_DIR_NAME = "janusgraph-0.2.1-hadoop2"

print("===== OPERATE JANUSGRAPH =====")

command = sys.argv[1]
cwd = os.getcwd()

print("command: " + command)
print("cwd: " + cwd)
print("third party: " + globalconf.THIRD_PARTY_DIR)
print("deploy: " + globalconf.DEPLOY_DIR)


def delete():
    print("==== DELETE ====")
    installDir = "{0}/{1}".format(globalconf.DEPLOY_DIR, JANUS_DIR_NAME)
    shutil.rmtree(installDir, ignore_errors=True)
    print("Done!")

def deploy():
    delete()
    print("==== DEPLOY ====")
    installPath = cwd + "/" + "tmp-deploy"
    print("install to: " + installPath)
    janusZipFile = globalconf.THIRD_PARTY_DIR + "/" + JANUS_ZIP_NAME
    command = "unzip {0} -d {1}".format(janusZipFile, installPath)
    subprocess.call(command, shell=True)
    print("JanusGraph deploy done!")

if command == "deploy":
    deploy()
elif command == "delete":
    delete()
else:
    print("Unknown command: " + command)

