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

zipFilePath = "{0}/{1}".format(globalconf.THIRD_PARTY_DIR, JANUS_ZIP_NAME)
installDir = "{0}/{1}".format(globalconf.DEPLOY_DIR, JANUS_DIR_NAME)


def delete():
    print("==== DELETE ====")
    shutil.rmtree(installDir, ignore_errors=True)
    shutil.rmtree("db", ignore_errors=True)
    shutil.rmtree("log", ignore_errors=True)
    print("Done!")

def install():
    delete()
    print("==== INSTALL ====")
    print("install to: " + installDir)
    command = "unzip {0} -d {1}".format(zipFilePath, globalconf.DEPLOY_DIR)
    subprocess.call(command, shell=True)
    print("JanusGraph deploy done!")

def start():
    print("==== START ====")
    if not os.path.isdir(installDir):
        print("Janus is not installed! Install first!")
        sys.exit(1)
    command = "{0}/bin/janusgraph.sh start".format(installDir)
    subprocess.call(command, shell=True)
    command = "{0}/bin/janusgraph.sh status".format(installDir)
    subprocess.call(command, shell=True)
    print("JanusGraph starts!")

def stop():
    print("==== STOP ====")
    if not os.path.isdir(installDir):
        print("Janus is not installed! Install first!")
        sys.exit(1)
    command = "{0}/bin/janusgraph.sh stop".format(installDir)
    subprocess.call(command, shell=True)
    print("JanusGraph stops!")    

if command == "install":
    install()
elif command == "delete":
    delete()
elif command == "start":
    start()
elif command == "stop":
    stop()
else:
    print("Unknown command: " + command)

