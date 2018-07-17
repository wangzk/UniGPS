#!/bin/bash
./setup_janus_env.sh
gremlin-server.sh `pwd`/conf/test-gremlin-server.yaml
