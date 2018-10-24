#!/bin/bash
bash ./setup_janus_env.sh
gremlin.sh -e ./init.db.groovy `pwd`/conf/test-db.properties

