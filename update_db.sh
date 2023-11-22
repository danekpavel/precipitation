#!/bin/bash

cd /home/danekpavel/precipitation
git pull
python -m scripts.update_db
