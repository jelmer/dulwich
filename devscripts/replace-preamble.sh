#!/usr/bin/zsh
perl -i -p0e "s{\Q$(cat PREAMBLE.py.old)\E}{$(cat devscripts/PREAMBLE.py)}g" dulwich/**/*.py bin/dul*
perl -i -p0e "s{\Q$(cat PREAMBLE.c.old)\E}{$(cat devscripts/PREAMBLE.c)}g" dulwich/*.c
