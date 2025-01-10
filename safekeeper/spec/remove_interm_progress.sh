#!/bin/bash

awk -f remove_interm_progress.awk $1 > $1.thin