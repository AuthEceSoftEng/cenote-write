#!/bin/bash

storm nimbus &
wait 15
storm supervisor 
