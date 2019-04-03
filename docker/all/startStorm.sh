#!/bin/bash

storm nimbus &
sleep 15
storm supervisor &
storm jar /conf/write-0.1.0-jar-with-dependencies.jar com.issel.cenote.WriteTopology WriteTopology
storm ui 
