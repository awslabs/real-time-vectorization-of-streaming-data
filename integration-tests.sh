#!/bin/bash

while getopts t:b: option
do
    case "${option}"
        in
        t)testConfigFile=${OPTARG};;
        b)blueprintCDKTemplateURL=${OPTARG};;
    esac
done

mvn clean verify -DskipUTs=true -DskipITs=false -DintegTestInputsFile=$testConfigFile -DblueprintCDKTemplateURL=$blueprintCDKTemplateURL