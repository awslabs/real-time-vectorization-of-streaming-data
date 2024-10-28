# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# Apache-2.0

import os
import json
import cfnresponse
import math
import random

def handler(event, context):
    try:
        print("Received Event:" + json.dumps(event))
        disambiguitorLength = int(event["ResourceProperties"]["disambiguitorLength"])
        characters = "abcdefghijklmnopqrstuvwxyz0123456789"
        result = ""
        for i in range(disambiguitorLength):
            result += characters[math.floor(random.random() * len(characters))]
        cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData={ "Disambiguitor": result })
    except Exception as err:
        print(err)
        cfnresponse.send(event, context, cfnresponse.FAILED, err)
