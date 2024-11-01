# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# Apache-2.0

import boto3
import os
import json
import cfnresponse
import datetime

def handler(event, context):
    try:
        print("Received Event:" + json.dumps(event))
        if(event["RequestType"] == "Create"):
            vpcId = os.environ["vpc_id"]
            region = os.environ["region"]
            print("Received vpcId: " + vpcId)
            props = event["ResourceProperties"]
            print("Received properties: " + str(props))
            endpointType = props["endpointType"]
            createIfNotExists = props["createIfNotExists"]
            subnetIds = props["subnetIds"].split(",")
            securityGroups = props["securityGroups"]
            ec2Client = boto3.client("ec2")
            vpcEndpointsResponse = ec2Client.describe_vpc_endpoints(
                Filters=[
                {
                    "Name": "vpc-id",
                    "Values": [vpcId]
                }
            ])
            vpc_endpoints = vpcEndpointsResponse["VpcEndpoints"]
            print("VPC endpoints: ", vpc_endpoints)

            if (endpointType == "BEDROCK"):
                exists, vpce_id = check_bedrock_endpoints(vpc_endpoints)
                print("Bedrock VPC endpoint already exists: " + str(exists))
                responseData = {
                    "HasExistingVpcEndpoints": str(exists),
                    "CreatedVpcEndpoints": str(False),
                    "VpcEndpointId": vpce_id
                }
                if createIfNotExists and not exists:
                    createResponse = create_bedrock_endpoint(ec2Client, region, vpcId, subnetIds, securityGroups)
                    print("Created Bedrock VPC endpoint response: " + str(createResponse))
                    responseData["CreatedVpcEndpoints"] = str(True)
                    responseData["VpcEndpointId"] = createResponse["VpcEndpoint"]["VpcEndpointId"]
                cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData=responseData)

            elif (endpointType == "OPENSEARCH-PROVISIONED"):
                domainName = props["domainName"]
                in_vpc = check_opensearch_provisioned_in_vpc(domainName)
                if not in_vpc:
                    print("OpenSearch provisioned domain is public and not in a VPC. Not creating VPC endpoints.")
                    responseData = {
                        "HasExistingVpcEndpoints": str(False),
                        "CreatedVpcEndpoints": str(False),
                        "VpcEndpointId": "Not applicable. This OpenSearch domain is public and not in a VPC."
                    }
                    cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData=responseData)
                    return
                exists, vpce_id = check_opensearch_provisioned_vpc_endpoint(vpc_endpoints, vpcId, subnetIds, securityGroups)
                print("Opensearch provisioned VPC endpoint(s) already exists: " + str(exists))
                responseData = {
                    "HasExistingVpcEndpoints": str(exists),
                    "CreatedVpcEndpoints": str(False),
                    "VpcEndpointId": vpce_id
                }
                if createIfNotExists and not exists:
                    createResponse = create_opensearch_provisioned_endpoint(domainName, subnetIds, securityGroups)
                    print("Created Opensearch provisioned VPC endpoint response: " + str(createResponse))
                    responseData["CreatedVpcEndpoints"] = str(True)
                    responseData["VpcEndpointId"] = createResponse["VpcEndpoint"]["VpcEndpointId"]
                cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData=responseData)

            elif (endpointType == "OPENSEARCH-SERVERLESS"):
                exists, vpce_id = check_opensearch_serverless_vpc_endpoint(vpc_endpoints)
                print("Opensearch serverless VPC endpoint already exists: " + str(exists))
                responseData = {
                    "HasExistingVpcEndpoints": str(exists),
                    "CreatedVpcEndpoints": str(False),
                    "VpcEndpointId": vpce_id
                }
                if createIfNotExists and not exists:
                    createResponse = create_opensearch_serverless_endpoint(vpcId, subnetIds, securityGroups)
                    print("Created serverless Opensearch VPC endpoint response: " + str(createResponse))
                    responseData["CreatedVpcEndpoints"] = str(True)
                    responseData["VpcEndpointId"] = createResponse["createVpcEndpointDetail"]["id"]
                cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData=responseData)

            else:
                cfnresponse.send(event, context, cfnresponse.FAILED, "invalid endpoint type")

        elif(event["RequestType"] == "Delete"):
            cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData={
                "response": "No-op at delete. Please manually clean up any VPC endpoints if needed."
            })

    except Exception as err:
            print(err)
            cfnresponse.send(event, context, cfnresponse.FAILED, err)

def check_bedrock_endpoints(vpc_endpoints):
    print("Checking Bedrock endpoints")
    for vpc_endpoint in vpc_endpoints:
        if ".bedrock-runtime" in vpc_endpoint["ServiceName"]:
            print("Bedrock VPC endpoint found: ", vpc_endpoint["VpcEndpointId"])
            return True, vpc_endpoint["VpcEndpointId"]
    return False, ""

def check_opensearch_provisioned_vpc_endpoint(vpc_endpoints, vpcId, subnetIds, securityGroups):
    print("Checking opensearch provisioned endpoints")
    existing_os_vpce_ids = []
    for vpc_endpoint in vpc_endpoints:
        tags = vpc_endpoint.get("Tags", [])
        opensearch_managed = any(tag for tag in tags if tag["Key"] == "OpenSearchManaged" and tag["Value"] == "true")
        if opensearch_managed:
            found_vpc_id = vpc_endpoint.get("VpcId", "")
            found_subnet_ids = vpc_endpoint.get("SubnetIds", [])
            found_security_groups = [group["GroupId"] for group in vpc_endpoint.get("Groups", [])]
            vpc_ids_match = (found_vpc_id == vpcId)
            subnet_ids_contains = not any(subnet for subnet in subnetIds if subnet not in found_subnet_ids)
            security_groups_contains = not any(group for group in securityGroups if group not in found_security_groups)
            if vpc_ids_match and subnet_ids_contains and security_groups_contains:
                vpce_id = vpc_endpoint["VpcEndpointId"]
                os_vpce_ids = [tag for tag in tags if tag.get("Key", "") == "OpenSearchVPCEndpointId" and tag.get("Value", "") != ""]
                if len(os_vpce_ids) == 1:
                    existing_os_vpce_ids.append(os_vpce_ids[0]["Value"])
                else:
                    existing_os_vpce_ids.append(vpce_id)
    if len(existing_os_vpce_ids) > 0:
        os_vpce_ids_str = ", ".join(existing_os_vpce_ids)
        print(f"OpenSearch provisioned VPC Endpoint(s): ", os_vpce_ids_str)
        return True, os_vpce_ids_str
    return False, ""

def check_opensearch_serverless_vpc_endpoint(vpc_endpoints):
    print("Checking opensearch endpoints")
    for vpc_endpoint in vpc_endpoints:
        for dns_entry in vpc_endpoint["DnsEntries"]:
            if "aoss.searchservices.aws.dev" in dns_entry["DnsName"]:
                print("OpenSearch serverless VPC endpoint: " + vpc_endpoint["VpcEndpointId"])
                return True, vpc_endpoint["VpcEndpointId"]
    return False, ""

def create_bedrock_endpoint(ec2Client, region, vpcId, subnetIds, securityGroups):
    vpcDisambiguitor = vpcId[len(vpcId) - 4:]
    vpcEndpoint = ec2Client.create_vpc_endpoint(
        DryRun=False,
        VpcEndpointType="Interface",
        VpcId=vpcId,
        ServiceName="com.amazonaws." + region + ".bedrock-runtime",
        SubnetIds=subnetIds,
        SecurityGroupIds=securityGroups,
        PrivateDnsEnabled=True,
        TagSpecifications=[
            {
                "ResourceType": "vpc-endpoint",
                "Tags": [
                    {
                        "Key": "Name",
                        "Value": "real-time-vec-embed-bedrock-" + vpcDisambiguitor,
                    },
                ]
            },
        ])
    return vpcEndpoint

def create_opensearch_serverless_endpoint(vpcId, subnetIds, securityGroups):
    osServerlessClient = boto3.client("opensearchserverless")
    vpcDisambiguitor = vpcId[len(vpcId) - 4:]
    vpcEndpoint = osServerlessClient.create_vpc_endpoint(
        name="real-time-vec-embed-aoss-" + vpcDisambiguitor,
        securityGroupIds=securityGroups,
        subnetIds=subnetIds,
        vpcId=vpcId)
    return vpcEndpoint

def create_opensearch_provisioned_endpoint(domainName, subnetIds, securityGroups):
    osClient = boto3.client("opensearch")
    domainResponse = osClient.describe_domain(DomainName=domainName)
    domainArn = domainResponse["DomainStatus"]["ARN"]
    vpcEndpoint = osClient.create_vpc_endpoint(
        DomainArn=domainArn,
        VpcOptions={
            'SubnetIds': subnetIds,
            'SecurityGroupIds': securityGroups
        })
    return vpcEndpoint

def check_opensearch_provisioned_in_vpc(domainName):
    osClient = boto3.client("opensearch")
    domainResponse = osClient.describe_domain(DomainName=domainName)
    domainStatus = domainResponse["DomainStatus"]
    return "VPCOptions" in domainStatus
