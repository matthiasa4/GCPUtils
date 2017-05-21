from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials

import argparse
from ProtoBufferMessage import ProtoBufferMessage
import pprint

pp = pprint.PrettyPrinter()

# TODO:
# LATER:
# - add syntax support for proto2 (optional, ...)

# Get specified BQ schema
def get_bq_schema(project, dataset, table, message_name):
    credentials = GoogleCredentials.get_application_default()
    bigquery_service = build('bigquery', 'v2', credentials=credentials)

    schema = bigquery_service.tables().get(projectId=project, datasetId=dataset, tableId=table).execute()["schema"]

    parsedSchema = ProtoBufferMessage(schema, 'bigquery')

    return parsedSchema.toString_nested(parsedSchema.schema, message_name)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", help="Specify the GCP project.", required=True)
    parser.add_argument("--dataset", help="Specify the BQ dataset.", required=True)
    parser.add_argument("--table", help="Specify the BQ table.", required=True)
    parser.add_argument("--message_name", help="Specify the name of your Proto message.", required=True)
    parser.add_argument("--output", help="Specify output name or path.", required=True)
    parser.add_argument("--proto_syntax", help="Specify the syntax for the protobuffer. [proto2 or proto3]", required=True)
    parser.add_argument("--package", help="Specify the package for your Protobuffer class.", required=True)
    parser.add_argument("--java_outer_classname", help="Defines the Java class name which should contain all of the classes in this file. [JAVA SPECIFIC]", required=False)
    args = parser.parse_args()



    # Write schema to file
    with open(args.output + '.proto', 'w') as file:
        file.write("syntax = \"" + args.proto_syntax + "\";\n\n")
        file.write("package " + args.package + ";\n\n")
        if(args.java_outer_classname is not None):
            file.write("option java_outer_classname = \"" + args.java_outer_classname + "\";\n\n")
        file.write(get_bq_schema(args.project, args.dataset, args.table, args.message_name).split('\n', 2)[2])

if __name__ == "__main__":
    main()
