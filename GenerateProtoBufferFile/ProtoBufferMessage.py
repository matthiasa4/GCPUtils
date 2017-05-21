import re
import pprint
import operator

from gevent._socket2 import socket

pp = pprint.PrettyPrinter()

class ProtoBufferMessage(object):
    schema = dict()

    def __init__(self, schema_in, type_of_schema):
        if type_of_schema == 'bigquery':
            self.schema = self.parseBigQueryToProtoBuffer(schema_in['fields'])

    # Parse BQ schema to ProtoBuffer schmea
    def parseBigQueryToProtoBuffer(self, schema):
        pp.pprint(schema)
        map = self.getBigQueryToProtoBufferMapping()
        res = dict()
        for i, field in enumerate(schema):
            if field['type'] != 'RECORD':
                res[field['name']] = dict()
                res[field['name']]['type'] = map['type'][field['type']]
                res[field['name']]['index'] = str(i + 1)
                res[field['name']]['mode'] = field['mode'].lower()
            else:
                res[field['name']] = dict()
                res[field['name']]['schema'] = self.parseBigQueryToProtoBuffer(field['fields'])
                res[field['name']]['type'] = 'dict'
                res[field['name']]['index'] = str(i + 1)
                res[field['name']]['mode'] = field['mode'].lower()
        return res

    # BQ to ProtoBuffer mapping
    def getBigQueryToProtoBufferMapping(self):
        mapping = dict()
        mapping['type'] = dict()
        mapping['type']['STRING'] = 'string';
        mapping['type']['BYTES'] = 'bytes';
        mapping['type']['INTEGER'] = 'int64';
        mapping['type']['FLOAT'] = 'float';
        mapping['type']['BOOLEAN'] = 'bool';
        mapping['type']['TIMESTAMP'] = 'int64';
        mapping['type']['DATE'] = 'int64';
        mapping['type']['TIME'] = 'int64';
        mapping['type']['DATETIME'] = 'int64';

        return mapping

    # ProtoBuffer schema to string
    def toString_nested(self, schema, name):
        sorted_schema = sorted(schema.items(), key=lambda x: int(x[1]['index']))
        res = ""
        res += '\n\nmessage ' + name.title() + ' {'
        for f in sorted_schema:
            f=f[0]
            #if f == 'type' or f == 'index' or f == 'mode':
            #    pass
            if schema[f]['type'] == 'dict':
                mode = '' if schema[f]['mode'] != 'repeated' else 'repeated '
                res += '\t'.join(self.toString_nested(schema[f]['schema'], f).splitlines(True))
                res += '\n\t' + mode + f.title() + ' ' + self.convert(f) + ' = ' + schema[f]['index'] + ';'
            else:
                mode = '' if schema[f]['mode'] != 'repeated' else 'repeated '
                res += '\n\t' + mode + schema[f]['type'] + ' ' + self.convert(f) + ' = ' + schema[f]['index'] + ';'

        res += '\n}\n'
        return res

    # ProtoBuffer schema to string
    def toString_unnested(self, schema, name):
        sorted_schema = sorted(schema.items(), key=lambda x: int(x[1]['index']))
        res = ""
        nested = ""
        res += '\n\nmessage ' + name + ' {'
        for f in sorted_schema:
            f = f[0]
            mode = '' if schema[f]['mode'] != 'repeated' else 'repeated'
            if f == 'type' or f == 'index' or f == 'mode':
                pass
            elif schema[f]['type'] == 'dict':
                nested += self.toString_unnested(schema[f], f)
                res += '\n\t' + mode + ' ' + f + ' ' + self.convert(f) + ' = ' + schema[f]['index'] + ';'
            else:
                res += '\n\t' + mode + ' '  + schema[f]['type'] + ' ' + self.convert(f) + ' = ' + schema[f]['index'] + ';'

        res += '\n}\n'
        return nested + '\n '+ res


    def convert(self, name):
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
