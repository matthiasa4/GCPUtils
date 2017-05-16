package com.datatonic.sky.sky_q_poc.protobuffer.util;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

public class ProtobufUtils {
	
	public static String getFieldType(String in) {
		String className = "";
		if (in.toLowerCase().equals("boolean")) {
			className = "BOOLEAN";
		} else if (in.toLowerCase().contains(".string")) {
			className = "STRING";
		} else if (in.toLowerCase().equals("int") || in.toLowerCase().equals("int32")
				|| in.toLowerCase().equals("int64")) {
			className = "INTEGER";
		} else if (in.toLowerCase().equals("float") || in.toLowerCase().equals("double")) {
			className = "FLOAT";
		}
		return className;
	}

	public static TableSchema makeTableSchema(Descriptor d) {
		TableSchema res = new TableSchema();

		List<FieldDescriptor> fields = d.getFields();
		
		List<TableFieldSchema> schema_fields = new ArrayList<TableFieldSchema>();

		for (FieldDescriptor f : fields) {
			String type = "STRING";
			String mode = "NULLABLE";

			if (f.isRepeated()) {
				mode = "REPEATED";
			}
			
			if (f.getType().toString().toUpperCase().contains("BYTES")) {
				type = "BYTES";
			}
			else if (f.getType().toString().toUpperCase().contains("INT")) {
				type = "INTEGER";
			}
			else if (f.getType().toString().toUpperCase().contains("BOOL")) {
				type = "BOOLEAN";
			}
			else if (f.getType().toString().toUpperCase().contains("FLOAT")
					|| f.getType().toString().toUpperCase().contains("DOUBLE")) {
				type = "FLOAT";
			}
			else if (f.getType().toString().toUpperCase().contains("MESSAGE")) {
				type = "RECORD";
				TableSchema ts = makeTableSchema(f.getMessageType());
				
				schema_fields.add(new TableFieldSchema().setName(f.getName().replace(".", "_")).setType(type).setMode(mode).setFields(ts.getFields()));
			}
			
			if(!type.equals("RECORD")){
				schema_fields.add(new TableFieldSchema().setName(f.getName().replace(".", "_")).setType(type).setMode(mode));
			}
		}
				
		res.setFields(schema_fields);

		return res;
	}

	public static TableRow makeTableRow(com.google.protobuf.Message message) {
		TableRow res = new TableRow();
		List<FieldDescriptor> fields = message.getDescriptorForType().getFields();

		for (FieldDescriptor f : fields) {
			String type = "STRING";

			if (f.getType().toString().toUpperCase().contains("STRING")) {
				res.set(f.getName().replace(".", "_"), String.valueOf(message.getField(f)));
			} else if (f.getType().toString().toUpperCase().contains("BYTES")) {
				res.set(f.getName().replace(".", "_"), (byte[]) message.getField(f));
			} else if (f.getType().toString().toUpperCase().contains("INT32")) {
				res.set(f.getName().replace(".", "_"), (int) message.getField(f));
			} else if (f.getType().toString().toUpperCase().contains("INT64")) {
				res.set(f.getName().replace(".", "_"), (long) message.getField(f));
			} else if (f.getType().toString().toUpperCase().contains("BOOL")) {
				res.set(f.getName().replace(".", "_"), (boolean) message.getField(f));
			} else if (f.getType().toString().toUpperCase().contains("FLOAT")
					|| f.getType().toString().toUpperCase().contains("DOUBLE")) {
				res.set(f.getName().replace(".", "_"), (double) message.getField(f));
			} else if (f.getType().toString().toUpperCase().contains("MESSAGE")) {
				type = "RECORD";
				if (message.getAllFields().containsKey(f)) {
					if (f.isRepeated()) {
						List<TableRow> tr = ((List<Message>) message.getField(f)).stream().map(m -> makeTableRow(m))
								.collect(Collectors.toList());
						res.set(f.getName().replace(".", "_"), tr);
					} else {
						TableRow tr = makeTableRow((Message) message.getField(f));
						res.set(f.getName().replace(".", "_"), tr);
					}
				}
			}
		}

		return res;
	}
}
