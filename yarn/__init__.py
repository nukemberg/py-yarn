import google.protobuf
import yarn.protobuf.yarn_protos_pb2 as yarn_protos

YARN_PROTOCOL_VERSION=9

YARN_ENVIRONMENT_VARS = (
	'APPLICATION_WEB_PROXY_BASE',
	'MAX_APP_ATTEMPTS',
	'CONTAINER_ID',
	'NM_HOST',
	'NM_PORT',
	'LOCAL_DIRS',
	'LOG_DIRS',
	'HADOOP_CONF_DIR')

def pb_to_dict(obj):
	if hasattr(obj, 'ListFields'):
		return dict((f[0].name, pb_to_dict(f[1])) for f in obj.ListFields())
	elif isinstance(obj, google.protobuf.internal.containers.RepeatedCompositeFieldContainer):
		return map(pb_to_dict, obj)
	elif isinstance(obj, google.protobuf.internal.containers.RepeatedScalarFieldContainer):
		return map(pb_to_dict, obj)
	else:
		return obj

def pb_message_type(type_name):
	return getattr(yarn_protos, type_name)

def assign_pb(obj, field, f_value):
	if field.label == field.LABEL_REPEATED:
		if type(f_value) not in (list, tuple):
			f_value = [f_value]
		for v in f_value:
			getattr(obj, field.name).append(v)
	else:
		setattr(obj, field.name, f_value)
	return obj

def dict_to_pb(pb_type, data):
	"""Recursively build a protobuf object from dict data
	"""
	def _assign_field(obj, field, f_value):
		if field.type == field.TYPE_MESSAGE:
			if field.label == field.LABEL_REPEATED:
				if type(f_value) not in (tuple, list):
					f_value = [f_value]
				for f_val in f_value:
					getattr(obj, field.name).MergeFrom(
						dict_to_pb(pb_message_type(field.message_type.name), f_val))
			else:
				getattr(obj, field.name).MergeFrom(
					dict_to_pb(pb_message_type(field.message_type.name), f_value))
		elif field.type == field.TYPE_ENUM:
			assign_pb(obj, field, pb_message_type(field.message_type.name).Value(f_value.upper()))
		else:
			assign_pb(obj, field, f_value)


	obj = pb_type()

	# if pb message has only one field, just set it and return
	if len(obj.DESCRIPTOR.fields) == 1:
		_assign_field(obj, obj.DESCRIPTOR.fields[0], data)
		return obj

	for f_name, f_value in data.iteritems():
		field = obj.DESCRIPTOR.fields_by_name[f_name]
		_assign_field(obj, field, f_value)
	return obj

# o = dict_to_pb(yarn_protos.ApplicationSubmissionContextProto, 
# 	{'application_id': {'id': 2321}, 'queue': 'test',
# 	'applicationTags': ['test1', 'test2'],
# 	'priority': 1})