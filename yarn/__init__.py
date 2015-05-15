import google.protobuf

YARN_PROTOCOL_VERSION=9

def pb_to_dict(obj):
	if hasattr(obj, 'ListFields'):
		return dict((f[0].name, pb_to_dict(f[1])) for f in obj.ListFields())
	elif isinstance(obj, google.protobuf.internal.containers.RepeatedCompositeFieldContainer):
		return map(pb_to_dict, obj)
	else:
		return obj
