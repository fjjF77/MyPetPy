import json

class SerializeJsonEncoder(json.JSONEncoder):
    def serialize_singal_obj(self, obj):
        if hasattr(obj, 'serialize'):
            print(obj.serialize())
            return obj.serialize()
        else:
            return json.JSONEncoder.default(self, obj)

    def default(self, obj):
        if isinstance(obj, list):
            return [self.serialize_singal_obj(o) for o in obj]
        else:
            self.serialize_singal_obj(obj)