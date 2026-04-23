from common import message_protocol
import uuid


class MessageHandler:

    def __init__(self):
        self.client_id = str(uuid.uuid4())
        self.total_sent = 0
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        self.total_sent += 1
        return message_protocol.internal.serialize([self.client_id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.client_id, self.total_sent])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        if fields[0] != self.client_id:
            return None
        return fields[1:]
