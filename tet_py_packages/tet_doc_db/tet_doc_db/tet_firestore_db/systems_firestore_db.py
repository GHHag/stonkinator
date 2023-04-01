import json

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

from tet_doc_db.doc_database_meta_classes.tet_signals_doc_db import ITetSignalsDocumentDatabase


class TetSystemsFirestoreDb(ITetSignalsDocumentDatabase):

    def __init__(self, credentials_path):
        self.__credentials = credentials.Certificate(credentials_path)
        firebase_admin.initialize_app(self.__credentials)
        self.__client = firestore.client()

    def _insert_system(self, system_name):
        self.__client.collection(u'systems').add({'name': system_name})

    def _get_system_id(self, system_name):
        query = self.__client.collection(u'systems').where('name', u'==', system_name).get()
        result = [system.id for system in query]
        if len(result) > 0:
            return result[0]
        else:
            return False

    def get_systems(self):
        query = self.__client.collection(u'systems').get()
        return json.dumps(
            {
                system.id: system.to_dict()
                for system in query
            }
        )

    def insert_system_metrics(self, system_name, metrics, *args):
        system_id = self._get_system_id(system_name)
        if not system_id:
            return False
        else:
            doc = self.__client.collection(u'systems').document(system_id)
            doc.update({'metrics': metrics})
            for data_dict in list(args):
                if isinstance(data_dict, dict):
                    for k, v in data_dict.items():
                        doc.update({k: v})
            return True

    def insert_entry_signal_data(self, system_name, data):
        data = json.loads(data)
        system_id = self._get_system_id(system_name)
        if not system_id:
            self._insert_system(system_name)
            system_id = self._get_system_id(system_name)
        else:
            query = self.__client.collection(u'entry_signals').where('system_id', u'==', system_id).get()
            for doc in query:
                doc.reference.delete()
        for data_point in data['data']:
            data_point['system_id'] = system_id
            self.__client.collection(u'entry_signals').add(data_point)
        return True

    def insert_active_position_data(self, system_name, data):
        data = json.loads(data)
        system_id = self._get_system_id(system_name)
        if not system_id:
            self._insert_system(system_name)
            system_id = self._get_system_id(system_name)
        else:
            query = self.__client.collection(u'active_positions').where('system_id', u'==', system_id).get()
            for doc in query:
                doc.reference.delete()
        for data_point in data['data']:
            data_point['system_id'] = system_id
            self.__client.collection(u'active_positions').add(data_point)
        return True

    def insert_exit_signal_data(self, system_name, data):
        data = json.loads(data)
        system_id = self._get_system_id(system_name)
        if not system_id:
            self._insert_system(system_name)
            system_id = self._get_system_id(system_name)
        else:
            query = self.__client.collection(u'exit_signals').where('system_id', u'==', system_id).get()
            for doc in query:
                doc.reference.delete()
        for data_point in data['data']:
            data_point['system_id'] = system_id
            self.__client.collection(u'exit_signals').add(data_point)
        return True


if __name__ == '__main__':
    pass
