from . import glacier_io, database


# ToDo: obsolete?

class VaultManager:

    def __init__(self, vault_name):
        self.vault_name = vault_name
        self.storage = glacier_io.VaultStorage(vault_name)
        self.db = database.InventoryLog(vault_name)

    def get_inventory(self):
        # get inventory from database
        inv = self.db.get_latest_response()
        # ToDo: extract inventory from job result
        return inv

    def request_inventory(self):
        # ToDo: check whether there are open requests
        request = self.storage.request_inventory()
        self.db.store_request(request)

    def store_inventory_responses(self):
        for request in self.db.get_open_requests():
            response = self.storage.retrieve_inventory(request.job_id)
            self.db.store_response(request.request_id, response)
