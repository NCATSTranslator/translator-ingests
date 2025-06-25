# Abstract encapsulation of Translator Ingest process datasets
# Probably will be a collection of KGX jsonlines + metadata files
# but for now, we specify the concept in an agnostic fashion here

# TODO: could be something like a UUID instead of just a simple Python str
DataSetId = str

# TODO: this class is just a stub proxy for some real data, so very incomplete!
#       Could perhaps be abstract and inherit from ABC too (like Storage)?
class DataSet:
    def __init__(self, dataset_id: DataSetId):
        self._dataset_id = dataset_id
