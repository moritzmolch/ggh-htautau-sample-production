import law


class ProductionTask(law.Task):
    def __init__(self, *args, **kwargs):
        super(ProductionTask, self).__init__(*args, **kwargs)

